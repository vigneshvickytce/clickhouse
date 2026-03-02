# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Java 11 Maven benchmark project that seeds 1,000,000 rows of Active Directory user data into both MySQL and ClickHouse, then runs side-by-side performance comparisons across SELECT, cursor navigation, JOIN, DML, and CSV export workloads.

## Build & Run Commands

```bash
# Compile (handled in IntelliJ — user manages compilation)
mvn compile

# Seed both databases (run once before benchmarks)
mvn exec:java -Dexec.mainClass=DataSeeder

# Run full SELECT + DML benchmark
mvn exec:java -Dexec.mainClass=clickHouseDemo

# Run CSV export benchmark
mvn exec:java -Dexec.mainClass=ExportBenchmark
```

## Runtime Requirements

- **MySQL** at `localhost:3306`, user `root`, no password, database `reports_db` (created by DataSeeder)
- **ClickHouse** at `localhost:8123`, user `default`, no password, database `reports_db` (created by DataSeeder)

## Source Files

| File | Purpose |
|------|---------|
| `DataSeeder.java` | Creates schema and seeds 1M rows into both DBs |
| `clickHouseDemo.java` | Runs benchmark queries and prints side-by-side timing table |
| `ExportBenchmark.java` | Benchmarks CSV export using cursor pagination (5000-row batches) |

## Database Schema

Three tables mirror Active Directory attributes, joined by `unique_id` (`uid-0000000001` … `uid-1000000000`):

### ADSMUserGeneralDetails (primary)
- MySQL: `id BIGINT AUTO_INCREMENT`, `unique_id VARCHAR(36) UNIQUE`, plus 10 AD attribute columns
- ClickHouse: `ReplacingMergeTree(updated_at) ORDER BY (unique_id)` — supports upsert and soft-delete via `is_deleted UInt8`

### ADSMUserAccountDetails
- Columns: `unique_id`, `logon_name`, `logon_to_machine`, `last_logon_time`, `account_expires`, `pwd_last_set`, `account_enabled`
- ClickHouse: plain `MergeTree ORDER BY (unique_id)`

### ADSMUserExchangeDetails
- Columns: `unique_id`, `mailbox_name`, `mailbox_database_name`, `email_address`, `quota_mb`, `mailbox_properties`
- ClickHouse: plain `MergeTree ORDER BY (unique_id)`

## Key Design Decisions

**ReplacingMergeTree pattern** (ADSMUserGeneralDetails in ClickHouse):
- UPDATE → INSERT same `unique_id` + newer `updated_at`; engine deduplicates on next background merge
- DELETE → INSERT same `unique_id` + `is_deleted=1` + newer `updated_at`; reads use `FINAL WHERE is_deleted = 0`
- `ORDER BY (unique_id)` is collision-free because `unique_id` is globally unique (unlike `(user_id, event_date)` which had birthday-paradox collisions at 1M rows)

**Cursor pagination** — all navigation and export queries use keyset pagination:
```sql
WHERE id > {lastId} ORDER BY id LIMIT N
```
ClickHouse `ORDER BY (unique_id)` means cursor queries on `id` do not use the primary sparse index (full scan per page). This is why MySQL outperforms ClickHouse in cursor-based export benchmarks.

**Data seeding** — fixed `Random` seeds per table (42 / 43 / 44) ensure MySQL and ClickHouse receive identical data. `unique_id` format `uid-%010d` sorts lexicographically in insertion order.

## Dependencies

- `clickhouse-jdbc 0.4.6` (all-in-one shaded jar)
- `mysql-connector-java 8.0.33`
- `exec-maven-plugin 3.1.0`
