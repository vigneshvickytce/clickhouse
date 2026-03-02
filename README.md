# MySQL vs ClickHouse Benchmark

A Java 11 / Maven project that seeds **1,000,000 rows** of synthetic Active Directory user data into both MySQL and ClickHouse, then runs side-by-side performance comparisons across multiple workload types. Results are written to `benchmark_results.txt` and rendered as `benchmark_report.html`.

---

## Database Configuration

|                | MySQL                        | ClickHouse            |
|----------------|------------------------------|-----------------------|
| **Version**    | 8.0.33                       | 26.1.3.52             |
| **Host**       | localhost:3306               | localhost:8123        |
| **User**       | root (no password)           | default (no password) |
| **Database**   | reports_db                   | reports_db            |
| **Driver**     | mysql-connector-java 8.0.33  | clickhouse-jdbc 0.4.6 |

### How MySQL is used

Standard InnoDB tables with `BIGINT AUTO_INCREMENT` primary keys and `UNIQUE` constraints on `unique_id`. DML uses conventional `UPDATE` and `DELETE` statements. Cursor pagination uses `WHERE id > {lastId} ORDER BY id LIMIT N`.

### How ClickHouse is used

- **ADSMUserGeneralDetails** uses `ReplacingMergeTree(updated_at) ORDER BY (unique_id)` — an append-only upsert pattern where updates are new `INSERT`s with a newer `updated_at`, and deletes set `is_deleted = 1`. Reads use `FINAL WHERE is_deleted = 0` to get the deduplicated view.
- **ADSMUserAccountDetails** and **ADSMUserExchangeDetails** use plain `MergeTree ORDER BY (unique_id)`, with updates done via `INSERT` (overwrite).
- Cursor pagination for export uses `WHERE unique_id > '{lastUniqueId}'` to leverage the sparse primary index.

---

## Data Schema

Three tables mirror Active Directory attributes, joined by `unique_id` (`uid-0000000001` … `uid-1000000000`):

### ADSMUserGeneralDetails *(primary)*
`id`, `unique_id`, `object_guid`, `sam_account_name`, `name`, `firstname`, `lastname`, `initial`, `display_name`, `distinguished_name`, `department`, `title`
> ClickHouse adds: `is_deleted UInt8`, `updated_at DateTime`

### ADSMUserAccountDetails
`unique_id`, `logon_name`, `logon_to_machine`, `last_logon_time`, `account_expires`, `pwd_last_set`, `account_enabled`

### ADSMUserExchangeDetails
`unique_id`, `mailbox_name`, `mailbox_database_name`, `email_address`, `quota_mb`, `mailbox_properties`

---

## Seeding

- **1,000,000 rows** inserted into each table in both databases
- Fixed `Random` seeds (42 / 43 / 44 per table) ensure both DBs receive identical data
- Run once via:
  ```bash
  mvn exec:java -Dexec.mainClass=DataSeeder
  ```

---

## Build & Run

```bash
# Seed both databases (run once)
mvn exec:java -Dexec.mainClass=DataSeeder

# Run SELECT + DML benchmark
mvn exec:java -Dexec.mainClass=clickHouseDemo

# Run CSV export benchmark
mvn exec:java -Dexec.mainClass=ExportBenchmark
```

---

## Performance Metrics

### 1. Aggregation — full-table scans

ClickHouse dominates due to columnar storage.

| Query | MySQL | ClickHouse | Winner |
|-------|-------|------------|--------|
| `COUNT(*)` all rows | 95 ms | 2 ms | **CH 48×** |
| `GROUP BY department` | 8,315 ms | 29 ms | **CH 287×** |
| `WHERE department = 'IT'` | 12 ms | 13 ms | ~Tie |
| Count enabled accounts | 100 ms | 3 ms | **CH 33×** |
| `GROUP BY` mailbox DB + `AVG(quota_mb)` | 3,359 ms | 10 ms | **CH 336×** |

### 2. IN-filter lookups — 10,000 random IDs across 1M rows

Point-lookup style — MySQL's B-Tree index wins.

| Query | MySQL | ClickHouse | Winner |
|-------|-------|------------|--------|
| `COUNT(*)` for 10k IDs | 50 ms | 92 ms | **MySQL 1.8×** |
| Fetch all 12 cols for 10k | 57 ms | 184 ms | **MySQL 3.2×** |
| 3-table JOIN fetch for 10k | 160 ms | 343 ms | **MySQL 2.1×** |

### 3. Sorting & LIKE search

ClickHouse faster on large sorts; MySQL faster on full-table LIKE.

| Query | MySQL | ClickHouse | Winner |
|-------|-------|------------|--------|
| Sort 50k by department | 588 ms | 290 ms | **CH 2×** |
| Sort 50k by sam_account_name | 1,194 ms | 282 ms | **CH 4.2×** |
| Full-table `LIKE name '%John%'` | 7 ms | 20 ms | **MySQL 2.9×** |
| JOIN sort 50k by department | 997 ms | 1,057 ms | ~Tie |

### 4. Cursor navigation — keyset pagination (5 pages × 100 rows)

Timings in **microseconds (µs)**. MySQL's indexed integer `id` cursor is far faster.

| Cursor type | MySQL avg (µs) | ClickHouse avg (µs) | Winner |
|-------------|---------------|---------------------|--------|
| Simple `WHERE id > N` | ~551 | ~11,177 | **MySQL ~20×** |
| JOIN `WHERE id > N` | ~765 | ~509,485 | **MySQL ~665×** |
| IN-filter cursor (temp table) | ~174 | ~107,425 | **MySQL ~617×** |

### 5. DML operations

ClickHouse is slower for single-row writes (append-log overhead) but faster for batch operations.

| Operation | MySQL | ClickHouse | Winner |
|-----------|-------|------------|--------|
| 100 single inserts | 12 ms | 527 ms | **MySQL 44×** |
| Batch insert 1,000 rows | 202 ms | 26 ms | **CH 7.8×** |
| Update 50 rows | 16 ms | 246 ms | **MySQL 15×** |
| Delete rows | 3 ms | 233 ms | **MySQL 78×** |
| AD sync batch (1,000 users, 3 tables) | 401 ms | 39 ms | **CH 10.3×** |

### 6. CSV Export

Paginated export of all 1M rows in 5,000-row batches. MySQL wins because ClickHouse's `FINAL` keyword forces a deduplication pass per query, adding overhead absent in MySQL.

| | MySQL | ClickHouse |
|--|-------|------------|
| Pagination cursor | `WHERE id > {N}` | `WHERE unique_id > '{N}' FINAL` |
| Dedup overhead | None | Per-query `ReplacingMergeTree` dedup |
| Winner | ✅ Faster overall | Slower due to `FINAL` overhead |

---

## Key Takeaway

| Database | Excels at | Struggles with |
|----------|-----------|----------------|
| **ClickHouse** | Full-table aggregations, large sorts, columnar scans, batch writes, bulk AD sync | Point lookups (IN filter), row-level DML, cursor pagination, single-row inserts |
| **MySQL** | Point lookups by ID, row-level DML, cursor-based pagination, full-table LIKE | Full-table aggregations, large-scale GROUP BY, analytics scans |

> Use **ClickHouse** for analytics/reporting workloads. Use **MySQL** for OLTP, record navigation, and row-level mutations.
