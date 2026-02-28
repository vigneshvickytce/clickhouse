# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A minimal Java 11 Maven demo project that connects to a local ClickHouse instance via JDBC and queries a `user_activity` table in the `reports_db` database.

## Build & Run Commands

```bash
# Compile
mvn compile

# Package (produces fat jar with dependencies)
mvn package

# Run the main class directly via Maven
mvn exec:java -Dexec.mainClass=clickHouseDemo

# Or run the compiled class
java -cp target/classes:$(ls target/dependency/*.jar 2>/dev/null | tr '\n' ':') clickHouseDemo
```

## Dependencies

- **clickhouse-jdbc 0.4.6** (all-in-one shaded jar) — provides the JDBC driver for ClickHouse

## Runtime Requirements

- A running ClickHouse server at `localhost:8123` (HTTP port)
- Database `reports_db` with a table `user_activity` containing columns: `country` (String), `duration_ms` (Numeric)
- ClickHouse `default` user with no password

## Project Structure

Single source file: `src/main/java/clickHouseDemo.java` — connects via JDBC, runs a GROUP BY aggregate query, and prints results to stdout.
