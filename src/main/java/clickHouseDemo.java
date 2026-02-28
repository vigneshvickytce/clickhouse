import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class clickHouseDemo {

    // --- Connection parameters ---
    private static final String MYSQL_URL =
        "jdbc:mysql://localhost:3306/reports_db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASS = "";

    private static final String CH_URL  = "jdbc:clickhouse://localhost:8123/reports_db?compress=0";
    private static final String CH_USER = "default";
    private static final String CH_PASS = "";

    // --- SELECT benchmark config ---
    private static final int WARMUP_RUNS      = 1;
    private static final int TIMED_RUNS       = 3;
    private static final int CURSOR_PAGES     = 5;
    private static final int CURSOR_PAGE_SIZE = 100;

    // --- DML benchmark config ---
    private static final int SINGLE_INSERT_COUNT = 100;
    private static final int BATCH_INSERT_COUNT  = 1_000;
    private static final int UPDATE_COUNT        = 50;

    // 16 columns from user_activity alone
    private static final String ALL_COLS =
        "id, user_id, country, event_type, duration_ms, created_at, " +
        "session_id, device_type, os, browser, page_url, referrer, " +
        "ip_address, response_time_ms, bytes_transferred, is_error";

    // 15 columns across all 3 joined tables (5 from each)
    private static final String JOIN_COLS =
        "ua.id, ua.user_id, ua.country, ua.event_type, ua.duration_ms, " +
        "g.city, g.region, g.latitude, g.longitude, g.isp, " +
        "d.screen_width, d.screen_height, d.language, d.timezone, d.connection_type";

    private static final String JOIN_CLAUSE =
        " FROM user_activity ua" +
        " JOIN activity_geo g    ON g.activity_id = ua.id" +
        " JOIN activity_device d ON d.activity_id = ua.id";

    // INSERT SQL templates (MySQL omits id — AUTO_INCREMENT; ClickHouse needs explicit id)
    private static final String MYSQL_INSERT_SQL =
        "INSERT INTO user_activity " +
        "(user_id, country, event_type, duration_ms, created_at, session_id," +
        " device_type, os, browser, page_url, referrer, ip_address," +
        " response_time_ms, bytes_transferred, is_error)" +
        " VALUES (?,?,?,?,?,?, ?,?,?,?,?,?, ?,?,?)";

    // ClickHouse INSERT includes event_date + updated_at + is_deleted (ReplacingMergeTree columns)
    private static final String CH_INSERT_SQL =
        "INSERT INTO user_activity " +
        "(id, user_id, country, event_type, duration_ms, created_at, session_id," +
        " device_type, os, browser, page_url, referrer, ip_address," +
        " response_time_ms, bytes_transferred, is_error," +
        " event_date, updated_at, is_deleted)" +
        " VALUES (?,?,?,?,?,?,?, ?,?,?,?,?,?, ?,?,?, ?,?,?)";

    // UPDATE SQL templates (ClickHouse uses ALTER TABLE mutation)
    private static final String MYSQL_UPDATE_SQL =
        "UPDATE user_activity SET duration_ms = %d, event_type = '%s' WHERE id = %d";
    private static final String CH_UPDATE_SQL =
        "ALTER TABLE user_activity UPDATE duration_ms = %d, event_type = '%s' WHERE id = %d";

    // Random data pools for DML inserts
    private static final String[] DML_COUNTRIES    = {"US","GB","DE","FR","JP","IN","BR","CA","AU","MX"};
    private static final String[] DML_EVENT_TYPES  = {"login","logout","purchase","view","click","search","share","download"};
    private static final String[] DML_DEVICE_TYPES = {"mobile","desktop","tablet","smart_tv"};
    private static final String[] DML_OS_LIST      = {"Windows","macOS","Linux","iOS","Android"};
    private static final String[] DML_BROWSERS     = {"Chrome","Firefox","Safari","Edge","Opera"};
    private static final String[] DML_PAGE_URLS    = {"/home","/products","/cart","/checkout","/profile"};
    private static final String[] DML_REFERRERS    = {"direct","google.com","facebook.com","twitter.com"};
    private static final DateTimeFormatter DML_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // --- Query plan: null SQL = section header ---
    private static final List<String[]> QUERY_PLAN = new ArrayList<>();
    static {
        QUERY_PLAN.add(new String[]{"AGGREGATION", null});
        QUERY_PLAN.add(new String[]{"Count All",
            "SELECT COUNT(*) FROM user_activity"});
        QUERY_PLAN.add(new String[]{"Group By Country",
            "SELECT country, COUNT(*), AVG(duration_ms) FROM user_activity GROUP BY country ORDER BY 2 DESC"});
        QUERY_PLAN.add(new String[]{"Filter by Country",
            "SELECT COUNT(*) FROM user_activity WHERE country = 'US'"});
        QUERY_PLAN.add(new String[]{"Filter by Duration",
            "SELECT COUNT(*) FROM user_activity WHERE duration_ms > 5000"});
        QUERY_PLAN.add(new String[]{"Group By Event Type",
            "SELECT event_type, SUM(duration_ms) FROM user_activity GROUP BY event_type ORDER BY 2 DESC"});

        QUERY_PLAN.add(new String[]{"SORTING  (16 cols, single table)", null});
        QUERY_PLAN.add(new String[]{"Top 100 by Duration",
            "SELECT " + ALL_COLS + " FROM user_activity ORDER BY duration_ms DESC LIMIT 100"});
        QUERY_PLAN.add(new String[]{"Sort 50k by Duration",
            "SELECT " + ALL_COLS + " FROM user_activity ORDER BY duration_ms DESC LIMIT 50000"});
        QUERY_PLAN.add(new String[]{"Sort 50k by user_id",
            "SELECT " + ALL_COLS + " FROM user_activity ORDER BY user_id DESC LIMIT 50000"});

        QUERY_PLAN.add(new String[]{"CONTAINS SEARCH  (16 cols, single table, LIMIT 1000)", null});
        QUERY_PLAN.add(new String[]{"LIKE '%user_5%'",
            "SELECT " + ALL_COLS + " FROM user_activity WHERE user_id LIKE '%user_5%' LIMIT 1000"});
        QUERY_PLAN.add(new String[]{"LIKE '%urch%' (purchase)",
            "SELECT " + ALL_COLS + " FROM user_activity WHERE event_type LIKE '%urch%' LIMIT 1000"});
        QUERY_PLAN.add(new String[]{"LIKE '%J%' (country JP)",
            "SELECT " + ALL_COLS + " FROM user_activity WHERE country LIKE '%J%' LIMIT 1000"});

        QUERY_PLAN.add(new String[]{"SORTING WITH JOIN  (15 cols, 3 tables)", null});
        QUERY_PLAN.add(new String[]{"JOIN Top 100 by Duration",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " ORDER BY ua.duration_ms DESC LIMIT 100"});
        QUERY_PLAN.add(new String[]{"JOIN Sort 50k by Duration",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " ORDER BY ua.duration_ms DESC LIMIT 50000"});
        QUERY_PLAN.add(new String[]{"JOIN Sort 50k by city",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " ORDER BY g.city DESC LIMIT 50000"});

        QUERY_PLAN.add(new String[]{"CONTAINS SEARCH WITH JOIN  (15 cols, LIMIT 1000)", null});
        QUERY_PLAN.add(new String[]{"JOIN + LIKE user '%user_5%'",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " WHERE ua.user_id LIKE '%user_5%' LIMIT 1000"});
        QUERY_PLAN.add(new String[]{"JOIN + LIKE city '%York%'",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " WHERE g.city LIKE '%York%' LIMIT 1000"});
        QUERY_PLAN.add(new String[]{"JOIN + LIKE isp '%com%'",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " WHERE g.isp LIKE '%com%' LIMIT 1000"});
        QUERY_PLAN.add(new String[]{"JOIN + LIKE language '%en%'",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " WHERE d.language LIKE '%en%' LIMIT 1000"});
    }

    // ════════════════════════════════════════════════════════════════════════
    // main
    // ════════════════════════════════════════════════════════════════════════

    public static void main(String[] args) {
        System.out.println("=".repeat(80));
        System.out.println("     MySQL vs ClickHouse Performance Benchmark (1,000,000 rows, 3 tables)");
        System.out.println("=".repeat(80));

        List<String> names = new ArrayList<>();
        List<String> sqls  = new ArrayList<>();
        for (String[] e : QUERY_PLAN) {
            if (e[1] != null) { names.add(e[0]); sqls.add(e[1]); }
        }

        long[] mysqlTimes      = new long[names.size()];
        long[] clickhouseTimes = new long[names.size()];
        long[] mysqlCursor     = new long[CURSOR_PAGES];
        long[] chCursor        = new long[CURSOR_PAGES];
        long[] mysqlCursorJoin = new long[CURSOR_PAGES];
        long[] chCursorJoin    = new long[CURSOR_PAGES];
        long[] mysqlDml        = new long[4];   // [singleInsert, batchInsert, update, delete]
        long[] chDml           = new long[4];

        // ---- MySQL ----
        System.out.println("\n[MySQL] Running SELECT benchmarks...");
        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASS)) {
            for (int q = 0; q < sqls.size(); q++) {
                mysqlTimes[q] = benchmark(conn, names.get(q), sqls.get(q));
            }
            System.out.println("  -- Cursor navigation (single table) --");
            mysqlCursor = benchmarkCursorNav(conn);
            System.out.println("  -- Cursor navigation (3-table JOIN) --");
            mysqlCursorJoin = benchmarkCursorNavJoin(conn);
            System.out.println("  -- DML benchmarks --");
            mysqlDml = benchmarkDML(conn, false);
        } catch (SQLException e) {
            System.err.println("[MySQL] Connection failed: " + e.getMessage());
        }

        // ---- ClickHouse ----
        System.out.println("\n[ClickHouse] Running SELECT benchmarks...");
        try (Connection conn = DriverManager.getConnection(CH_URL, CH_USER, CH_PASS)) {
            for (int q = 0; q < sqls.size(); q++) {
                clickhouseTimes[q] = benchmark(conn, names.get(q), sqls.get(q));
            }
            System.out.println("  -- Cursor navigation (single table) --");
            chCursor = benchmarkCursorNav(conn);
            System.out.println("  -- Cursor navigation (3-table JOIN) --");
            chCursorJoin = benchmarkCursorNavJoin(conn);
            System.out.println("  -- DML benchmarks --");
            chDml = benchmarkDML(conn, true);
        } catch (SQLException e) {
            System.err.println("[ClickHouse] Connection failed: " + e.getMessage());
        }

        printResults(names, mysqlTimes, clickhouseTimes,
                     mysqlCursor, chCursor,
                     mysqlCursorJoin, chCursorJoin,
                     mysqlDml, chDml);
    }

    // ════════════════════════════════════════════════════════════════════════
    // DML benchmarks
    // ════════════════════════════════════════════════════════════════════════

    /** Returns [singleInsertMs, batchInsertMs, updateMs, deleteMs]. */
    private static long[] benchmarkDML(Connection conn, boolean isCH) {
        long[] results = new long[4];
        results[0] = runSingleInserts(conn, isCH);
        results[1] = runBatchInsert(conn, isCH);
        results[2] = runUpdates(conn, isCH);
        results[3] = runDeletes(conn, isCH);
        return results;
    }

    /** 100 individual single-row INSERTs, one executeUpdate() per row. */
    private static long runSingleInserts(Connection conn, boolean isCH) {
        String sql = isCH ? CH_INSERT_SQL : MYSQL_INSERT_SQL;
        System.out.printf("  %-35s%n", "Single INSERT (" + SINGLE_INSERT_COUNT + " rows, 1×1)");
        System.out.printf("    SQL: %s  [×%d, one per execute]%n", sql, SINGLE_INSERT_COUNT);
        System.out.flush();

        java.util.Random rng = new java.util.Random(99);
        LocalDateTime now = LocalDateTime.now();
        long baseId = 5_000_000L;

        long t0 = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < SINGLE_INSERT_COUNT; i++) {
                bindDmlInsert(ps, rng, now, isCH, baseId + i, false);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            System.err.println("    Single INSERT error: " + e.getMessage());
        }
        long elapsed = System.currentTimeMillis() - t0;
        System.out.printf("    → total %d ms  (avg %.1f ms/row)%n",
            elapsed, (double) elapsed / SINGLE_INSERT_COUNT);
        return elapsed;
    }

    /** 1 batch of 1000 rows via addBatch / executeBatch. */
    private static long runBatchInsert(Connection conn, boolean isCH) {
        String sql = isCH ? CH_INSERT_SQL : MYSQL_INSERT_SQL;
        System.out.printf("  %-35s%n", "Batch INSERT (" + BATCH_INSERT_COUNT + " rows)");
        System.out.printf("    SQL: %s  [×%d, batched]%n", sql, BATCH_INSERT_COUNT);
        System.out.flush();

        java.util.Random rng = new java.util.Random(88);
        LocalDateTime now = LocalDateTime.now();
        long baseId = 6_000_000L;

        long t0 = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < BATCH_INSERT_COUNT; i++) {
                bindDmlInsert(ps, rng, now, isCH, baseId + i, false);
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            System.err.println("    Batch INSERT error: " + e.getMessage());
        }
        long elapsed = System.currentTimeMillis() - t0;
        System.out.printf("    → total %d ms  (avg %.2f ms/row)%n",
            elapsed, (double) elapsed / BATCH_INSERT_COUNT);
        return elapsed;
    }

    /**
     * MySQL      : UPDATE user_activity SET ... WHERE id = ?  (in-place B-tree update)
     * ClickHouse : INSERT new row with same ORDER BY key (user_id, event_date) and
     *              newer updated_at. ReplacingMergeTree keeps the latest version on
     *              the next background merge. No ALTER TABLE mutation needed.
     */
    private static long runUpdates(Connection conn, boolean isCH) {
        System.out.printf("  %-35s%n", "UPDATE (" + UPDATE_COUNT + " rows)");
        if (isCH) {
            System.out.printf("    SQL: INSERT upsert — same (user_id, event_date) + newer updated_at%n");
            System.out.printf("         ReplacingMergeTree deduplicates on next merge, keeping latest row%n");
        } else {
            System.out.printf("    SQL: %s  [×%d]%n", MYSQL_UPDATE_SQL, UPDATE_COUNT);
        }
        System.out.flush();

        java.util.Random rng = new java.util.Random(77);
        LocalDateTime    now = LocalDateTime.now();
        long baseId = 7_000_000L;

        long t0 = System.currentTimeMillis();
        if (isCH) {
            // Upsert: insert a row with the same (user_id, event_date) key but
            // a fresh updated_at so ReplacingMergeTree picks this as the winner
            try (PreparedStatement ps = conn.prepareStatement(CH_INSERT_SQL)) {
                for (int i = 0; i < UPDATE_COUNT; i++) {
                    bindDmlInsert(ps, rng, now, true, baseId + i, false);
                    ps.executeUpdate();
                }
            } catch (SQLException e) {
                System.err.println("    Upsert error: " + e.getMessage());
            }
        } else {
            try (Statement stmt = conn.createStatement()) {
                for (int i = 0; i < UPDATE_COUNT; i++) {
                    long   id       = rng.nextInt(1_000_000) + 1L;
                    int    duration = rng.nextInt(10_000);
                    String evtType  = DML_EVENT_TYPES[rng.nextInt(DML_EVENT_TYPES.length)];
                    stmt.execute(String.format(MYSQL_UPDATE_SQL, duration, evtType, id));
                }
            } catch (SQLException e) {
                System.err.println("    UPDATE error: " + e.getMessage());
            }
        }
        long elapsed = System.currentTimeMillis() - t0;
        System.out.printf("    → total %d ms  (avg %.1f ms/row)%n",
            elapsed, (double) elapsed / UPDATE_COUNT);
        return elapsed;
    }

    /**
     * MySQL      : DELETE FROM user_activity WHERE id = ?  (hard delete)
     * ClickHouse : INSERT a new row with is_deleted = 1 and newer updated_at.
     *              ReplacingMergeTree keeps only this marker on next merge.
     *              Read queries use:  SELECT ... FINAL WHERE is_deleted = 0
     */
    private static long runDeletes(Connection conn, boolean isCH) {
        System.out.printf("  %-35s%n", "DELETE (" + UPDATE_COUNT + " rows)");
        if (isCH) {
            System.out.printf("    SQL: INSERT soft-delete — same (user_id, event_date) + is_deleted=1%n");
            System.out.printf("         Read with: SELECT ... FINAL WHERE is_deleted = 0%n");
        } else {
            System.out.printf("    SQL: DELETE FROM user_activity WHERE id = ?  [×%d]%n", UPDATE_COUNT);
        }
        System.out.flush();

        java.util.Random rng = new java.util.Random(66);
        LocalDateTime    now = LocalDateTime.now();
        long baseId = 8_000_000L;

        long t0 = System.currentTimeMillis();
        if (isCH) {
            // Soft-delete: insert a marker row with is_deleted=1 and newer updated_at
            try (PreparedStatement ps = conn.prepareStatement(CH_INSERT_SQL)) {
                for (int i = 0; i < UPDATE_COUNT; i++) {
                    bindDmlInsert(ps, rng, now, true, baseId + i, true);  // isDeleted=true
                    ps.executeUpdate();
                }
            } catch (SQLException e) {
                System.err.println("    Soft-delete error: " + e.getMessage());
            }
        } else {
            try (Statement stmt = conn.createStatement()) {
                for (int i = 0; i < UPDATE_COUNT; i++) {
                    long id = rng.nextInt(1_000_000) + 1L;
                    stmt.execute("DELETE FROM user_activity WHERE id = " + id);
                }
            } catch (SQLException e) {
                System.err.println("    DELETE error: " + e.getMessage());
            }
        }
        long elapsed = System.currentTimeMillis() - t0;
        System.out.printf("    → total %d ms  (avg %.1f ms/row)%n",
            elapsed, (double) elapsed / UPDATE_COUNT);
        return elapsed;
    }

    /**
     * Binds all user_activity columns.
     * isDeleted=true sets is_deleted=1 (soft-delete marker for ClickHouse).
     */
    private static void bindDmlInsert(PreparedStatement ps,
                                       java.util.Random rng,
                                       LocalDateTime now,
                                       boolean isCH,
                                       long id,
                                       boolean isDeleted) throws SQLException {
        int p = 1;
        if (isCH) ps.setLong(p++, id);
        ps.setString(p++, "user_"  + (rng.nextInt(100_000) + 1));
        ps.setString(p++, DML_COUNTRIES[rng.nextInt(DML_COUNTRIES.length)]);
        ps.setString(p++, DML_EVENT_TYPES[rng.nextInt(DML_EVENT_TYPES.length)]);
        ps.setInt   (p++, rng.nextInt(10_000));
        LocalDateTime created = now.minusSeconds(rng.nextInt(30 * 24 * 3600));
        ps.setString(p++, created.format(DML_FMT));              // created_at
        ps.setString(p++, "sess_"  + (rng.nextInt(500_000) + 1));
        ps.setString(p++, DML_DEVICE_TYPES[rng.nextInt(DML_DEVICE_TYPES.length)]);
        ps.setString(p++, DML_OS_LIST[rng.nextInt(DML_OS_LIST.length)]);
        ps.setString(p++, DML_BROWSERS[rng.nextInt(DML_BROWSERS.length)]);
        ps.setString(p++, DML_PAGE_URLS[rng.nextInt(DML_PAGE_URLS.length)]);
        ps.setString(p++, DML_REFERRERS[rng.nextInt(DML_REFERRERS.length)]);
        ps.setString(p++, rng.nextInt(256) + "." + rng.nextInt(256) + "."
                        + rng.nextInt(256) + "." + rng.nextInt(256));
        ps.setInt   (p++, rng.nextInt(2_000));
        ps.setInt   (p++, rng.nextInt(1_000_000));
        ps.setInt   (p++, rng.nextInt(10) == 0 ? 1 : 0);        // is_error
        if (isCH) {
            // ReplacingMergeTree extra columns
            ps.setString(p++, created.toLocalDate().toString()); // event_date (ORDER BY key)
            ps.setString(p++, LocalDateTime.now().format(DML_FMT)); // updated_at = now (version)
            ps.setInt   (p,   isDeleted ? 1 : 0);                // is_deleted flag
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Cursor navigation
    // ════════════════════════════════════════════════════════════════════════

    private static long[] benchmarkCursorNav(Connection conn) {
        return runCursorBenchmark(conn,
            "SELECT " + ALL_COLS + " FROM user_activity"
            + " WHERE id > %d ORDER BY id LIMIT " + CURSOR_PAGE_SIZE);
    }

    private static long[] benchmarkCursorNavJoin(Connection conn) {
        return runCursorBenchmark(conn,
            "SELECT " + JOIN_COLS + JOIN_CLAUSE
            + " WHERE ua.id > %d ORDER BY ua.id LIMIT " + CURSOR_PAGE_SIZE);
    }

    private static long[] runCursorBenchmark(Connection conn, String sqlTemplate) {
        long[] times  = new long[CURSOR_PAGES];
        long cursor   = 0;

        for (int page = 0; page < CURSOR_PAGES; page++) {
            String sql = String.format(sqlTemplate, cursor);

            System.out.printf("  %-35s%n", "Nav " + (page + 1) + " (id > " + cursor + ")");
            System.out.printf("    SQL: %s%n", sql);
            System.out.flush();

            runQuery(conn, sql);   // warmup

            long total      = 0;
            long nextCursor = cursor;
            for (int run = 0; run < TIMED_RUNS; run++) {
                long t0 = System.currentTimeMillis();
                nextCursor = runQueryGetLastId(conn, sql, cursor);
                total += System.currentTimeMillis() - t0;
            }

            times[page] = total / TIMED_RUNS;
            cursor = nextCursor;
            System.out.printf("    → avg %d ms  (next cursor: %d)%n", times[page], cursor);
        }
        return times;
    }

    private static long runQueryGetLastId(Connection conn, String sql, long fallback) {
        long lastId = fallback;
        try (Statement stmt = conn.createStatement();
             ResultSet rs   = stmt.executeQuery(sql)) {
            while (rs.next()) lastId = rs.getLong(1);
        } catch (SQLException e) {
            System.err.println("Query error: " + e.getMessage());
        }
        return lastId;
    }

    // ════════════════════════════════════════════════════════════════════════
    // SELECT benchmark
    // ════════════════════════════════════════════════════════════════════════

    private static long benchmark(Connection conn, String name, String sql) {
        System.out.printf("  %-35s%n", name);
        System.out.printf("    SQL: %s%n", sql);
        System.out.flush();

        for (int i = 0; i < WARMUP_RUNS; i++) runQuery(conn, sql);

        long total = 0;
        for (int i = 0; i < TIMED_RUNS; i++) {
            long t0 = System.currentTimeMillis();
            runQuery(conn, sql);
            total += System.currentTimeMillis() - t0;
        }

        long avg = total / TIMED_RUNS;
        System.out.printf("    → avg %d ms%n", avg);
        return avg;
    }

    private static void runQuery(Connection conn, String sql) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs   = stmt.executeQuery(sql)) {
            while (rs.next()) { /* drain */ }
        } catch (SQLException e) {
            System.err.println("Query error: " + e.getMessage());
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Results table
    // ════════════════════════════════════════════════════════════════════════

    private static void printResults(List<String> names,
                                     long[] mysql,      long[] ch,
                                     long[] mysqlCur,   long[] chCur,
                                     long[] mysqlCurJ,  long[] chCurJ,
                                     long[] mysqlDml,   long[] chDml) {
        System.out.println();
        String div  = "=".repeat(84);
        String thin = "-".repeat(84);

        System.out.println(div);
        System.out.printf("  %-33s  %-14s  %-14s  %s%n",
            "Query", "MySQL (ms)", "ClickHouse (ms)", "Winner");
        System.out.println(thin);

        // SELECT sections
        int qi = 0;
        for (String[] entry : QUERY_PLAN) {
            if (entry[1] == null) {
                System.out.printf("%n  ── %s ──%n", entry[0]);
            } else {
                printRow(entry[0], mysql[qi], ch[qi]);
                qi++;
            }
        }

        // Cursor nav
        System.out.printf("%n  ── CURSOR NAV  (single table, 16 cols, %d pages × %d rows) ──%n",
            CURSOR_PAGES, CURSOR_PAGE_SIZE);
        for (int p = 0; p < CURSOR_PAGES; p++) {
            printRow("Nav " + (p + 1) + " (page " + (p + 1) + ")", mysqlCur[p], chCur[p]);
        }

        System.out.printf("%n  ── CURSOR NAV WITH JOIN  (3 tables, 15 cols, %d pages × %d rows) ──%n",
            CURSOR_PAGES, CURSOR_PAGE_SIZE);
        for (int p = 0; p < CURSOR_PAGES; p++) {
            printRow("Nav " + (p + 1) + " (page " + (p + 1) + ")", mysqlCurJ[p], chCurJ[p]);
        }

        // DML section
        System.out.printf("%n  ── DML PERFORMANCE ──%n");
        System.out.printf("  %-33s  %-14s  %-14s  %s%n",
            "", "MySQL (ms)", "ClickHouse (ms)", "Winner");
        System.out.println(thin);
        printRow("Single INSERT (" + SINGLE_INSERT_COUNT + " rows, 1×1)",  mysqlDml[0], chDml[0]);
        printRow("Batch  INSERT (" + BATCH_INSERT_COUNT  + " rows)",        mysqlDml[1], chDml[1]);
        printRow("UPDATE (" + UPDATE_COUNT + " rows)",                      mysqlDml[2], chDml[2]);
        printRow("DELETE (" + UPDATE_COUNT + " rows)",                      mysqlDml[3], chDml[3]);
        System.out.println();
        System.out.println("  Note — ClickHouse UPDATE/DELETE strategy (ReplacingMergeTree):");
        System.out.println("    UPDATE → INSERT same (user_id, event_date) key + newer updated_at");
        System.out.println("             Engine deduplicates on next background merge (no ALTER TABLE mutation)");
        System.out.println("    DELETE → INSERT is_deleted=1 marker row + newer updated_at");
        System.out.println("             Read with: SELECT ... FINAL WHERE is_deleted = 0");

        System.out.println();
        System.out.println(div);
    }

    /** Prints one result row. Shows which engine won and by how much. */
    private static void printRow(String name, long mysql, long ch) {
        String winner;
        if (mysql == 0 || ch == 0) {
            winner = "--";
        } else if (mysql >= ch) {
            winner = String.format("CH %.1fx faster",    (double) mysql / ch);
        } else {
            winner = String.format("MySQL %.1fx faster", (double) ch / mysql);
        }
        System.out.printf("  %-33s  %-14d  %-14d  %s%n", name, mysql, ch, winner);
    }
}
