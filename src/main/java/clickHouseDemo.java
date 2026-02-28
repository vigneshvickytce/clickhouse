import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class clickHouseDemo {

    // ── Connection parameters ─────────────────────────────────────────────────
    private static final String MYSQL_URL =
        "jdbc:mysql://localhost:3306/reports_db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASS = "";

    private static final String CH_URL  = "jdbc:clickhouse://localhost:8123/reports_db?compress=0";
    private static final String CH_USER = "default";
    private static final String CH_PASS = "";

    // ── SELECT benchmark config ───────────────────────────────────────────────
    private static final int WARMUP_RUNS      = 1;
    private static final int TIMED_RUNS       = 3;
    private static final int CURSOR_PAGES     = 5;
    private static final int CURSOR_PAGE_SIZE = 100;

    // ── DML benchmark config ──────────────────────────────────────────────────
    private static final int SINGLE_INSERT_COUNT = 100;
    private static final int BATCH_INSERT_COUNT  = 1_000;
    private static final int UPDATE_COUNT        = 50;

    // ── Column lists ──────────────────────────────────────────────────────────

    // 12 columns from ADSMUserGeneralDetails (id is col 1 → used for cursor nav)
    private static final String ALL_COLS =
        "id, unique_id, object_guid, sam_account_name, name, firstname, lastname, " +
        "initial, display_name, distinguished_name, department, title";

    // 15 columns across all 3 joined AD tables (5 from each)
    private static final String JOIN_COLS =
        "g.id, g.unique_id, g.name, g.department, g.title, " +
        "a.logon_name, a.logon_to_machine, a.account_enabled, a.last_logon_time, a.account_expires, " +
        "e.mailbox_name, e.mailbox_database_name, e.email_address, e.quota_mb, e.mailbox_properties";

    private static final String JOIN_CLAUSE =
        " FROM ADSMUserGeneralDetails g" +
        " JOIN ADSMUserAccountDetails  a ON a.unique_id = g.unique_id" +
        " JOIN ADSMUserExchangeDetails e ON e.unique_id = g.unique_id";

    // ── DML SQL ───────────────────────────────────────────────────────────────

    // MySQL: id is AUTO_INCREMENT — omit from INSERT
    private static final String MYSQL_INSERT_SQL =
        "INSERT INTO ADSMUserGeneralDetails " +
        "(unique_id, object_guid, sam_account_name, name, firstname, lastname, initial," +
        " display_name, distinguished_name, department, title)" +
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)";

    // ClickHouse: explicit id + is_deleted + updated_at (ReplacingMergeTree columns)
    private static final String CH_INSERT_SQL =
        "INSERT INTO ADSMUserGeneralDetails " +
        "(id, unique_id, object_guid, sam_account_name, name, firstname, lastname, initial," +
        " display_name, distinguished_name, department, title, is_deleted, updated_at)" +
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String MYSQL_UPDATE_SQL =
        "UPDATE ADSMUserGeneralDetails SET department = ?, title = ? WHERE id = ?";

    private static final String MYSQL_DELETE_SQL =
        "DELETE FROM ADSMUserGeneralDetails WHERE id = ?";

    // ── DML data pools ────────────────────────────────────────────────────────
    private static final String[] DML_FIRSTNAMES  = {"John","Jane","Michael","Sarah","David","Emily","Robert","Lisa"};
    private static final String[] DML_LASTNAMES   = {"Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis"};
    private static final String[] DML_DEPARTMENTS = {"IT","HR","Finance","Marketing","Sales","Operations","Legal","Engineering"};
    private static final String[] DML_TITLES      = {"Manager","Director","Developer","Analyst","Engineer","Consultant","Administrator","Specialist"};

    private static final DateTimeFormatter DML_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // ── Query plan: null SQL = section header ─────────────────────────────────
    private static final List<String[]> QUERY_PLAN = new ArrayList<>();
    static {
        QUERY_PLAN.add(new String[]{"AGGREGATION", null});
        QUERY_PLAN.add(new String[]{"Count All",
            "SELECT COUNT(*) FROM ADSMUserGeneralDetails"});
        QUERY_PLAN.add(new String[]{"Group By Department",
            "SELECT department, COUNT(*), COUNT(DISTINCT title) FROM ADSMUserGeneralDetails GROUP BY department ORDER BY 2 DESC"});
        QUERY_PLAN.add(new String[]{"Filter IT Dept",
            "SELECT COUNT(*) FROM ADSMUserGeneralDetails WHERE department = 'IT'"});
        QUERY_PLAN.add(new String[]{"Count Enabled Accounts",
            "SELECT COUNT(*) FROM ADSMUserAccountDetails WHERE account_enabled = 1"});
        QUERY_PLAN.add(new String[]{"Group By Mailbox DB",
            "SELECT mailbox_database_name, COUNT(*), AVG(quota_mb) FROM ADSMUserExchangeDetails GROUP BY mailbox_database_name ORDER BY 2 DESC"});

        QUERY_PLAN.add(new String[]{"SORTING  (12 cols, single table)", null});
        QUERY_PLAN.add(new String[]{"Top 100 by name",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails ORDER BY name DESC LIMIT 100"});
        QUERY_PLAN.add(new String[]{"Sort 50k by department",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails ORDER BY department DESC LIMIT 50000"});
        QUERY_PLAN.add(new String[]{"Sort 50k by sam_account_name",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails ORDER BY sam_account_name DESC LIMIT 50000"});

        QUERY_PLAN.add(new String[]{"CONTAINS SEARCH  (12 cols, single table, LIMIT 1000)", null});
        QUERY_PLAN.add(new String[]{"LIKE name '%John%'",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails WHERE name LIKE '%John%' LIMIT 1000"});
        QUERY_PLAN.add(new String[]{"LIKE department '%IT%'",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails WHERE department LIKE '%IT%' LIMIT 1000"});
        QUERY_PLAN.add(new String[]{"LIKE dn '%Finance%'",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails WHERE distinguished_name LIKE '%Finance%' LIMIT 1000"});

        QUERY_PLAN.add(new String[]{"SORTING WITH JOIN  (15 cols, 3 tables)", null});
        QUERY_PLAN.add(new String[]{"JOIN Top 100 by name",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " ORDER BY g.name DESC LIMIT 100"});
        QUERY_PLAN.add(new String[]{"JOIN Sort 50k by department",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " ORDER BY g.department DESC LIMIT 50000"});
        QUERY_PLAN.add(new String[]{"JOIN Sort 50k by mailbox DB",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " ORDER BY e.mailbox_database_name DESC LIMIT 50000"});

        QUERY_PLAN.add(new String[]{"CONTAINS SEARCH WITH JOIN  (15 cols, LIMIT 1000)", null});
        QUERY_PLAN.add(new String[]{"JOIN + LIKE name '%John%'",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " WHERE g.name LIKE '%John%' LIMIT 1000"});
        QUERY_PLAN.add(new String[]{"JOIN + LIKE email '%example%'",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " WHERE e.email_address LIKE '%example%' LIMIT 1000"});
        QUERY_PLAN.add(new String[]{"JOIN + LIKE dept '%IT%'",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " WHERE g.department LIKE '%IT%' LIMIT 1000"});
        QUERY_PLAN.add(new String[]{"JOIN + LIKE logon '%corp%'",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " WHERE a.logon_name LIKE '%corp%' LIMIT 1000"});
    }

    // ════════════════════════════════════════════════════════════════════════
    // main
    // ════════════════════════════════════════════════════════════════════════

    public static void main(String[] args) {
        System.out.println("=".repeat(84));
        System.out.println("     MySQL vs ClickHouse Performance Benchmark (1,000,000 rows, 3 AD tables)");
        System.out.println("=".repeat(84));

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
        long[] mysqlDml        = new long[4];
        long[] chDml           = new long[4];

        // ── MySQL ─────────────────────────────────────────────────────────────
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

        // ── ClickHouse ────────────────────────────────────────────────────────
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

    private static long[] benchmarkDML(Connection conn, boolean isCH) {
        long[] results = new long[4];
        results[0] = runSingleInserts(conn, isCH);
        results[1] = runBatchInsert(conn, isCH);
        results[2] = runUpdates(conn, isCH);
        results[3] = runDeletes(conn, isCH);
        return results;
    }

    private static long runSingleInserts(Connection conn, boolean isCH) {
        String sql = isCH ? CH_INSERT_SQL : MYSQL_INSERT_SQL;
        System.out.printf("  %-40s%n", "Single INSERT (" + SINGLE_INSERT_COUNT + " rows, 1×1)");
        System.out.printf("    SQL: %s  [×%d]%n", sql, SINGLE_INSERT_COUNT);

        java.util.Random rng = new java.util.Random(99);
        long baseId = 5_000_000L;

        long t0 = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < SINGLE_INSERT_COUNT; i++) {
                bindDmlInsert(ps, rng, isCH, baseId + i, null, false);
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

    private static long runBatchInsert(Connection conn, boolean isCH) {
        String sql = isCH ? CH_INSERT_SQL : MYSQL_INSERT_SQL;
        System.out.printf("  %-40s%n", "Batch INSERT (" + BATCH_INSERT_COUNT + " rows)");
        System.out.printf("    SQL: %s  [×%d, batched]%n", sql, BATCH_INSERT_COUNT);

        java.util.Random rng = new java.util.Random(88);
        long baseId = 6_000_000L;

        long t0 = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < BATCH_INSERT_COUNT; i++) {
                bindDmlInsert(ps, rng, isCH, baseId + i, null, false);
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
     * MySQL      : UPDATE ADSMUserGeneralDetails SET department=?, title=? WHERE id=?
     * ClickHouse : INSERT upsert — same unique_id as existing row + newer updated_at.
     *              ReplacingMergeTree keeps the latest version on next background merge.
     */
    private static long runUpdates(Connection conn, boolean isCH) {
        System.out.printf("  %-40s%n", "UPDATE (" + UPDATE_COUNT + " rows)");
        if (isCH) {
            System.out.printf("    SQL: INSERT upsert — same unique_id as target row + newer updated_at%n");
            System.out.printf("         ReplacingMergeTree deduplicates on next merge, keeping latest row%n");
        } else {
            System.out.printf("    SQL: %s  [×%d]%n", MYSQL_UPDATE_SQL, UPDATE_COUNT);
        }

        java.util.Random rng = new java.util.Random(77);
        long baseId = 7_000_000L;

        long t0 = System.currentTimeMillis();
        if (isCH) {
            try (PreparedStatement ps = conn.prepareStatement(CH_INSERT_SQL)) {
                for (int i = 0; i < UPDATE_COUNT; i++) {
                    // Target an existing seeded row — same unique_id triggers dedup on merge
                    long targetId = rng.nextInt(1_000_000) + 1L;
                    String existingUniqueId = String.format("uid-%010d", targetId);
                    bindDmlInsert(ps, rng, true, baseId + i, existingUniqueId, false);
                    ps.executeUpdate();
                }
            } catch (SQLException e) {
                System.err.println("    Upsert error: " + e.getMessage());
            }
        } else {
            try (PreparedStatement ps = conn.prepareStatement(MYSQL_UPDATE_SQL)) {
                for (int i = 0; i < UPDATE_COUNT; i++) {
                    long   targetId = rng.nextInt(1_000_000) + 1L;
                    String newDept  = DML_DEPARTMENTS[rng.nextInt(DML_DEPARTMENTS.length)];
                    String newTitle = DML_TITLES[rng.nextInt(DML_TITLES.length)];
                    ps.setString(1, newDept);
                    ps.setString(2, newTitle);
                    ps.setLong  (3, targetId);
                    ps.executeUpdate();
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
     * MySQL      : DELETE FROM ADSMUserGeneralDetails WHERE id=?
     * ClickHouse : INSERT soft-delete — same unique_id + is_deleted=1 + newer updated_at.
     *              Read with: SELECT ... FINAL WHERE is_deleted = 0
     */
    private static long runDeletes(Connection conn, boolean isCH) {
        System.out.printf("  %-40s%n", "DELETE (" + UPDATE_COUNT + " rows)");
        if (isCH) {
            System.out.printf("    SQL: INSERT soft-delete — same unique_id + is_deleted=1 + newer updated_at%n");
            System.out.printf("         Read with: SELECT ... FINAL WHERE is_deleted = 0%n");
        } else {
            System.out.printf("    SQL: %s  [×%d]%n", MYSQL_DELETE_SQL, UPDATE_COUNT);
        }

        java.util.Random rng = new java.util.Random(66);
        long baseId = 8_000_000L;

        long t0 = System.currentTimeMillis();
        if (isCH) {
            try (PreparedStatement ps = conn.prepareStatement(CH_INSERT_SQL)) {
                for (int i = 0; i < UPDATE_COUNT; i++) {
                    long targetId = rng.nextInt(1_000_000) + 1L;
                    String existingUniqueId = String.format("uid-%010d", targetId);
                    bindDmlInsert(ps, rng, true, baseId + i, existingUniqueId, true);
                    ps.executeUpdate();
                }
            } catch (SQLException e) {
                System.err.println("    Soft-delete error: " + e.getMessage());
            }
        } else {
            try (PreparedStatement ps = conn.prepareStatement(MYSQL_DELETE_SQL)) {
                for (int i = 0; i < UPDATE_COUNT; i++) {
                    long targetId = rng.nextInt(1_000_000) + 1L;
                    ps.setLong(1, targetId);
                    ps.executeUpdate();
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
     * Binds all ADSMUserGeneralDetails columns for DML.
     * uniqueIdOverride: if non-null, uses this unique_id instead of deriving from rowId.
     *   Used for CH upsert/soft-delete to target an existing row's ORDER BY key.
     * isDeleted: sets is_deleted=1 for CH soft-delete pattern.
     */
    private static void bindDmlInsert(PreparedStatement ps,
                                       java.util.Random rng,
                                       boolean isCH,
                                       long rowId,
                                       String uniqueIdOverride,
                                       boolean isDeleted) throws SQLException {
        String uniqueId   = uniqueIdOverride != null
                            ? uniqueIdOverride
                            : String.format("uid-%010d", rowId);
        String objectGuid = String.format("%08x-0000-0000-0000-%012x", rowId, rowId);
        String fname      = DML_FIRSTNAMES[rng.nextInt(DML_FIRSTNAMES.length)];
        String lname      = DML_LASTNAMES[rng.nextInt(DML_LASTNAMES.length)];
        String dept       = DML_DEPARTMENTS[rng.nextInt(DML_DEPARTMENTS.length)];
        String title      = DML_TITLES[rng.nextInt(DML_TITLES.length)];
        String samName    = (Character.toLowerCase(fname.charAt(0)) + lname).toLowerCase();
        String fullName   = fname + " " + lname;
        String dn         = "CN=" + fullName + ",OU=" + dept + ",DC=example,DC=com";

        int p = 1;
        if (isCH) ps.setLong(p++, rowId);
        ps.setString(p++, uniqueId);
        ps.setString(p++, objectGuid);
        ps.setString(p++, samName);
        ps.setString(p++, fullName);
        ps.setString(p++, fname);
        ps.setString(p++, lname);
        ps.setString(p++, String.valueOf(fname.charAt(0)));
        ps.setString(p++, fullName);
        ps.setString(p++, dn);
        ps.setString(p++, dept);
        ps.setString(p++, title);
        if (isCH) {
            ps.setInt   (p++, isDeleted ? 1 : 0);
            ps.setString(p,   LocalDateTime.now().format(DML_FMT));
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Cursor navigation
    // ════════════════════════════════════════════════════════════════════════

    private static long[] benchmarkCursorNav(Connection conn) {
        return runCursorBenchmark(conn,
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails"
            + " WHERE id > %d ORDER BY id LIMIT " + CURSOR_PAGE_SIZE);
    }

    private static long[] benchmarkCursorNavJoin(Connection conn) {
        return runCursorBenchmark(conn,
            "SELECT " + JOIN_COLS + JOIN_CLAUSE
            + " WHERE g.id > %d ORDER BY g.id LIMIT " + CURSOR_PAGE_SIZE);
    }

    private static long[] runCursorBenchmark(Connection conn, String sqlTemplate) {
        long[] times  = new long[CURSOR_PAGES];
        long   cursor = 0;

        for (int page = 0; page < CURSOR_PAGES; page++) {
            String sql = String.format(sqlTemplate, cursor);

            System.out.printf("  %-40s%n", "Nav " + (page + 1) + " (id > " + cursor + ")");
            System.out.printf("    SQL: %s%n", sql);

            runQuery(conn, sql);  // warmup

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
        System.out.printf("  %-40s%n", name);
        System.out.printf("    SQL: %s%n", sql);

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
                                     long[] mysql,     long[] ch,
                                     long[] mysqlCur,  long[] chCur,
                                     long[] mysqlCurJ, long[] chCurJ,
                                     long[] mysqlDml,  long[] chDml) {
        System.out.println();
        String div  = "=".repeat(84);
        String thin = "-".repeat(84);

        System.out.println(div);
        System.out.printf("  %-38s  %-14s  %-14s  %s%n",
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

        // Cursor nav — single table
        System.out.printf("%n  ── CURSOR NAV  (single table, 12 cols, %d pages × %d rows) ──%n",
            CURSOR_PAGES, CURSOR_PAGE_SIZE);
        for (int p = 0; p < CURSOR_PAGES; p++) {
            printRow("Nav page " + (p + 1), mysqlCur[p], chCur[p]);
        }

        // Cursor nav — 3-table JOIN
        System.out.printf("%n  ── CURSOR NAV WITH JOIN  (3 AD tables, 15 cols, %d pages × %d rows) ──%n",
            CURSOR_PAGES, CURSOR_PAGE_SIZE);
        for (int p = 0; p < CURSOR_PAGES; p++) {
            printRow("Nav page " + (p + 1), mysqlCurJ[p], chCurJ[p]);
        }

        // DML
        System.out.printf("%n  ── DML PERFORMANCE ──%n");
        System.out.printf("  %-38s  %-14s  %-14s  %s%n",
            "", "MySQL (ms)", "ClickHouse (ms)", "Winner");
        System.out.println(thin);
        printRow("Single INSERT (" + SINGLE_INSERT_COUNT + " rows, 1×1)", mysqlDml[0], chDml[0]);
        printRow("Batch  INSERT (" + BATCH_INSERT_COUNT  + " rows)",       mysqlDml[1], chDml[1]);
        printRow("UPDATE (" + UPDATE_COUNT + " rows)",                     mysqlDml[2], chDml[2]);
        printRow("DELETE (" + UPDATE_COUNT + " rows)",                     mysqlDml[3], chDml[3]);
        System.out.println();
        System.out.println("  Note — ClickHouse UPDATE/DELETE strategy (ReplacingMergeTree):");
        System.out.println("    UPDATE → INSERT same unique_id + newer updated_at");
        System.out.println("             Engine deduplicates on next background merge, keeping latest row");
        System.out.println("    DELETE → INSERT same unique_id + is_deleted=1 + newer updated_at");
        System.out.println("             Read with: SELECT ... FINAL WHERE is_deleted = 0");

        System.out.println();
        System.out.println(div);
    }

    private static void printRow(String name, long mysql, long ch) {
        String winner;
        if (mysql == 0 || ch == 0) {
            winner = "--";
        } else if (mysql >= ch) {
            winner = String.format("CH %.1fx faster",    (double) mysql / ch);
        } else {
            winner = String.format("MySQL %.1fx faster", (double) ch / mysql);
        }
        System.out.printf("  %-38s  %-14d  %-14d  %s%n", name, mysql, ch, winner);
    }
}
