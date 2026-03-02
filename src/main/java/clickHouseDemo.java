import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Map;

public class clickHouseDemo {

    // ── Connection parameters ─────────────────────────────────────────────────
    private static final String MYSQL_URL =
        "jdbc:mysql://localhost:3306/reports_db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASS = "";

    private static final String CH_URL  = "jdbc:clickhouse://localhost:8123/reports_db?compress=0";
    private static final String CH_USER = "default";
    private static final String CH_PASS = "";

    // ── Results persistence ───────────────────────────────────────────────────
    private static final String RESULTS_FILE = "benchmark_results.txt";
    private static final String HTML_FILE     = "benchmark_report.html";

    // ── SELECT benchmark config ───────────────────────────────────────────────
    private static final int WARMUP_RUNS      = 1;
    private static final int TIMED_RUNS       = 3;
    private static final int CURSOR_PAGES     = 5;
    private static final int CURSOR_PAGE_SIZE = 100;

    // ── DML benchmark config ──────────────────────────────────────────────────
    private static final int SINGLE_INSERT_COUNT = 100;
    private static final int BATCH_INSERT_COUNT  = 1_000;
    private static final int UPDATE_COUNT        = 50;
    private static final int SYNC_BATCH_SIZE     = 1_000;  // users per AD sync run

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

    // Like JOIN_CLAUSE but with FINAL to force ReplacingMergeTree dedup + exclude soft-deleted rows
    private static final String FINAL_JOIN_CLAUSE =
        " FROM ADSMUserGeneralDetails FINAL g" +
        " JOIN ADSMUserAccountDetails  a ON a.unique_id = g.unique_id" +
        " JOIN ADSMUserExchangeDetails e ON e.unique_id = g.unique_id";

    // 10,000 unique random IDs scattered across the 1M dataset (seed 123 — fixed for repeatability)
    private static final String IN_IDS;
    private static final String IN_IDS_DISPLAY;   // truncated form for console output
    static {
        Set<Integer> ids = new LinkedHashSet<>();
        Random rngIn = new Random(123);
        while (ids.size() < 10_000) ids.add(rngIn.nextInt(1_000_000) + 1);
        StringBuilder sb = new StringBuilder();
        for (int id : ids) { if (sb.length() > 0) sb.append(','); sb.append(id); }
        IN_IDS         = sb.toString();
        IN_IDS_DISPLAY = sb.substring(0, sb.indexOf(",", 40)) + ", ... [10,000 ids]";
    }

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

    // MySQL AD sync: UPDATE 3 tables per user WHERE unique_id = ?
    private static final String MYSQL_SYNC_GENERAL_SQL =
        "UPDATE ADSMUserGeneralDetails " +
        "SET name=?, sam_account_name=?, department=?, title=?, display_name=?, distinguished_name=? " +
        "WHERE unique_id=?";
    private static final String MYSQL_SYNC_ACCOUNT_SQL =
        "UPDATE ADSMUserAccountDetails " +
        "SET logon_name=?, last_logon_time=?, account_enabled=? WHERE unique_id=?";
    private static final String MYSQL_SYNC_EXCHANGE_SQL =
        "UPDATE ADSMUserExchangeDetails " +
        "SET quota_mb=?, mailbox_properties=? WHERE unique_id=?";

    // ClickHouse AD sync: INSERT into Account/Exchange (upsert GeneralDetails via CH_INSERT_SQL)
    private static final String CH_SYNC_ACCOUNT_SQL =
        "INSERT INTO ADSMUserAccountDetails " +
        "(unique_id, logon_name, logon_to_machine, last_logon_time," +
        " account_expires, pwd_last_set, account_enabled) VALUES (?,?,?,?,?,?,?)";
    private static final String CH_SYNC_EXCHANGE_SQL =
        "INSERT INTO ADSMUserExchangeDetails " +
        "(unique_id, mailbox_name, mailbox_database_name, email_address, quota_mb, mailbox_properties)" +
        " VALUES (?,?,?,?,?,?)";

    // ── DML data pools ────────────────────────────────────────────────────────
    private static final String[] DML_FIRSTNAMES    = {"John","Jane","Michael","Sarah","David","Emily","Robert","Lisa"};
    private static final String[] DML_LASTNAMES     = {"Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis"};
    private static final String[] DML_DEPARTMENTS   = {"IT","HR","Finance","Marketing","Sales","Operations","Legal","Engineering"};
    private static final String[] DML_TITLES        = {"Manager","Director","Developer","Analyst","Engineer","Consultant","Administrator","Specialist"};
    private static final String[] DML_DOMAINS       = {"example.com","corp.local","company.org","enterprise.net"};
    private static final String[] DML_MACHINE_PFXS  = {"DESKTOP","LAPTOP","WORKSTATION","THIN-CLIENT"};
    private static final String[] DML_MAILBOX_DBS   = {"MBDB01","MBDB02","MBDB03","MBDB04","MBDB05"};
    private static final String[] DML_MAILBOX_PROPS = {"ActiveSync,OWA,MAPI","OWA,MAPI","ActiveSync,MAPI","MAPI only"};
    private static final int[]    DML_QUOTA_OPTIONS = {1024, 2048, 5120, 10240, 20480};

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

        QUERY_PLAN.add(new String[]{"IN FILTER  (10,000 random ids, scattered across 1M)", null});
        QUERY_PLAN.add(new String[]{"IN 10k — COUNT(*)",
            "SELECT COUNT(*) FROM ADSMUserGeneralDetails WHERE id IN (" + IN_IDS + ")"});
        QUERY_PLAN.add(new String[]{"IN 10k — fetch all cols",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails WHERE id IN (" + IN_IDS + ")"});
        QUERY_PLAN.add(new String[]{"IN 10k — GROUP BY dept",
            "SELECT department, COUNT(*) FROM ADSMUserGeneralDetails WHERE id IN (" + IN_IDS + ") GROUP BY department ORDER BY 2 DESC"});
        QUERY_PLAN.add(new String[]{"IN 10k — JOIN fetch",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " WHERE g.id IN (" + IN_IDS + ")"});

        QUERY_PLAN.add(new String[]{"IN FILTER + SORTING  (10,000 ids)", null});
        QUERY_PLAN.add(new String[]{"IN 10k + sort by name",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails WHERE id IN (" + IN_IDS + ") ORDER BY name DESC"});
        QUERY_PLAN.add(new String[]{"IN 10k + sort by dept",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails WHERE id IN (" + IN_IDS + ") ORDER BY department DESC"});
        QUERY_PLAN.add(new String[]{"IN 10k + JOIN + sort by name",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " WHERE g.id IN (" + IN_IDS + ") ORDER BY g.name DESC"});
        QUERY_PLAN.add(new String[]{"IN 10k + JOIN + sort by mailbox",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " WHERE g.id IN (" + IN_IDS + ") ORDER BY e.mailbox_database_name DESC"});

        QUERY_PLAN.add(new String[]{"IN FILTER + CONTAINS SEARCH  (10,000 ids)", null});
        QUERY_PLAN.add(new String[]{"IN 10k + LIKE name '%John%'",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails WHERE id IN (" + IN_IDS + ") AND name LIKE '%John%'"});
        QUERY_PLAN.add(new String[]{"IN 10k + LIKE dept '%IT%'",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails WHERE id IN (" + IN_IDS + ") AND department LIKE '%IT%'"});
        QUERY_PLAN.add(new String[]{"IN 10k + JOIN + LIKE name '%John%'",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " WHERE g.id IN (" + IN_IDS + ") AND g.name LIKE '%John%'"});
        QUERY_PLAN.add(new String[]{"IN 10k + JOIN + LIKE email '%example%'",
            "SELECT " + JOIN_COLS + JOIN_CLAUSE + " WHERE g.id IN (" + IN_IDS + ") AND e.email_address LIKE '%example%'"});

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

    // ── CH FINAL query plan: mirrors QUERY_PLAN — GeneralDetails queries gain FINAL + is_deleted=0 ──
    // AccountDetails / ExchangeDetails are plain MergeTree; their queries are unchanged.
    private static final List<String[]> CH_FINAL_QUERY_PLAN = new ArrayList<>();
    static {
        CH_FINAL_QUERY_PLAN.add(new String[]{"AGGREGATION", null});
        CH_FINAL_QUERY_PLAN.add(new String[]{"Count All",
            "SELECT COUNT(*) FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"Group By Department",
            "SELECT department, COUNT(*), COUNT(DISTINCT title) FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0 GROUP BY department ORDER BY 2 DESC"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"Filter IT Dept",
            "SELECT COUNT(*) FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0 AND department = 'IT'"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"Count Enabled Accounts",
            "SELECT COUNT(*) FROM ADSMUserAccountDetails WHERE account_enabled = 1"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"Group By Mailbox DB",
            "SELECT mailbox_database_name, COUNT(*), AVG(quota_mb) FROM ADSMUserExchangeDetails GROUP BY mailbox_database_name ORDER BY 2 DESC"});

        CH_FINAL_QUERY_PLAN.add(new String[]{"IN FILTER  (10,000 random ids, scattered across 1M)", null});
        CH_FINAL_QUERY_PLAN.add(new String[]{"IN 10k — COUNT(*)",
            "SELECT COUNT(*) FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0 AND id IN (" + IN_IDS + ")"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"IN 10k — fetch all cols",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0 AND id IN (" + IN_IDS + ")"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"IN 10k — GROUP BY dept",
            "SELECT department, COUNT(*) FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0 AND id IN (" + IN_IDS + ") GROUP BY department ORDER BY 2 DESC"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"IN 10k — JOIN fetch",
            "SELECT " + JOIN_COLS + FINAL_JOIN_CLAUSE + " WHERE g.is_deleted = 0 AND g.id IN (" + IN_IDS + ")"});

        CH_FINAL_QUERY_PLAN.add(new String[]{"IN FILTER + SORTING  (10,000 ids)", null});
        CH_FINAL_QUERY_PLAN.add(new String[]{"IN 10k + sort by name",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0 AND id IN (" + IN_IDS + ") ORDER BY name DESC"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"IN 10k + sort by dept",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0 AND id IN (" + IN_IDS + ") ORDER BY department DESC"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"IN 10k + JOIN + sort by name",
            "SELECT " + JOIN_COLS + FINAL_JOIN_CLAUSE + " WHERE g.is_deleted = 0 AND g.id IN (" + IN_IDS + ") ORDER BY g.name DESC"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"IN 10k + JOIN + sort by mailbox",
            "SELECT " + JOIN_COLS + FINAL_JOIN_CLAUSE + " WHERE g.is_deleted = 0 AND g.id IN (" + IN_IDS + ") ORDER BY e.mailbox_database_name DESC"});

        CH_FINAL_QUERY_PLAN.add(new String[]{"IN FILTER + CONTAINS SEARCH  (10,000 ids)", null});
        CH_FINAL_QUERY_PLAN.add(new String[]{"IN 10k + LIKE name '%John%'",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0 AND id IN (" + IN_IDS + ") AND name LIKE '%John%'"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"IN 10k + LIKE dept '%IT%'",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0 AND id IN (" + IN_IDS + ") AND department LIKE '%IT%'"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"IN 10k + JOIN + LIKE name '%John%'",
            "SELECT " + JOIN_COLS + FINAL_JOIN_CLAUSE + " WHERE g.is_deleted = 0 AND g.id IN (" + IN_IDS + ") AND g.name LIKE '%John%'"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"IN 10k + JOIN + LIKE email '%example%'",
            "SELECT " + JOIN_COLS + FINAL_JOIN_CLAUSE + " WHERE g.is_deleted = 0 AND g.id IN (" + IN_IDS + ") AND e.email_address LIKE '%example%'"});

        CH_FINAL_QUERY_PLAN.add(new String[]{"SORTING  (12 cols, single table)", null});
        CH_FINAL_QUERY_PLAN.add(new String[]{"Top 100 by name",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0 ORDER BY name DESC LIMIT 100"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"Sort 50k by department",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0 ORDER BY department DESC LIMIT 50000"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"Sort 50k by sam_account_name",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0 ORDER BY sam_account_name DESC LIMIT 50000"});

        CH_FINAL_QUERY_PLAN.add(new String[]{"CONTAINS SEARCH  (12 cols, single table, LIMIT 1000)", null});
        CH_FINAL_QUERY_PLAN.add(new String[]{"LIKE name '%John%'",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0 AND name LIKE '%John%' LIMIT 1000"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"LIKE department '%IT%'",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0 AND department LIKE '%IT%' LIMIT 1000"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"LIKE dn '%Finance%'",
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL WHERE is_deleted = 0 AND distinguished_name LIKE '%Finance%' LIMIT 1000"});

        CH_FINAL_QUERY_PLAN.add(new String[]{"SORTING WITH JOIN  (15 cols, 3 tables)", null});
        CH_FINAL_QUERY_PLAN.add(new String[]{"JOIN Top 100 by name",
            "SELECT " + JOIN_COLS + FINAL_JOIN_CLAUSE + " WHERE g.is_deleted = 0 ORDER BY g.name DESC LIMIT 100"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"JOIN Sort 50k by department",
            "SELECT " + JOIN_COLS + FINAL_JOIN_CLAUSE + " WHERE g.is_deleted = 0 ORDER BY g.department DESC LIMIT 50000"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"JOIN Sort 50k by mailbox DB",
            "SELECT " + JOIN_COLS + FINAL_JOIN_CLAUSE + " WHERE g.is_deleted = 0 ORDER BY e.mailbox_database_name DESC LIMIT 50000"});

        CH_FINAL_QUERY_PLAN.add(new String[]{"CONTAINS SEARCH WITH JOIN  (15 cols, LIMIT 1000)", null});
        CH_FINAL_QUERY_PLAN.add(new String[]{"JOIN + LIKE name '%John%'",
            "SELECT " + JOIN_COLS + FINAL_JOIN_CLAUSE + " WHERE g.is_deleted = 0 AND g.name LIKE '%John%' LIMIT 1000"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"JOIN + LIKE email '%example%'",
            "SELECT " + JOIN_COLS + FINAL_JOIN_CLAUSE + " WHERE g.is_deleted = 0 AND e.email_address LIKE '%example%' LIMIT 1000"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"JOIN + LIKE dept '%IT%'",
            "SELECT " + JOIN_COLS + FINAL_JOIN_CLAUSE + " WHERE g.is_deleted = 0 AND g.department LIKE '%IT%' LIMIT 1000"});
        CH_FINAL_QUERY_PLAN.add(new String[]{"JOIN + LIKE logon '%corp%'",
            "SELECT " + JOIN_COLS + FINAL_JOIN_CLAUSE + " WHERE g.is_deleted = 0 AND a.logon_name LIKE '%corp%' LIMIT 1000"});
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

        // Load previously saved results — skip any benchmark whose key is already present
        Map<String, Long> cache = loadResults();
        if (!cache.isEmpty()) {
            System.out.printf("Loaded %d results from \"%s\" — matching benchmarks will be skipped.%n%n",
                cache.size(), RESULTS_FILE);
        }

        long[] mysqlTimes      = new long[names.size()];
        long[] clickhouseTimes = new long[names.size()];
        long[] mysqlCursor       = new long[CURSOR_PAGES];
        long[] chCursor          = new long[CURSOR_PAGES];
        long[] mysqlCursorJoin   = new long[CURSOR_PAGES];
        long[] chCursorJoin      = new long[CURSOR_PAGES];
        long[] mysqlCursorIn     = new long[CURSOR_PAGES];
        long[] chCursorIn        = new long[CURSOR_PAGES];
        long[] mysqlCursorInJoin = new long[CURSOR_PAGES];
        long[] chCursorInJoin    = new long[CURSOR_PAGES];
        long[] mysqlCursorInTmp  = new long[CURSOR_PAGES];
        long[] chCursorInTmp     = new long[CURSOR_PAGES];
        long[] mysqlDml          = new long[5];
        long[] chDml             = new long[5];

        // ── MySQL ─────────────────────────────────────────────────────────────
        boolean mysqlAnyNeeded =
            names.stream().anyMatch(n -> !cache.containsKey("mysql." + n))
            || !allRangePresent(cache, "mysql.cursor_us_",         1, CURSOR_PAGES)
            || !allRangePresent(cache, "mysql.cursor_join_us_",    1, CURSOR_PAGES)
            || !allRangePresent(cache, "mysql.cursor_in_us_",      1, CURSOR_PAGES)
            || !allRangePresent(cache, "mysql.cursor_in_join_us_", 1, CURSOR_PAGES)
            || !allRangePresent(cache, "mysql.cursor_in_tmp_us_",  1, CURSOR_PAGES)
            || !allRangePresent(cache, "mysql.dml_",               0, 4);

        if (!mysqlAnyNeeded) {
            System.out.println("\n[MySQL] All results loaded from file — skipping execution.");
            for (int q = 0; q < names.size(); q++)
                mysqlTimes[q] = cache.getOrDefault("mysql." + names.get(q), 0L);
            for (int p = 0; p < CURSOR_PAGES; p++)
                mysqlCursor[p] = cache.getOrDefault("mysql.cursor_us_" + (p + 1), 0L);
            for (int p = 0; p < CURSOR_PAGES; p++)
                mysqlCursorJoin[p] = cache.getOrDefault("mysql.cursor_join_us_" + (p + 1), 0L);
            for (int p = 0; p < CURSOR_PAGES; p++)
                mysqlCursorIn[p] = cache.getOrDefault("mysql.cursor_in_us_" + (p + 1), 0L);
            for (int p = 0; p < CURSOR_PAGES; p++)
                mysqlCursorInJoin[p] = cache.getOrDefault("mysql.cursor_in_join_us_" + (p + 1), 0L);
            for (int p = 0; p < CURSOR_PAGES; p++)
                mysqlCursorInTmp[p] = cache.getOrDefault("mysql.cursor_in_tmp_us_" + (p + 1), 0L);
            for (int i = 0; i < 5; i++)
                mysqlDml[i] = cache.getOrDefault("mysql.dml_" + i, 0L);
        } else {
            System.out.println("\n[MySQL] Running benchmarks...");
            try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASS)) {

                System.out.println("  -- SELECT benchmarks --");
                for (int q = 0; q < sqls.size(); q++)
                    mysqlTimes[q] = cachedOrRun(conn, names.get(q), sqls.get(q), "mysql.", cache);

                if (allRangePresent(cache, "mysql.cursor_us_", 1, CURSOR_PAGES)) {
                    System.out.println("  [Cursor nav] loaded from file.");
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        mysqlCursor[p] = cache.get("mysql.cursor_us_" + (p + 1));
                } else {
                    System.out.println("  -- Cursor navigation (single table) --");
                    mysqlCursor = benchmarkCursorNav(conn);
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        cache.put("mysql.cursor_us_" + (p + 1), mysqlCursor[p]);
                    saveResults(cache);
                }

                if (allRangePresent(cache, "mysql.cursor_join_us_", 1, CURSOR_PAGES)) {
                    System.out.println("  [Cursor nav JOIN] loaded from file.");
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        mysqlCursorJoin[p] = cache.get("mysql.cursor_join_us_" + (p + 1));
                } else {
                    System.out.println("  -- Cursor navigation (3-table JOIN) --");
                    mysqlCursorJoin = benchmarkCursorNavJoin(conn);
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        cache.put("mysql.cursor_join_us_" + (p + 1), mysqlCursorJoin[p]);
                    saveResults(cache);
                }

                if (allRangePresent(cache, "mysql.cursor_in_us_", 1, CURSOR_PAGES)) {
                    System.out.println("  [Cursor nav IN filter] loaded from file.");
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        mysqlCursorIn[p] = cache.get("mysql.cursor_in_us_" + (p + 1));
                } else {
                    System.out.println("  -- Cursor navigation IN filter (single table) --");
                    mysqlCursorIn = benchmarkCursorNavIn(conn);
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        cache.put("mysql.cursor_in_us_" + (p + 1), mysqlCursorIn[p]);
                    saveResults(cache);
                }

                if (allRangePresent(cache, "mysql.cursor_in_join_us_", 1, CURSOR_PAGES)) {
                    System.out.println("  [Cursor nav IN filter JOIN] loaded from file.");
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        mysqlCursorInJoin[p] = cache.get("mysql.cursor_in_join_us_" + (p + 1));
                } else {
                    System.out.println("  -- Cursor navigation IN filter (3-table JOIN) --");
                    mysqlCursorInJoin = benchmarkCursorNavInJoin(conn);
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        cache.put("mysql.cursor_in_join_us_" + (p + 1), mysqlCursorInJoin[p]);
                    saveResults(cache);
                }

                if (allRangePresent(cache, "mysql.cursor_in_tmp_us_", 1, CURSOR_PAGES)) {
                    System.out.println("  [Cursor nav IN filter (temp table)] loaded from file.");
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        mysqlCursorInTmp[p] = cache.get("mysql.cursor_in_tmp_us_" + (p + 1));
                } else {
                    System.out.println("  -- Cursor navigation IN filter (temp table optimisation) --");
                    mysqlCursorInTmp = benchmarkCursorNavInTmp(conn, false);
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        cache.put("mysql.cursor_in_tmp_us_" + (p + 1), mysqlCursorInTmp[p]);
                    saveResults(cache);
                }

                if (allRangePresent(cache, "mysql.dml_", 0, 4)) {
                    System.out.println("  [DML] loaded from file.");
                    for (int i = 0; i < 5; i++) mysqlDml[i] = cache.get("mysql.dml_" + i);
                } else {
                    System.out.println("  -- DML benchmarks --");
                    mysqlDml = benchmarkDML(conn, false);
                    for (int i = 0; i < 5; i++) cache.put("mysql.dml_" + i, mysqlDml[i]);
                    saveResults(cache);
                }
            } catch (SQLException e) {
                System.err.println("[MySQL] Connection failed: " + e.getMessage());
            }
        }

        // ── ClickHouse ────────────────────────────────────────────────────────
        boolean chAnyNeeded =
            names.stream().anyMatch(n -> !cache.containsKey("ch." + n))
            || !allRangePresent(cache, "ch.cursor_us_",         1, CURSOR_PAGES)
            || !allRangePresent(cache, "ch.cursor_join_us_",    1, CURSOR_PAGES)
            || !allRangePresent(cache, "ch.cursor_in_us_",      1, CURSOR_PAGES)
            || !allRangePresent(cache, "ch.cursor_in_join_us_", 1, CURSOR_PAGES)
            || !allRangePresent(cache, "ch.cursor_in_tmp_us_",  1, CURSOR_PAGES)
            || !allRangePresent(cache, "ch.dml_",               0, 4);

        if (!chAnyNeeded) {
            System.out.println("\n[ClickHouse] All results loaded from file — skipping execution.");
            for (int q = 0; q < names.size(); q++)
                clickhouseTimes[q] = cache.getOrDefault("ch." + names.get(q), 0L);
            for (int p = 0; p < CURSOR_PAGES; p++)
                chCursor[p] = cache.getOrDefault("ch.cursor_us_" + (p + 1), 0L);
            for (int p = 0; p < CURSOR_PAGES; p++)
                chCursorJoin[p] = cache.getOrDefault("ch.cursor_join_us_" + (p + 1), 0L);
            for (int p = 0; p < CURSOR_PAGES; p++)
                chCursorIn[p] = cache.getOrDefault("ch.cursor_in_us_" + (p + 1), 0L);
            for (int p = 0; p < CURSOR_PAGES; p++)
                chCursorInJoin[p] = cache.getOrDefault("ch.cursor_in_join_us_" + (p + 1), 0L);
            for (int p = 0; p < CURSOR_PAGES; p++)
                chCursorInTmp[p] = cache.getOrDefault("ch.cursor_in_tmp_us_" + (p + 1), 0L);
            for (int i = 0; i < 5; i++)
                chDml[i] = cache.getOrDefault("ch.dml_" + i, 0L);
        } else {
            System.out.println("\n[ClickHouse] Running benchmarks...");
            try (Connection conn = DriverManager.getConnection(CH_URL, CH_USER, CH_PASS)) {

                System.out.println("  -- SELECT benchmarks --");
                for (int q = 0; q < sqls.size(); q++)
                    clickhouseTimes[q] = cachedOrRun(conn, names.get(q), sqls.get(q), "ch.", cache);

                if (allRangePresent(cache, "ch.cursor_us_", 1, CURSOR_PAGES)) {
                    System.out.println("  [Cursor nav] loaded from file.");
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        chCursor[p] = cache.get("ch.cursor_us_" + (p + 1));
                } else {
                    System.out.println("  -- Cursor navigation (single table) --");
                    chCursor = benchmarkCursorNav(conn);
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        cache.put("ch.cursor_us_" + (p + 1), chCursor[p]);
                    saveResults(cache);
                }

                if (allRangePresent(cache, "ch.cursor_join_us_", 1, CURSOR_PAGES)) {
                    System.out.println("  [Cursor nav JOIN] loaded from file.");
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        chCursorJoin[p] = cache.get("ch.cursor_join_us_" + (p + 1));
                } else {
                    System.out.println("  -- Cursor navigation (3-table JOIN) --");
                    chCursorJoin = benchmarkCursorNavJoin(conn);
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        cache.put("ch.cursor_join_us_" + (p + 1), chCursorJoin[p]);
                    saveResults(cache);
                }

                if (allRangePresent(cache, "ch.cursor_in_us_", 1, CURSOR_PAGES)) {
                    System.out.println("  [Cursor nav IN filter] loaded from file.");
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        chCursorIn[p] = cache.get("ch.cursor_in_us_" + (p + 1));
                } else {
                    System.out.println("  -- Cursor navigation IN filter (single table) --");
                    chCursorIn = benchmarkCursorNavIn(conn);
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        cache.put("ch.cursor_in_us_" + (p + 1), chCursorIn[p]);
                    saveResults(cache);
                }

                if (allRangePresent(cache, "ch.cursor_in_join_us_", 1, CURSOR_PAGES)) {
                    System.out.println("  [Cursor nav IN filter JOIN] loaded from file.");
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        chCursorInJoin[p] = cache.get("ch.cursor_in_join_us_" + (p + 1));
                } else {
                    System.out.println("  -- Cursor navigation IN filter (3-table JOIN) --");
                    chCursorInJoin = benchmarkCursorNavInJoin(conn);
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        cache.put("ch.cursor_in_join_us_" + (p + 1), chCursorInJoin[p]);
                    saveResults(cache);
                }

                if (allRangePresent(cache, "ch.cursor_in_tmp_us_", 1, CURSOR_PAGES)) {
                    System.out.println("  [Cursor nav IN filter (temp table)] loaded from file.");
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        chCursorInTmp[p] = cache.get("ch.cursor_in_tmp_us_" + (p + 1));
                } else {
                    System.out.println("  -- Cursor navigation IN filter (temp table optimisation) --");
                    chCursorInTmp = benchmarkCursorNavInTmp(conn, true);
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        cache.put("ch.cursor_in_tmp_us_" + (p + 1), chCursorInTmp[p]);
                    saveResults(cache);
                }

                if (allRangePresent(cache, "ch.dml_", 0, 4)) {
                    System.out.println("  [DML] loaded from file.");
                    for (int i = 0; i < 5; i++) chDml[i] = cache.get("ch.dml_" + i);
                } else {
                    System.out.println("  -- DML benchmarks --");
                    chDml = benchmarkDML(conn, true);
                    for (int i = 0; i < 5; i++) cache.put("ch.dml_" + i, chDml[i]);
                    saveResults(cache);
                }
            } catch (SQLException e) {
                System.err.println("[ClickHouse] Connection failed: " + e.getMessage());
            }
        }

        // ── ClickHouse FINAL ─────────────────────────────────────────────────
        List<String> chFinalNames = new ArrayList<>();
        List<String> chFinalSqls  = new ArrayList<>();
        for (String[] e : CH_FINAL_QUERY_PLAN) {
            if (e[1] != null) { chFinalNames.add(e[0]); chFinalSqls.add(e[1]); }
        }

        long[] chFinalTimes       = new long[chFinalNames.size()];
        long[] chFinalCursor      = new long[CURSOR_PAGES];
        long[] chFinalCursorJoin  = new long[CURSOR_PAGES];
        long[] chFinalCursorIn    = new long[CURSOR_PAGES];
        long[] chFinalCursorInTmp = new long[CURSOR_PAGES];

        boolean chFinalAnyNeeded =
            chFinalNames.stream().anyMatch(n -> !cache.containsKey("ch_final." + n))
            || !allRangePresent(cache, "ch_final.cursor_us_",        1, CURSOR_PAGES)
            || !allRangePresent(cache, "ch_final.cursor_join_us_",   1, CURSOR_PAGES)
            || !allRangePresent(cache, "ch_final.cursor_in_us_",     1, CURSOR_PAGES)
            || !allRangePresent(cache, "ch_final.cursor_in_tmp_us_", 1, CURSOR_PAGES);

        if (!chFinalAnyNeeded) {
            System.out.println("\n[ClickHouse FINAL] All results loaded from file — skipping execution.");
            for (int q = 0; q < chFinalNames.size(); q++)
                chFinalTimes[q] = cache.getOrDefault("ch_final." + chFinalNames.get(q), 0L);
            for (int p = 0; p < CURSOR_PAGES; p++)
                chFinalCursor[p] = cache.getOrDefault("ch_final.cursor_us_" + (p + 1), 0L);
            for (int p = 0; p < CURSOR_PAGES; p++)
                chFinalCursorJoin[p] = cache.getOrDefault("ch_final.cursor_join_us_" + (p + 1), 0L);
            for (int p = 0; p < CURSOR_PAGES; p++)
                chFinalCursorIn[p] = cache.getOrDefault("ch_final.cursor_in_us_" + (p + 1), 0L);
            for (int p = 0; p < CURSOR_PAGES; p++)
                chFinalCursorInTmp[p] = cache.getOrDefault("ch_final.cursor_in_tmp_us_" + (p + 1), 0L);
        } else {
            System.out.println("\n[ClickHouse FINAL] Running benchmarks (FINAL dedup)...");
            try (Connection conn = DriverManager.getConnection(CH_URL, CH_USER, CH_PASS)) {

                System.out.println("  -- SELECT benchmarks (FINAL) --");
                for (int q = 0; q < chFinalSqls.size(); q++)
                    chFinalTimes[q] = cachedOrRun(conn, chFinalNames.get(q), chFinalSqls.get(q), "ch_final.", cache);

                if (allRangePresent(cache, "ch_final.cursor_us_", 1, CURSOR_PAGES)) {
                    System.out.println("  [Cursor nav FINAL] loaded from file.");
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        chFinalCursor[p] = cache.get("ch_final.cursor_us_" + (p + 1));
                } else {
                    System.out.println("  -- Cursor navigation FINAL (single table) --");
                    chFinalCursor = benchmarkCursorNavFinal(conn);
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        cache.put("ch_final.cursor_us_" + (p + 1), chFinalCursor[p]);
                    saveResults(cache);
                }

                if (allRangePresent(cache, "ch_final.cursor_join_us_", 1, CURSOR_PAGES)) {
                    System.out.println("  [Cursor nav FINAL JOIN] loaded from file.");
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        chFinalCursorJoin[p] = cache.get("ch_final.cursor_join_us_" + (p + 1));
                } else {
                    System.out.println("  -- Cursor navigation FINAL (3-table JOIN) --");
                    chFinalCursorJoin = benchmarkCursorNavFinalJoin(conn);
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        cache.put("ch_final.cursor_join_us_" + (p + 1), chFinalCursorJoin[p]);
                    saveResults(cache);
                }

                if (allRangePresent(cache, "ch_final.cursor_in_us_", 1, CURSOR_PAGES)) {
                    System.out.println("  [Cursor nav FINAL IN filter] loaded from file.");
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        chFinalCursorIn[p] = cache.get("ch_final.cursor_in_us_" + (p + 1));
                } else {
                    System.out.println("  -- Cursor navigation FINAL IN filter (single table) --");
                    chFinalCursorIn = benchmarkCursorNavFinalIn(conn);
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        cache.put("ch_final.cursor_in_us_" + (p + 1), chFinalCursorIn[p]);
                    saveResults(cache);
                }

                if (allRangePresent(cache, "ch_final.cursor_in_tmp_us_", 1, CURSOR_PAGES)) {
                    System.out.println("  [Cursor nav FINAL IN filter (temp table)] loaded from file.");
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        chFinalCursorInTmp[p] = cache.get("ch_final.cursor_in_tmp_us_" + (p + 1));
                } else {
                    System.out.println("  -- Cursor navigation FINAL IN filter (temp table) --");
                    chFinalCursorInTmp = benchmarkCursorNavFinalInTmp(conn);
                    for (int p = 0; p < CURSOR_PAGES; p++)
                        cache.put("ch_final.cursor_in_tmp_us_" + (p + 1), chFinalCursorInTmp[p]);
                    saveResults(cache);
                }

            } catch (SQLException e) {
                System.err.println("[ClickHouse FINAL] Connection failed: " + e.getMessage());
            }
        }

        printResults(names, mysqlTimes, clickhouseTimes,
                     mysqlCursor,       chCursor,
                     mysqlCursorJoin,   chCursorJoin,
                     mysqlCursorIn,     chCursorIn,
                     mysqlCursorInJoin, chCursorInJoin,
                     mysqlCursorInTmp,  chCursorInTmp,
                     mysqlDml, chDml,
                     chFinalTimes,
                     chFinalCursor, chFinalCursorJoin,
                     chFinalCursorIn, chFinalCursorInTmp);

        writeHtmlReport(sqls, chFinalSqls,
                        mysqlTimes, clickhouseTimes,
                        mysqlCursor,       chCursor,
                        mysqlCursorJoin,   chCursorJoin,
                        mysqlCursorIn,     chCursorIn,
                        mysqlCursorInJoin, chCursorInJoin,
                        mysqlCursorInTmp,  chCursorInTmp,
                        mysqlDml, chDml,
                        chFinalTimes,
                        chFinalCursor, chFinalCursorJoin,
                        chFinalCursorIn, chFinalCursorInTmp);
    }

    // ════════════════════════════════════════════════════════════════════════
    // DML benchmarks
    // ════════════════════════════════════════════════════════════════════════

    private static long[] benchmarkDML(Connection conn, boolean isCH) {
        long[] results = new long[5];
        results[0] = runSingleInserts(conn, isCH);
        results[1] = runBatchInsert(conn, isCH);
        results[2] = runUpdates(conn, isCH);
        results[3] = runDeletes(conn, isCH);
        results[4] = runSyncBatch(conn, isCH);
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
     * Simulates a real AD sync batch: re-syncs SYNC_BATCH_SIZE existing users across all 3 tables.
     *
     * MySQL      : UPDATE all 3 tables WHERE unique_id = ? for each user (batched).
     * ClickHouse : INSERT upsert into GeneralDetails (ReplacingMergeTree — same unique_id + newer
     *              updated_at keeps latest version on merge) + INSERT re-sync into Account and
     *              Exchange tables (plain MergeTree in this benchmark; use ReplacingMergeTree in
     *              production for true upsert semantics there too).
     *
     * 5% of users are deprovisioned:
     *   CH    → is_deleted=1 in GeneralDetails upsert
     *   MySQL → account_enabled=0 in AccountDetails update
     */
    private static long runSyncBatch(Connection conn, boolean isCH) {
        System.out.printf("  %-40s%n", "AD Sync batch (" + SYNC_BATCH_SIZE + " users, 3 tables)");
        if (isCH) {
            System.out.printf("    GeneralDetails  : INSERT upsert (ReplacingMergeTree) same unique_id + newer updated_at%n");
            System.out.printf("    Account/Exchange: INSERT re-sync (MergeTree)%n");
            System.out.printf("    Deprovisioned (5%%): is_deleted=1 in GeneralDetails%n");
        } else {
            System.out.printf("    SQL: UPDATE 3 tables per user WHERE unique_id=?  [×%d, batched]%n", SYNC_BATCH_SIZE);
            System.out.printf("    Deprovisioned (5%%): account_enabled=0 in AccountDetails%n");
        }

        java.util.Random rng = new java.util.Random(55);
        LocalDateTime    now = LocalDateTime.now();
        long baseId          = 9_000_000L;

        long t0 = System.currentTimeMillis();
        if (isCH) {
            try (PreparedStatement psGen  = conn.prepareStatement(CH_INSERT_SQL);
                 PreparedStatement psAcc  = conn.prepareStatement(CH_SYNC_ACCOUNT_SQL);
                 PreparedStatement psExch = conn.prepareStatement(CH_SYNC_EXCHANGE_SQL)) {

                for (int i = 0; i < SYNC_BATCH_SIZE; i++) {
                    long   targetId = rng.nextInt(1_000_000) + 1L;
                    String uniqueId = String.format("uid-%010d", targetId);
                    boolean deprov  = (i % 20 == 0);   // 5% deprovisioned

                    bindDmlInsert(psGen, rng, true, baseId + i, uniqueId, deprov);
                    psGen.addBatch();

                    bindChSyncAccount(psAcc, rng, uniqueId, now, deprov ? 0 : 1);
                    psAcc.addBatch();

                    bindChSyncExchange(psExch, rng, uniqueId);
                    psExch.addBatch();
                }
                psGen.executeBatch();
                psAcc.executeBatch();
                psExch.executeBatch();
            } catch (SQLException e) {
                System.err.println("    CH sync error: " + e.getMessage());
            }
        } else {
            try (PreparedStatement psGen  = conn.prepareStatement(MYSQL_SYNC_GENERAL_SQL);
                 PreparedStatement psAcc  = conn.prepareStatement(MYSQL_SYNC_ACCOUNT_SQL);
                 PreparedStatement psExch = conn.prepareStatement(MYSQL_SYNC_EXCHANGE_SQL)) {

                for (int i = 0; i < SYNC_BATCH_SIZE; i++) {
                    long   targetId = rng.nextInt(1_000_000) + 1L;
                    String uniqueId = String.format("uid-%010d", targetId);
                    boolean deprov  = (i % 20 == 0);

                    bindMysqlSyncGeneral(psGen, rng, uniqueId);
                    psGen.addBatch();

                    bindMysqlSyncAccount(psAcc, rng, uniqueId, now, deprov ? 0 : 1);
                    psAcc.addBatch();

                    bindMysqlSyncExchange(psExch, rng, uniqueId);
                    psExch.addBatch();
                }
                psGen.executeBatch();
                psAcc.executeBatch();
                psExch.executeBatch();
            } catch (SQLException e) {
                System.err.println("    MySQL sync error: " + e.getMessage());
            }
        }

        long elapsed = System.currentTimeMillis() - t0;
        System.out.printf("    → total %d ms  (%.2f ms/user across 3 tables)%n",
            elapsed, (double) elapsed / SYNC_BATCH_SIZE);
        return elapsed;
    }

    // ── Sync bind helpers ─────────────────────────────────────────────────────

    private static void bindMysqlSyncGeneral(PreparedStatement ps, java.util.Random rng,
                                              String uniqueId) throws SQLException {
        String fname  = DML_FIRSTNAMES[rng.nextInt(DML_FIRSTNAMES.length)];
        String lname  = DML_LASTNAMES[rng.nextInt(DML_LASTNAMES.length)];
        String dept   = DML_DEPARTMENTS[rng.nextInt(DML_DEPARTMENTS.length)];
        String title  = DML_TITLES[rng.nextInt(DML_TITLES.length)];
        String sam    = (Character.toLowerCase(fname.charAt(0)) + lname).toLowerCase();
        String full   = fname + " " + lname;
        String dn     = "CN=" + full + ",OU=" + dept + ",DC=example,DC=com";
        ps.setString(1, full);    // name
        ps.setString(2, sam);     // sam_account_name
        ps.setString(3, dept);    // department
        ps.setString(4, title);   // title
        ps.setString(5, full);    // display_name
        ps.setString(6, dn);      // distinguished_name
        ps.setString(7, uniqueId); // WHERE unique_id=?
    }

    private static void bindMysqlSyncAccount(PreparedStatement ps, java.util.Random rng,
                                              String uniqueId, LocalDateTime now,
                                              int enabled) throws SQLException {
        String domain   = DML_DOMAINS[rng.nextInt(DML_DOMAINS.length)];
        String fname    = DML_FIRSTNAMES[rng.nextInt(DML_FIRSTNAMES.length)];
        LocalDateTime lastLogon = now.minusSeconds(rng.nextInt(7 * 24 * 3600));
        ps.setString(1, fname.toLowerCase() + "@" + domain);  // logon_name
        ps.setString(2, lastLogon.format(DML_FMT));            // last_logon_time
        ps.setInt   (3, enabled);                              // account_enabled
        ps.setString(4, uniqueId);                             // WHERE unique_id=?
    }

    private static void bindMysqlSyncExchange(PreparedStatement ps, java.util.Random rng,
                                               String uniqueId) throws SQLException {
        ps.setInt   (1, DML_QUOTA_OPTIONS[rng.nextInt(DML_QUOTA_OPTIONS.length)]); // quota_mb
        ps.setString(2, DML_MAILBOX_PROPS[rng.nextInt(DML_MAILBOX_PROPS.length)]); // mailbox_properties
        ps.setString(3, uniqueId);                                                  // WHERE unique_id=?
    }

    private static void bindChSyncAccount(PreparedStatement ps, java.util.Random rng,
                                           String uniqueId, LocalDateTime now,
                                           int enabled) throws SQLException {
        String domain   = DML_DOMAINS[rng.nextInt(DML_DOMAINS.length)];
        String fname    = DML_FIRSTNAMES[rng.nextInt(DML_FIRSTNAMES.length)];
        String machine  = DML_MACHINE_PFXS[rng.nextInt(DML_MACHINE_PFXS.length)]
                        + "-" + String.format("%06X", rng.nextInt(0x1000000));
        LocalDateTime lastLogon   = now.minusSeconds(rng.nextInt(7 * 24 * 3600));
        LocalDateTime acctExpires = now.plusDays(rng.nextInt(365) + 1);
        LocalDateTime pwdLastSet  = now.minusSeconds(rng.nextInt(90 * 24 * 3600));
        ps.setString(1, uniqueId);
        ps.setString(2, fname.toLowerCase() + "@" + domain);
        ps.setString(3, machine);
        ps.setString(4, lastLogon.format(DML_FMT));
        ps.setString(5, acctExpires.format(DML_FMT));
        ps.setString(6, pwdLastSet.format(DML_FMT));
        ps.setInt   (7, enabled);
    }

    private static void bindChSyncExchange(PreparedStatement ps, java.util.Random rng,
                                            String uniqueId) throws SQLException {
        String fname  = DML_FIRSTNAMES[rng.nextInt(DML_FIRSTNAMES.length)];
        String lname  = DML_LASTNAMES[rng.nextInt(DML_LASTNAMES.length)];
        String domain = DML_DOMAINS[rng.nextInt(DML_DOMAINS.length)];
        String mbx    = fname.toLowerCase() + "." + lname.toLowerCase();
        ps.setString(1, uniqueId);
        ps.setString(2, mbx);
        ps.setString(3, DML_MAILBOX_DBS[rng.nextInt(DML_MAILBOX_DBS.length)]);
        ps.setString(4, mbx + "@" + domain);
        ps.setInt   (5, DML_QUOTA_OPTIONS[rng.nextInt(DML_QUOTA_OPTIONS.length)]);
        ps.setString(6, DML_MAILBOX_PROPS[rng.nextInt(DML_MAILBOX_PROPS.length)]);
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

    /**
     * Optimised IN-filter cursor nav: loads the 10,000 IDs into a temporary table once
     * (indexed for MySQL, Memory-engine for ClickHouse), then pages with a simple JOIN +
     * WHERE id > cursor — no large IN literal re-sent per page.
     *
     * MySQL  : CREATE TEMPORARY TABLE tmp_bench_ids (id BIGINT PRIMARY KEY) SELECT id ...
     *          → cursor query: JOIN tmp_bench_ids t ON t.id = g.id WHERE g.id > ? LIMIT N
     * CH     : CREATE TEMPORARY TABLE tmp_bench_ids (id UInt64) ENGINE = Memory + INSERT
     *          → cursor query: JOIN tmp_bench_ids t ON t.id = g.id WHERE g.id > ? LIMIT N
     */
    /**
     * Optimised IN-filter cursor nav: loads the 10,000 IDs into a lookup table once,
     * then pages with a simple JOIN + WHERE id > cursor — no large IN literal re-sent per page.
     *
     * MySQL      : TEMPORARY TABLE (PRIMARY KEY) — session-scoped, survives across JDBC calls.
     * ClickHouse : Regular Memory-engine table — required because the CH HTTP interface is
     *              stateless: each Statement.execute() is a separate HTTP request, so a
     *              TEMPORARY TABLE created in one request is invisible to the next.
     *              A regular Memory table lives in reports_db and is visible across requests.
     */
    private static long[] benchmarkCursorNavInTmp(Connection conn, boolean isCH) {
        System.out.println("  [Setting up lookup table with 10,000 IDs...]");
        try (Statement s = conn.createStatement()) {
            if (isCH) {
                // Drop any leftover from a previous failed run, then create fresh
                s.execute("DROP TABLE IF EXISTS tmp_bench_ids");
                s.execute("CREATE TABLE tmp_bench_ids (id UInt64) ENGINE = Memory");
            } else {
                s.execute(
                    "CREATE TEMPORARY TABLE IF NOT EXISTS tmp_bench_ids " +
                    "(id BIGINT PRIMARY KEY) " +
                    "SELECT id FROM ADSMUserGeneralDetails WHERE id IN (" + IN_IDS + ")");
            }
        } catch (SQLException e) {
            System.err.println("    Table create error: " + e.getMessage());
            return new long[CURSOR_PAGES];
        }

        // ClickHouse: populate in a separate statement (table now exists in reports_db)
        if (isCH) {
            try (Statement s = conn.createStatement()) {
                s.execute("INSERT INTO tmp_bench_ids " +
                          "SELECT id FROM ADSMUserGeneralDetails WHERE id IN (" + IN_IDS + ")");
            } catch (SQLException e) {
                System.err.println("    Table populate error: " + e.getMessage());
                return new long[CURSOR_PAGES];
            }
        }

        String sqlTemplate =
            "SELECT " + ALL_COLS +
            " FROM ADSMUserGeneralDetails g" +
            " JOIN tmp_bench_ids t ON t.id = g.id" +
            " WHERE g.id > %d ORDER BY g.id LIMIT " + CURSOR_PAGE_SIZE;

        long[] results = runCursorBenchmark(conn, sqlTemplate);

        try (Statement s = conn.createStatement()) {
            s.execute((isCH ? "DROP TABLE" : "DROP TEMPORARY TABLE") + " IF EXISTS tmp_bench_ids");
        } catch (SQLException ignored) {}

        return results;
    }

    private static long[] benchmarkCursorNavIn(Connection conn) {
        return runCursorBenchmark(conn,
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails"
            + " WHERE id IN (" + IN_IDS + ") AND id > %d ORDER BY id LIMIT " + CURSOR_PAGE_SIZE);
    }

    private static long[] benchmarkCursorNavInJoin(Connection conn) {
        return runCursorBenchmark(conn,
            "SELECT " + JOIN_COLS + JOIN_CLAUSE
            + " WHERE g.id IN (" + IN_IDS + ") AND g.id > %d ORDER BY g.id LIMIT " + CURSOR_PAGE_SIZE);
    }

    // ── CH FINAL cursor nav variants ─────────────────────────────────────────

    private static long[] benchmarkCursorNavFinal(Connection conn) {
        return runCursorBenchmark(conn,
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL"
            + " WHERE is_deleted = 0 AND id > %d ORDER BY id LIMIT " + CURSOR_PAGE_SIZE);
    }

    private static long[] benchmarkCursorNavFinalJoin(Connection conn) {
        return runCursorBenchmark(conn,
            "SELECT " + JOIN_COLS + FINAL_JOIN_CLAUSE
            + " WHERE g.is_deleted = 0 AND g.id > %d ORDER BY g.id LIMIT " + CURSOR_PAGE_SIZE);
    }

    private static long[] benchmarkCursorNavFinalIn(Connection conn) {
        return runCursorBenchmark(conn,
            "SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL"
            + " WHERE is_deleted = 0 AND id IN (" + IN_IDS + ") AND id > %d ORDER BY id LIMIT " + CURSOR_PAGE_SIZE);
    }

    /**
     * Like benchmarkCursorNavInTmp but uses FINAL on ADSMUserGeneralDetails.
     * ClickHouse HTTP is stateless — uses a regular Memory-engine table, not TEMPORARY TABLE.
     */
    private static long[] benchmarkCursorNavFinalInTmp(Connection conn) {
        System.out.println("  [Setting up FINAL lookup table with 10,000 IDs...]");
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS tmp_bench_ids");
            s.execute("CREATE TABLE tmp_bench_ids (id UInt64) ENGINE = Memory");
        } catch (SQLException e) {
            System.err.println("    Table create error: " + e.getMessage());
            return new long[CURSOR_PAGES];
        }
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO tmp_bench_ids " +
                      "SELECT id FROM ADSMUserGeneralDetails FINAL " +
                      "WHERE is_deleted = 0 AND id IN (" + IN_IDS + ")");
        } catch (SQLException e) {
            System.err.println("    Table populate error: " + e.getMessage());
            return new long[CURSOR_PAGES];
        }

        String sqlTemplate =
            "SELECT " + ALL_COLS +
            " FROM ADSMUserGeneralDetails FINAL g" +
            " JOIN tmp_bench_ids t ON t.id = g.id" +
            " WHERE g.is_deleted = 0 AND g.id > %d ORDER BY g.id LIMIT " + CURSOR_PAGE_SIZE;

        long[] results = runCursorBenchmark(conn, sqlTemplate);

        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS tmp_bench_ids");
        } catch (SQLException ignored) {}

        return results;
    }

    private static long[] runCursorBenchmark(Connection conn, String sqlTemplate) {
        long[] times  = new long[CURSOR_PAGES];
        long   cursor = 0;

        for (int page = 0; page < CURSOR_PAGES; page++) {
            String sql = String.format(sqlTemplate, cursor);

            System.out.printf("  %-40s%n", "Nav " + (page + 1) + " (id > " + cursor + ")");
            System.out.printf("    SQL: %s%n", displaySql(sql));

            runQuery(conn, sql);  // warmup

            long total      = 0;
            long nextCursor = cursor;
            for (int run = 0; run < TIMED_RUNS; run++) {
                long t0 = System.nanoTime();
                nextCursor = runQueryGetLastId(conn, sql, cursor);
                total += System.nanoTime() - t0;
            }

            times[page] = total / TIMED_RUNS / 1_000;  // avg in microseconds (μs)
            cursor = nextCursor;
            System.out.printf("    → avg %d μs  (next cursor: %d)%n", times[page], cursor);
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
        System.out.printf("    SQL: %s%n", displaySql(sql));

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
                                     long[] mysql,          long[] ch,
                                     long[] mysqlCur,       long[] chCur,
                                     long[] mysqlCurJ,      long[] chCurJ,
                                     long[] mysqlCurIn,     long[] chCurIn,
                                     long[] mysqlCurInJ,    long[] chCurInJ,
                                     long[] mysqlCurInTmp,  long[] chCurInTmp,
                                     long[] mysqlDml,       long[] chDml,
                                     long[] chFinal,
                                     long[] chFinalCur,     long[] chFinalCurJ,
                                     long[] chFinalCurIn,   long[] chFinalCurInTmp) {
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

        // Cursor nav — single table (times in μs for sub-ms precision)
        System.out.printf("%n  ── CURSOR NAV  (single table, 12 cols, %d pages × %d rows, times in μs) ──%n",
            CURSOR_PAGES, CURSOR_PAGE_SIZE);
        for (int p = 0; p < CURSOR_PAGES; p++) {
            printRowUs("Nav page " + (p + 1), mysqlCur[p], chCur[p]);
        }

        // Cursor nav — 3-table JOIN
        System.out.printf("%n  ── CURSOR NAV WITH JOIN  (3 AD tables, 15 cols, %d pages × %d rows, times in μs) ──%n",
            CURSOR_PAGES, CURSOR_PAGE_SIZE);
        for (int p = 0; p < CURSOR_PAGES; p++) {
            printRowUs("Nav page " + (p + 1), mysqlCurJ[p], chCurJ[p]);
        }

        // Cursor nav — IN filter, single table
        System.out.printf("%n  ── CURSOR NAV IN FILTER  (single table, 12 cols, %d pages × %d rows, id IN 10k, μs) ──%n",
            CURSOR_PAGES, CURSOR_PAGE_SIZE);
        for (int p = 0; p < CURSOR_PAGES; p++) {
            printRowUs("Nav page " + (p + 1), mysqlCurIn[p], chCurIn[p]);
        }

        // Cursor nav — IN filter, 3-table JOIN
        System.out.printf("%n  ── CURSOR NAV IN FILTER WITH JOIN  (3 AD tables, 15 cols, %d pages × %d rows, μs) ──%n",
            CURSOR_PAGES, CURSOR_PAGE_SIZE);
        for (int p = 0; p < CURSOR_PAGES; p++) {
            printRowUs("Nav page " + (p + 1), mysqlCurInJ[p], chCurInJ[p]);
        }

        // Cursor nav — IN filter, temp table optimisation
        System.out.printf("%n  ── CURSOR NAV IN FILTER — TEMP TABLE OPT  (%d pages × %d rows, μs) ──%n",
            CURSOR_PAGES, CURSOR_PAGE_SIZE);
        System.out.printf("  %-38s%n",
            "  MySQL: tmp tbl (PRIMARY KEY) + JOIN | CH: Memory tmp tbl + JOIN");
        for (int p = 0; p < CURSOR_PAGES; p++) {
            printRowUs("Nav page " + (p + 1), mysqlCurInTmp[p], chCurInTmp[p]);
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
        printRow("AD Sync batch (" + SYNC_BATCH_SIZE + " users, 3 tables)",mysqlDml[4], chDml[4]);
        System.out.println();
        System.out.println("  Note — ClickHouse UPDATE/DELETE/SYNC strategy (ReplacingMergeTree):");
        System.out.println("    UPDATE → INSERT same unique_id + newer updated_at");
        System.out.println("             Engine deduplicates on next background merge, keeping latest row");
        System.out.println("    DELETE → INSERT same unique_id + is_deleted=1 + newer updated_at");
        System.out.println("             Read with: SELECT ... FINAL WHERE is_deleted = 0");
        System.out.println("    SYNC   → INSERT all 3 tables per user (batched)");
        System.out.println("             GeneralDetails : ReplacingMergeTree upsert — latest updated_at wins");
        System.out.println("             Account/Exchange: plain MergeTree re-insert (use ReplacingMergeTree");
        System.out.println("                              in production for dedup on those tables too)");
        System.out.println("             Deprovisioned users: is_deleted=1 in GeneralDetails (CH)");
        System.out.println("                                  account_enabled=0 in AccountDetails (MySQL)");

        // ── CH vs CH FINAL ──────────────────────────────────────────────────
        System.out.printf("%n%n  ── CH vs CH FINAL  (ReplacingMergeTree dedup overhead — same instance) ──%n");
        System.out.printf("  %-38s  %-16s  %-16s  %s%n",
            "Query", "CH no-FINAL (ms)", "CH FINAL (ms)", "FINAL overhead");
        System.out.println(thin);
        int chfQi = 0;
        for (String[] entry : QUERY_PLAN) {
            if (entry[1] == null) {
                System.out.printf("%n  ── %s ──%n", entry[0]);
            } else {
                printRowChFinal(entry[0], ch[chfQi], chFinal[chfQi]);
                chfQi++;
            }
        }

        System.out.printf("%n  ── CURSOR NAV: CH vs CH FINAL  (single table, 12 cols, μs) ──%n");
        for (int p = 0; p < CURSOR_PAGES; p++)
            printRowUsChFinal("Nav page " + (p + 1), chCur[p], chFinalCur[p]);

        System.out.printf("%n  ── CURSOR NAV: CH vs CH FINAL + JOIN  (3 AD tables, 15 cols, μs) ──%n");
        for (int p = 0; p < CURSOR_PAGES; p++)
            printRowUsChFinal("Nav page " + (p + 1), chCurJ[p], chFinalCurJ[p]);

        System.out.printf("%n  ── CURSOR NAV: CH vs CH FINAL + IN FILTER  (id IN 10k, μs) ──%n");
        for (int p = 0; p < CURSOR_PAGES; p++)
            printRowUsChFinal("Nav page " + (p + 1), chCurIn[p], chFinalCurIn[p]);

        System.out.printf("%n  ── CURSOR NAV: CH vs CH FINAL + TEMP TABLE  (id IN 10k via tmp tbl, μs) ──%n");
        for (int p = 0; p < CURSOR_PAGES; p++)
            printRowUsChFinal("Nav page " + (p + 1), chCurInTmp[p], chFinalCurInTmp[p]);

        System.out.println();
        System.out.println("  Note — FINAL forces ADSMUserGeneralDetails (ReplacingMergeTree) to:");
        System.out.println("    1. Merge duplicate versions in-place (same ORDER BY key = unique_id)");
        System.out.println("    2. Return only the latest row per unique_id");
        System.out.println("    3. Exclude is_deleted=1 rows via WHERE is_deleted = 0");
        System.out.println("    AccountDetails + ExchangeDetails are plain MergeTree — FINAL not used there.");

        System.out.println();
        System.out.println(div);
    }

    private static void printRowChFinal(String name, long chNoFinal, long chFinal) {
        String winner;
        if (chNoFinal == 0 || chFinal == 0) {
            winner = "--";
        } else if (chNoFinal <= chFinal) {
            winner = String.format("No-FINAL %.1fx faster", (double) chFinal / chNoFinal);
        } else {
            winner = String.format("FINAL %.1fx faster",    (double) chNoFinal / chFinal);
        }
        System.out.printf("  %-38s  %-16d  %-16d  %s%n", name, chNoFinal, chFinal, winner);
    }

    private static void printRowUsChFinal(String name, long chNoFinal, long chFinal) {
        String chNFStr = chNoFinal + " μs";
        String chFStr  = chFinal   + " μs";
        String winner;
        if (chNoFinal == 0 || chFinal == 0) {
            winner = "--";
        } else if (chNoFinal <= chFinal) {
            winner = String.format("No-FINAL %.1fx faster", (double) chFinal / chNoFinal);
        } else {
            winner = String.format("FINAL %.1fx faster",    (double) chNoFinal / chFinal);
        }
        System.out.printf("  %-38s  %-16s  %-16s  %s%n", name, chNFStr, chFStr, winner);
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

    /** Like printRow but values are in microseconds — formats as "N μs" strings. */
    private static void printRowUs(String name, long mysql, long ch) {
        String mysqlStr = mysql + " μs";
        String chStr    = ch    + " μs";
        String winner;
        if (mysql == 0 || ch == 0) {
            winner = "--";
        } else if (mysql >= ch) {
            winner = String.format("CH %.1fx faster",    (double) mysql / ch);
        } else {
            winner = String.format("MySQL %.1fx faster", (double) ch / mysql);
        }
        System.out.printf("  %-38s  %-14s  %-14s  %s%n", name, mysqlStr, chStr, winner);
    }

    // ════════════════════════════════════════════════════════════════════════
    // Results persistence
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Loads key=value pairs from RESULTS_FILE into a LinkedHashMap (preserves insertion order).
     * Lines starting with '#' and blank lines are skipped.
     */
    private static Map<String, Long> loadResults() {
        Map<String, Long> map = new LinkedHashMap<>();
        File f = new File(RESULTS_FILE);
        if (!f.exists()) return map;
        try (BufferedReader r = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("#") || line.isEmpty()) continue;
                int eq = line.indexOf('=');
                if (eq > 0) {
                    try {
                        map.put(line.substring(0, eq).trim(),
                                Long.parseLong(line.substring(eq + 1).trim()));
                    } catch (NumberFormatException ignored) {}
                }
            }
        } catch (Exception e) {
            System.err.println("Warning: could not read " + RESULTS_FILE + ": " + e.getMessage());
        }
        return map;
    }

    /** Replaces the full IN_IDS list with a short display form for console output. */
    private static String displaySql(String sql) {
        return sql.replace(IN_IDS, IN_IDS_DISPLAY);
    }

    /** Rewrites RESULTS_FILE with the current cache contents. */
    private static void saveResults(Map<String, Long> cache) {
        try (PrintWriter w = new PrintWriter(new FileWriter(RESULTS_FILE))) {
            w.println("# MySQL vs ClickHouse Benchmark Results");
            w.println("# Saved: " + LocalDateTime.now().format(DML_FMT));
            w.println("# Delete this file to re-run all benchmarks.");
            w.println();
            for (Map.Entry<String, Long> e : cache.entrySet())
                w.println(e.getKey() + "=" + e.getValue());
        } catch (Exception e) {
            System.err.println("Warning: could not write " + RESULTS_FILE + ": " + e.getMessage());
        }
    }

    /**
     * Returns true if all keys in the range [from..to] exist in the cache.
     * Keys are formed as: prefix + index  (e.g. "mysql.cursor_1" for prefix="mysql.cursor_", from=1)
     */
    private static boolean allRangePresent(Map<String, Long> cache,
                                            String prefix, int from, int to) {
        for (int i = from; i <= to; i++)
            if (!cache.containsKey(prefix + i)) return false;
        return true;
    }

    /**
     * Returns the cached value for (prefix+name) if present (prints "[from file]"),
     * otherwise runs the benchmark, caches the result, and saves to disk.
     */
    private static long cachedOrRun(Connection conn, String name, String sql,
                                     String prefix, Map<String, Long> cache) {
        String key = prefix + name;
        if (cache.containsKey(key)) {
            long val = cache.get(key);
            System.out.printf("  %-40s [from file] → %d ms%n", name, val);
            return val;
        }
        long result = benchmark(conn, name, sql);
        cache.put(key, result);
        saveResults(cache);
        return result;
    }

    // ════════════════════════════════════════════════════════════════════════
    // HTML Report
    // ════════════════════════════════════════════════════════════════════════

    private static void writeHtmlReport(
            List<String> sqls,     List<String> chFinalSqls,
            long[] mysql,          long[] ch,
            long[] mysqlCur,       long[] chCur,
            long[] mysqlCurJ,      long[] chCurJ,
            long[] mysqlCurIn,     long[] chCurIn,
            long[] mysqlCurInJ,    long[] chCurInJ,
            long[] mysqlCurInTmp,  long[] chCurInTmp,
            long[] mysqlDml,       long[] chDml,
            long[] chFinal,
            long[] chFinalCur,     long[] chFinalCurJ,
            long[] chFinalCurIn,   long[] chFinalCurInTmp) {

        try (PrintWriter w = new PrintWriter(new FileWriter(HTML_FILE))) {

            // ── head + styles ──────────────────────────────────────────────
            w.println("<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\">");
            w.println("<title>MySQL vs ClickHouse Benchmark</title><style>");
            w.println("*{box-sizing:border-box}");
            w.println("body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:32px;background:#f0f2f5;color:#222}");
            w.println("h1{margin:0 0 4px;font-size:1.6rem}p.sub{margin:0 0 28px;color:#666;font-size:.88rem}");
            w.println("table{border-collapse:collapse;width:100%;background:#fff;border-radius:10px;");
            w.println("  box-shadow:0 2px 8px rgba(0,0,0,.08);margin-bottom:32px;overflow:hidden}");
            w.println("thead tr.tbl-title th{background:#0f172a;color:#f8fafc;font-size:.9rem;padding:11px 16px;text-align:left;letter-spacing:.02em}");
            w.println("thead tr.col-hdr th{background:#1e293b;color:#94a3b8;font-size:.75rem;padding:7px 16px;text-align:left;letter-spacing:.06em;text-transform:uppercase}");
            w.println("td{padding:9px 16px;border-bottom:1px solid #f1f5f9;vertical-align:top;font-size:.88rem}");
            w.println("tr:last-child td{border-bottom:none}");
            w.println("tr.sec td{background:#334155;color:#cbd5e1;font-weight:700;font-size:.75rem;letter-spacing:.06em;padding:6px 16px;text-transform:uppercase}");
            w.println(".qname{font-weight:600}");
            w.println(".qsql{font-family:'SFMono-Regular',Consolas,monospace;font-size:.72rem;color:#64748b;margin-top:5px;word-break:break-all;line-height:1.5}");
            w.println(".win{background:#dcfce7!important;color:#14532d;font-weight:700}");
            w.println(".wlabel{color:#16a34a;font-weight:700;white-space:nowrap}");
            w.println(".tie,.na{color:#94a3b8}");
            w.println(".num{font-variant-numeric:tabular-nums}");
            w.println("</style></head><body>");

            // ── page header ────────────────────────────────────────────────
            w.println("<h1>MySQL vs ClickHouse Benchmark</h1>");
            w.printf("<p class=\"sub\">1,000,000 rows &bull; 3 AD tables &bull; Generated %s</p>%n",
                LocalDateTime.now().format(DML_FMT));

            // ── SELECT benchmarks ──────────────────────────────────────────
            w.println("<table>");
            w.println("<thead>");
            w.println("<tr class=\"tbl-title\"><th colspan=\"4\">SELECT Benchmarks</th></tr>");
            w.println("<tr class=\"col-hdr\"><th>Query &amp; SQL</th><th>MySQL (ms)</th><th>ClickHouse (ms)</th><th>Winner</th></tr>");
            w.println("</thead><tbody>");
            int qi = 0;
            for (String[] entry : QUERY_PLAN) {
                if (entry[1] == null) {
                    w.printf("<tr class=\"sec\"><td colspan=\"4\">%s</td></tr>%n", h(entry[0]));
                } else {
                    htmlMsRow(w, entry[0], displaySql(sqls.get(qi)), mysql[qi], ch[qi]);
                    qi++;
                }
            }
            w.println("</tbody></table>");

            // ── Cursor nav sections ────────────────────────────────────────
            htmlCursorTable(w,
                "Cursor Nav — Single Table  (" + CURSOR_PAGES + " pages × " + CURSOR_PAGE_SIZE + " rows)",
                h("SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails") +
                    " WHERE id &gt; {cursor} ORDER BY id LIMIT " + CURSOR_PAGE_SIZE,
                mysqlCur, chCur);

            htmlCursorTable(w,
                "Cursor Nav With JOIN — 3 AD Tables  (" + CURSOR_PAGES + " pages × " + CURSOR_PAGE_SIZE + " rows)",
                h("SELECT " + JOIN_COLS + JOIN_CLAUSE) +
                    " WHERE g.id &gt; {cursor} ORDER BY g.id LIMIT " + CURSOR_PAGE_SIZE,
                mysqlCurJ, chCurJ);

            htmlCursorTable(w,
                "Cursor Nav IN Filter — Single Table  (" + CURSOR_PAGES + " pages × " + CURSOR_PAGE_SIZE + " rows, id IN 10k)",
                h("SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails") +
                    " WHERE id IN (" + h(IN_IDS_DISPLAY) + ") AND id &gt; {cursor} ORDER BY id LIMIT " + CURSOR_PAGE_SIZE,
                mysqlCurIn, chCurIn);

            htmlCursorTable(w,
                "Cursor Nav IN Filter With JOIN — 3 AD Tables  (" + CURSOR_PAGES + " pages × " + CURSOR_PAGE_SIZE + " rows)",
                h("SELECT " + JOIN_COLS + JOIN_CLAUSE) +
                    " WHERE g.id IN (" + h(IN_IDS_DISPLAY) + ") AND g.id &gt; {cursor} ORDER BY g.id LIMIT " + CURSOR_PAGE_SIZE,
                mysqlCurInJ, chCurInJ);

            htmlCursorTable(w,
                "Cursor Nav IN Filter — Temp Table Opt  (" + CURSOR_PAGES + " pages × " + CURSOR_PAGE_SIZE + " rows)",
                h("SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails g") +
                    " JOIN tmp_bench_ids t ON t.id = g.id WHERE g.id &gt; {cursor} ORDER BY g.id LIMIT " + CURSOR_PAGE_SIZE,
                mysqlCurInTmp, chCurInTmp);

            // ── DML benchmarks ─────────────────────────────────────────────
            w.println("<table>");
            w.println("<thead>");
            w.println("<tr class=\"tbl-title\"><th colspan=\"4\">DML Benchmarks</th></tr>");
            w.println("<tr class=\"col-hdr\"><th>Operation &amp; SQL</th><th>MySQL (ms)</th><th>ClickHouse (ms)</th><th>Winner</th></tr>");
            w.println("</thead><tbody>");
            w.println("<tr class=\"sec\"><td colspan=\"4\">Write Performance</td></tr>");
            htmlMsRow(w, "Single INSERT (" + SINGLE_INSERT_COUNT + " rows, 1×1)",
                "INSERT INTO ADSMUserGeneralDetails (unique_id, ...) VALUES (?, ...) [executed " + SINGLE_INSERT_COUNT + "×]",
                mysqlDml[0], chDml[0]);
            htmlMsRow(w, "Batch INSERT (" + BATCH_INSERT_COUNT + " rows)",
                "INSERT INTO ADSMUserGeneralDetails (...) VALUES (?, ...) [addBatch / executeBatch, " + BATCH_INSERT_COUNT + " rows]",
                mysqlDml[1], chDml[1]);
            htmlMsRow(w, "UPDATE (" + UPDATE_COUNT + " rows)",
                "MySQL: UPDATE ADSMUserGeneralDetails SET department=?, title=? WHERE id=?  |  " +
                "CH: INSERT upsert — same unique_id + newer updated_at (ReplacingMergeTree)",
                mysqlDml[2], chDml[2]);
            htmlMsRow(w, "DELETE (" + UPDATE_COUNT + " rows)",
                "MySQL: DELETE FROM ADSMUserGeneralDetails WHERE id=?  |  " +
                "CH: INSERT soft-delete — same unique_id + is_deleted=1 + newer updated_at",
                mysqlDml[3], chDml[3]);
            htmlMsRow(w, "AD Sync batch (" + SYNC_BATCH_SIZE + " users, 3 tables)",
                "UPDATE/INSERT all 3 tables per user (batched): GeneralDetails + AccountDetails + ExchangeDetails  |  5% deprovisioned",
                mysqlDml[4], chDml[4]);
            w.println("</tbody></table>");

            // ── notes ──────────────────────────────────────────────────────
            w.println("<div style=\"background:#fff;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.08);padding:20px 24px;font-size:.85rem;color:#444;line-height:1.7\">");
            w.println("<b>Notes</b><ul style=\"margin:8px 0 0;padding-left:20px\">");
            w.println("<li><code>ADSMUserGeneralDetails</code> uses <code>ReplacingMergeTree ORDER BY (unique_id)</code> in ClickHouse — <code>id</code> is not the primary sort key, so <code>WHERE id &gt; N</code> cursor queries cannot use the sparse primary index and trigger a full scan per page.</li>");
            w.println("<li>IN FILTER cursor (10k ids, raw): large IN literal re-parsed per page → MySQL ~370 ms; ClickHouse columnar scan ~95 ms.</li>");
            w.println("<li>Temp table opt: 10k IDs loaded once into an indexed table; cursor pages use JOIN — eliminates the IN literal from every page query.</li>");
            w.println("<li>CH UPDATE/DELETE pattern: INSERT with same <code>unique_id</code> + newer <code>updated_at</code>; engine deduplicates on next background merge.</li>");
            w.println("</ul></div>");

            // ── CH vs CH FINAL SELECT table ────────────────────────────────
            w.println("<table>");
            w.println("<thead>");
            w.println("<tr class=\"tbl-title\"><th colspan=\"4\">ClickHouse: No-FINAL vs FINAL &mdash; ReplacingMergeTree Dedup Overhead</th></tr>");
            w.println("<tr class=\"col-hdr\"><th>Query &amp; SQL</th><th>CH No-FINAL (ms)</th><th>CH FINAL (ms)</th><th>FINAL Overhead</th></tr>");
            w.println("</thead><tbody>");
            int chfQi = 0;
            for (int i = 0; i < QUERY_PLAN.size(); i++) {
                String[] entry  = QUERY_PLAN.get(i);
                String[] fentry = CH_FINAL_QUERY_PLAN.get(i);
                if (entry[1] == null) {
                    w.printf("<tr class=\"sec\"><td colspan=\"4\">%s</td></tr>%n", h(entry[0]));
                } else {
                    String bothSql = "No-FINAL: " + displaySql(sqls.get(chfQi))
                                   + "  |  FINAL: " + displaySql(chFinalSqls.get(chfQi));
                    htmlChFinalRow(w, entry[0], bothSql, ch[chfQi], chFinal[chfQi]);
                    chfQi++;
                }
            }
            w.println("</tbody></table>");

            // ── CH vs CH FINAL cursor nav tables ───────────────────────────
            htmlChFinalCursorTable(w,
                "CH: No-FINAL vs FINAL &mdash; Cursor Nav Single Table  (" + CURSOR_PAGES + " pages &times; " + CURSOR_PAGE_SIZE + " rows)",
                h("SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails") +
                    " WHERE id &gt; {cursor} ORDER BY id LIMIT " + CURSOR_PAGE_SIZE,
                h("SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL") +
                    " WHERE is_deleted = 0 AND id &gt; {cursor} ORDER BY id LIMIT " + CURSOR_PAGE_SIZE,
                chCur, chFinalCur);

            htmlChFinalCursorTable(w,
                "CH: No-FINAL vs FINAL &mdash; Cursor Nav With JOIN  (3 AD tables)",
                h("SELECT " + JOIN_COLS + JOIN_CLAUSE) +
                    " WHERE g.id &gt; {cursor} ORDER BY g.id LIMIT " + CURSOR_PAGE_SIZE,
                h("SELECT " + JOIN_COLS + FINAL_JOIN_CLAUSE) +
                    " WHERE g.is_deleted = 0 AND g.id &gt; {cursor} ORDER BY g.id LIMIT " + CURSOR_PAGE_SIZE,
                chCurJ, chFinalCurJ);

            htmlChFinalCursorTable(w,
                "CH: No-FINAL vs FINAL &mdash; Cursor Nav IN Filter  (id IN 10k)",
                h("SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails") +
                    " WHERE id IN (" + h(IN_IDS_DISPLAY) + ") AND id &gt; {cursor} ORDER BY id LIMIT " + CURSOR_PAGE_SIZE,
                h("SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL") +
                    " WHERE is_deleted = 0 AND id IN (" + h(IN_IDS_DISPLAY) + ") AND id &gt; {cursor} ORDER BY id LIMIT " + CURSOR_PAGE_SIZE,
                chCurIn, chFinalCurIn);

            htmlChFinalCursorTable(w,
                "CH: No-FINAL vs FINAL &mdash; Cursor Nav IN Filter Temp Table",
                h("SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails g") +
                    " JOIN tmp_bench_ids t ON t.id = g.id WHERE g.id &gt; {cursor} ORDER BY g.id LIMIT " + CURSOR_PAGE_SIZE,
                h("SELECT " + ALL_COLS + " FROM ADSMUserGeneralDetails FINAL g") +
                    " JOIN tmp_bench_ids t ON t.id = g.id WHERE g.is_deleted = 0 AND g.id &gt; {cursor} ORDER BY g.id LIMIT " + CURSOR_PAGE_SIZE,
                chCurInTmp, chFinalCurInTmp);

            // ── FINAL notes ────────────────────────────────────────────────
            w.println("<div style=\"background:#fff;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.08);padding:16px 24px;font-size:.85rem;color:#444;line-height:1.7;margin-bottom:32px\">");
            w.println("<b>ClickHouse FINAL notes</b><ul style=\"margin:8px 0 0;padding-left:20px\">");
            w.println("<li><code>FINAL</code> forces the <code>ReplacingMergeTree</code> engine to merge duplicate versions on-the-fly during the query, keeping only the latest row per <code>ORDER BY</code> key (<code>unique_id</code>).</li>");
            w.println("<li><code>WHERE is_deleted = 0</code> excludes soft-deleted rows that were inserted with the upsert-delete pattern (<code>is_deleted=1 + newer updated_at</code>).</li>");
            w.println("<li><code>ADSMUserAccountDetails</code> and <code>ADSMUserExchangeDetails</code> are plain <code>MergeTree</code> — <code>FINAL</code> is not applied to them and their queries are identical between the two columns.</li>");
            w.println("<li>The overhead depends on how many unmerged duplicate versions exist in the table parts. After a full background merge the overhead approaches zero.</li>");
            w.println("</ul></div>");

            // ── Export benchmark section (from export_results.txt) ─────────
            Map<String, String> exp = loadExportResults();
            if (!exp.isEmpty()) {
                htmlExportSection(w, exp);
            }

            w.println("</body></html>");

        } catch (Exception e) {
            System.err.println("Warning: could not write HTML report: " + e.getMessage());
            return;
        }
        System.out.println("\nHTML report written to: " + HTML_FILE);
    }

    /** HTML-escapes &, <, > and " in a string. */
    private static String h(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }

    /** Emits one benchmark row; times are in milliseconds. */
    private static void htmlMsRow(PrintWriter w, String name, String sql, long mysql, long ch) {
        boolean chWins    = mysql > 0 && ch > 0 && ch    < mysql;
        boolean mysqlWins = mysql > 0 && ch > 0 && mysql < ch;
        String  winLabel  = mysql <= 0 || ch <= 0 ? "—"
                          : chWins    ? String.format("CH %.1fx faster",    (double) mysql / ch)
                          : mysqlWins ? String.format("MySQL %.1fx faster", (double) ch / mysql)
                          : "tie";
        w.println("<tr>");
        w.printf("<td><div class=\"qname\">%s</div><div class=\"qsql\">%s</div></td>%n",
            h(name), h(sql));
        w.printf("<td class=\"num%s\">%d ms</td>%n", mysqlWins ? " win" : "", mysql);
        w.printf("<td class=\"num%s\">%d ms</td>%n", chWins    ? " win" : "", ch);
        w.printf("<td class=\"%s\">%s</td>%n",
            (chWins || mysqlWins) ? "wlabel" : (mysql <= 0 || ch <= 0 ? "na" : "tie"), h(winLabel));
        w.println("</tr>");
    }

    /** Reads export_results.txt (written by ExportBenchmark) as a String map. */
    private static Map<String, String> loadExportResults() {
        Map<String, String> map = new LinkedHashMap<>();
        File f = new File("export_results.txt");
        if (!f.exists()) return map;
        try (BufferedReader r = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("#") || line.isEmpty()) continue;
                int eq = line.indexOf('=');
                if (eq > 0) map.put(line.substring(0, eq).trim(), line.substring(eq + 1).trim());
            }
        } catch (Exception e) {
            System.err.println("Warning: could not read export_results.txt: " + e.getMessage());
        }
        return map;
    }

    /** Renders the CSV Export Benchmark section using data from export_results.txt. */
    private static void htmlExportSection(PrintWriter w, Map<String, String> e) {
        // Helper: get long value, default 0
        java.util.function.Function<String, Long> lng =
            k -> { try { return Long.parseLong(e.getOrDefault(k, "0")); } catch (NumberFormatException x) { return 0L; } };

        long mysqlMs       = lng.apply("export.mysql.totalMs");
        long mysqlRows     = lng.apply("export.mysql.rowsExported");
        long mysqlBatches  = lng.apply("export.mysql.batchCount");
        long mysqlBytes    = lng.apply("export.mysql.fileSizeBytes");
        long mysqlMinB     = lng.apply("export.mysql.minBatchMs");
        long mysqlAvgB     = lng.apply("export.mysql.avgBatchMs");
        long mysqlMaxB     = lng.apply("export.mysql.maxBatchMs");
        String mysqlSql    = e.getOrDefault("export.mysql.sqlTemplate", "");

        long chMs          = lng.apply("export.ch.totalMs");
        long chRows        = lng.apply("export.ch.rowsExported");
        long chBatches     = lng.apply("export.ch.batchCount");
        long chBytes       = lng.apply("export.ch.fileSizeBytes");
        long chMinB        = lng.apply("export.ch.minBatchMs");
        long chAvgB        = lng.apply("export.ch.avgBatchMs");
        long chMaxB        = lng.apply("export.ch.maxBatchMs");
        String chSql       = e.getOrDefault("export.ch.sqlTemplate", "");

        long mysqlRps      = mysqlMs > 0 ? mysqlRows * 1000L / mysqlMs : 0;
        long chRps         = chMs    > 0 ? chRows    * 1000L / chMs    : 0;

        w.println("<table>");
        w.println("<thead>");
        w.println("<tr class=\"tbl-title\"><th colspan=\"4\">CSV Export Benchmark — cursor pagination (LIMIT 5,000 per batch)</th></tr>");
        w.println("<tr class=\"col-hdr\"><th>Metric &amp; SQL</th><th>MySQL</th><th>ClickHouse</th><th>Winner</th></tr>");
        w.println("</thead><tbody>");
        w.println("<tr class=\"sec\"><td colspan=\"4\">Export Summary</td></tr>");

        // Total time — lower is better
        htmlExportRow(w, "Total export time",
            "MySQL: <code>" + h(mysqlSql) + "</code><br>ClickHouse: <code>" + h(chSql) + "</code>",
            String.format("%,d ms", mysqlMs), String.format("%,d ms", chMs),
            mysqlMs, chMs, true);

        // Rows exported — informational (may differ due to is_deleted filter)
        w.printf("<tr><td><div class=\"qname\">Rows exported</div>" +
                 "<div class=\"qsql\">CH excludes is_deleted=1 rows (FINAL WHERE is_deleted = 0)</div></td>" +
                 "<td class=\"num\">%,d</td><td class=\"num\">%,d</td><td class=\"tie\">—</td></tr>%n",
                 mysqlRows, chRows);

        // Batches
        w.printf("<tr><td><div class=\"qname\">Batches (pages of %,d rows)</div></td>" +
                 "<td class=\"num\">%,d</td><td class=\"num\">%,d</td><td class=\"tie\">—</td></tr>%n",
                 5_000, mysqlBatches, chBatches);

        // File size
        htmlExportRow(w, "Output file size",
            "CSV file written row-by-row with RFC 4180 escaping",
            String.format("%.1f MB", mysqlBytes / 1_048_576.0),
            String.format("%.1f MB", chBytes    / 1_048_576.0),
            mysqlBytes, chBytes, true);

        // Throughput — higher is better
        htmlExportRow(w, "Throughput (rows/sec)",
            "Sustained read + write throughput over the full export",
            String.format("%,d rows/s", mysqlRps),
            String.format("%,d rows/s", chRps),
            mysqlRps, chRps, false);

        w.println("<tr class=\"sec\"><td colspan=\"4\">Per-Batch Timing</td></tr>");

        // Min/avg/max batch — lower is better
        htmlExportRow(w, "Min batch time", "Best single page (5,000 rows read + written to CSV)",
            mysqlMinB + " ms", chMinB + " ms", mysqlMinB, chMinB, true);
        htmlExportRow(w, "Avg batch time", "Mean page time across all batches",
            mysqlAvgB + " ms", chAvgB + " ms", mysqlAvgB, chAvgB, true);
        htmlExportRow(w, "Max batch time", "Slowest single page (5,000 rows)",
            mysqlMaxB + " ms", chMaxB + " ms", mysqlMaxB, chMaxB, true);

        w.println("</tbody></table>");

        // Export-specific notes box
        w.println("<div style=\"background:#fff;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.08);padding:16px 24px;font-size:.85rem;color:#444;line-height:1.7;margin-bottom:32px\">");
        w.println("<b>Export notes</b><ul style=\"margin:8px 0 0;padding-left:20px\">");
        w.println("<li>MySQL uses: <code>WHERE id &gt; {lastId} ORDER BY id LIMIT 5,000</code></li>");
        w.println("<li>ClickHouse uses: <code>WHERE unique_id &gt; '{lastUniqueId}' ORDER BY unique_id LIMIT 5,000</code> + <code>FINAL WHERE is_deleted = 0</code></li>");
        w.println("<li><code>unique_id</code> is the <code>ReplacingMergeTree ORDER BY</code> key — paginating by <code>unique_id</code> allows ClickHouse to use its sparse primary index (one index lookup per page), unlike paginating by <code>id</code> which requires a full scan per batch.</li>");
        w.println("<li><code>FINAL</code> forces ReplacingMergeTree dedup before scan and excludes <code>is_deleted=1</code> soft-deleted rows.</li>");
        w.println("<li>Each batch is an independent query — no open cursor or server-side stream is held between pages.</li>");
        w.println("</ul></div>");
    }

    /** Export row where values are pre-formatted strings; longs used only for winner comparison. */
    private static void htmlExportRow(PrintWriter w, String name, String desc,
                                       String mysqlStr, String chStr,
                                       long mysqlVal, long chVal,
                                       boolean lowerIsBetter) {
        boolean chWins    = mysqlVal > 0 && chVal > 0 &&
                            (lowerIsBetter ? chVal < mysqlVal : chVal > mysqlVal);
        boolean mysqlWins = mysqlVal > 0 && chVal > 0 &&
                            (lowerIsBetter ? mysqlVal < chVal : mysqlVal > chVal);
        String winLabel   = mysqlVal <= 0 || chVal <= 0 ? "—"
                          : chWins    ? String.format("CH %.1fx %s",    (double)(lowerIsBetter ? mysqlVal : chVal)    / (lowerIsBetter ? chVal : mysqlVal),    lowerIsBetter ? "faster" : "higher")
                          : mysqlWins ? String.format("MySQL %.1fx %s", (double)(lowerIsBetter ? chVal    : mysqlVal) / (lowerIsBetter ? mysqlVal : chVal), lowerIsBetter ? "faster" : "higher")
                          : "tie";
        w.println("<tr>");
        w.printf("<td><div class=\"qname\">%s</div><div class=\"qsql\">%s</div></td>%n", h(name), desc);
        w.printf("<td class=\"num%s\">%s</td>%n", mysqlWins ? " win" : "", mysqlStr);
        w.printf("<td class=\"num%s\">%s</td>%n", chWins    ? " win" : "", chStr);
        w.printf("<td class=\"%s\">%s</td>%n",
            (chWins || mysqlWins) ? "wlabel" : "tie", h(winLabel));
        w.println("</tr>");
    }

    /** Emits one CH no-FINAL vs CH FINAL row; times are in milliseconds. */
    private static void htmlChFinalRow(PrintWriter w, String name, String sql, long chNoFinal, long chFinal) {
        boolean noFinalWins = chNoFinal > 0 && chFinal > 0 && chNoFinal < chFinal;
        boolean finalWins   = chNoFinal > 0 && chFinal > 0 && chFinal   < chNoFinal;
        String winLabel     = chNoFinal <= 0 || chFinal <= 0 ? "—"
                            : noFinalWins ? String.format("No-FINAL %.1fx faster", (double) chFinal    / chNoFinal)
                            : finalWins   ? String.format("FINAL %.1fx faster",    (double) chNoFinal  / chFinal)
                            : "tie";
        w.println("<tr>");
        w.printf("<td><div class=\"qname\">%s</div><div class=\"qsql\">%s</div></td>%n", h(name), h(sql));
        w.printf("<td class=\"num%s\">%d ms</td>%n", noFinalWins ? " win" : "", chNoFinal);
        w.printf("<td class=\"num%s\">%d ms</td>%n", finalWins   ? " win" : "", chFinal);
        w.printf("<td class=\"%s\">%s</td>%n",
            (noFinalWins || finalWins) ? "wlabel" : (chNoFinal <= 0 || chFinal <= 0 ? "na" : "tie"), h(winLabel));
        w.println("</tr>");
    }

    /** Emits a CH no-FINAL vs CH FINAL cursor-nav table; times are in microseconds. */
    private static void htmlChFinalCursorTable(PrintWriter w, String title,
                                                String noFinalSqlHtml, String finalSqlHtml,
                                                long[] chNoFinal, long[] chFinal) {
        w.println("<table>");
        w.printf("<thead><tr class=\"tbl-title\"><th colspan=\"4\">%s</th></tr>%n", title);
        w.println("<tr class=\"col-hdr\"><th>Page &amp; SQL</th><th>CH No-FINAL (&mu;s)</th><th>CH FINAL (&mu;s)</th><th>FINAL Overhead</th></tr></thead><tbody>");
        for (int p = 0; p < chNoFinal.length; p++) {
            boolean noFinalWins = chNoFinal[p] > 0 && chFinal[p] > 0 && chNoFinal[p] < chFinal[p];
            boolean finalWins   = chNoFinal[p] > 0 && chFinal[p] > 0 && chFinal[p]   < chNoFinal[p];
            String winLabel     = chNoFinal[p] <= 0 || chFinal[p] <= 0 ? "—"
                                : noFinalWins ? String.format("No-FINAL %.1fx faster", (double) chFinal[p]   / chNoFinal[p])
                                : finalWins   ? String.format("FINAL %.1fx faster",    (double) chNoFinal[p] / chFinal[p])
                                : "tie";
            w.println("<tr>");
            w.printf("<td><div class=\"qname\">Nav page %d</div>" +
                     "<div class=\"qsql\"><b>No-FINAL:</b> %s<br><b>FINAL:</b> %s</div></td>%n",
                p + 1, noFinalSqlHtml, finalSqlHtml);
            w.printf("<td class=\"num%s\">%d &mu;s</td>%n", noFinalWins ? " win" : "", chNoFinal[p]);
            w.printf("<td class=\"num%s\">%d &mu;s</td>%n", finalWins   ? " win" : "", chFinal[p]);
            w.printf("<td class=\"%s\">%s</td>%n",
                (noFinalWins || finalWins) ? "wlabel" : (chNoFinal[p] <= 0 || chFinal[p] <= 0 ? "na" : "tie"), h(winLabel));
            w.println("</tr>");
        }
        w.println("</tbody></table>");
    }

    /** Emits a full cursor-nav section table; times are in microseconds. */
    private static void htmlCursorTable(PrintWriter w, String title,
                                         String sqlHtml, long[] mysql, long[] ch) {
        w.println("<table>");
        w.printf("<thead><tr class=\"tbl-title\"><th colspan=\"4\">%s</th></tr>%n", h(title));
        w.println("<tr class=\"col-hdr\"><th>Page &amp; SQL</th><th>MySQL (μs)</th><th>ClickHouse (μs)</th><th>Winner</th></tr></thead><tbody>");
        for (int p = 0; p < mysql.length; p++) {
            boolean chWins    = mysql[p] > 0 && ch[p] > 0 && ch[p]    < mysql[p];
            boolean mysqlWins = mysql[p] > 0 && ch[p] > 0 && mysql[p] < ch[p];
            String  winLabel  = mysql[p] <= 0 || ch[p] <= 0 ? "—"
                              : chWins    ? String.format("CH %.1fx faster",    (double) mysql[p] / ch[p])
                              : mysqlWins ? String.format("MySQL %.1fx faster", (double) ch[p] / mysql[p])
                              : "tie";
            w.println("<tr>");
            w.printf("<td><div class=\"qname\">Nav page %d</div><div class=\"qsql\">%s</div></td>%n",
                p + 1, sqlHtml);   // sqlHtml already escaped
            w.printf("<td class=\"num%s\">%d μs</td>%n", mysqlWins ? " win" : "", mysql[p]);
            w.printf("<td class=\"num%s\">%d μs</td>%n", chWins    ? " win" : "", ch[p]);
            w.printf("<td class=\"%s\">%s</td>%n",
                (chWins || mysqlWins) ? "wlabel" : (mysql[p] <= 0 || ch[p] <= 0 ? "na" : "tie"), h(winLabel));
            w.println("</tr>");
        }
        w.println("</tbody></table>");
    }
}
