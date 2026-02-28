import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class DataSeeder {

    private static final int TOTAL_ROWS = 1_000_000;
    private static final int BATCH_SIZE = 10_000;

    // ── user_activity (seed: 42) ──────────────────────────────────────────
    private static final String[] COUNTRIES    = {"US","GB","DE","FR","JP","IN","BR","CA","AU","MX"};
    private static final String[] EVENT_TYPES  = {"login","logout","purchase","view","click","search","share","download"};
    private static final String[] DEVICE_TYPES = {"mobile","desktop","tablet","smart_tv"};
    private static final String[] OS_LIST      = {"Windows","macOS","Linux","iOS","Android"};
    private static final String[] BROWSERS     = {"Chrome","Firefox","Safari","Edge","Opera"};
    private static final String[] PAGE_URLS    = {"/home","/products","/cart","/checkout","/profile",
                                                   "/search","/about","/contact","/blog","/faq"};
    private static final String[] REFERRERS    = {"direct","google.com","facebook.com","twitter.com",
                                                   "instagram.com","youtube.com","linkedin.com","bing.com"};

    // ── activity_geo (seed: 43) ───────────────────────────────────────────
    private static final String[] CITIES  = {
        "New York","London","Berlin","Paris","Tokyo",
        "Mumbai","Sao Paulo","Toronto","Sydney","Mexico City",
        "Shanghai","Dubai","Singapore","Amsterdam","Seoul"
    };
    private static final String[] REGIONS = {
        "Northeast","South","Midwest","West","East",
        "North","Central","Southeast","Northwest","Southwest"
    };
    private static final String[] ISPS = {
        "Comcast","AT&T","Verizon","Deutsche Telekom","BT",
        "Orange","Jio","NTT","Telstra","Rogers"
    };

    // ── activity_device (seed: 44) ────────────────────────────────────────
    private static final int[][] SCREEN_SIZES = {
        {375,812},{390,844},{414,896},{768,1024},{1280,800},
        {1366,768},{1440,900},{1920,1080},{2560,1440},{360,640}
    };
    private static final String[] LANGUAGES = {
        "en-US","en-GB","de-DE","fr-FR","ja-JP",
        "es-ES","pt-BR","zh-CN","ko-KR","ar-SA"
    };
    private static final String[] TIMEZONES = {
        "America/New_York","America/Los_Angeles","Europe/London","Europe/Berlin","Asia/Tokyo",
        "Asia/Kolkata","America/Sao_Paulo","Australia/Sydney","Asia/Shanghai","America/Toronto"
    };
    private static final String[] CONNECTION_TYPES = {"wifi","4g","5g","3g","ethernet","2g"};

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        System.out.println("=== DataSeeder: seeding " + TOTAL_ROWS + " rows (3 tables) into MySQL and ClickHouse ===\n");
        seedMySQL();
        seedClickHouse();
        System.out.println("\n=== Seeding complete ===");
    }

    // ════════════════════════════════════════════════════════════════════════
    // MySQL
    // ════════════════════════════════════════════════════════════════════════

    private static void seedMySQL() throws Exception {
        String url = "jdbc:mysql://localhost:3306/?rewriteBatchedStatements=true"
                   + "&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

        System.out.println("[MySQL] Connecting...");
        try (Connection conn = DriverManager.getConnection(url, "root", "")) {
            conn.setAutoCommit(false);

            // ── Schema ───────────────────────────────────────────────────────
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE IF NOT EXISTS reports_db");
                s.execute("USE reports_db");
                // drop in reverse FK order
                s.execute("DROP TABLE IF EXISTS activity_device");
                s.execute("DROP TABLE IF EXISTS activity_geo");
                s.execute("DROP TABLE IF EXISTS user_activity");

                s.execute(
                    "CREATE TABLE user_activity (" +
                    "  id                BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                    "  user_id           VARCHAR(36)   NOT NULL," +
                    "  country           VARCHAR(50)   NOT NULL," +
                    "  event_type        VARCHAR(50)   NOT NULL," +
                    "  duration_ms       INT           NOT NULL," +
                    "  created_at        DATETIME      NOT NULL," +
                    "  session_id        VARCHAR(50)   NOT NULL," +
                    "  device_type       VARCHAR(20)   NOT NULL," +
                    "  os                VARCHAR(30)   NOT NULL," +
                    "  browser           VARCHAR(30)   NOT NULL," +
                    "  page_url          VARCHAR(200)  NOT NULL," +
                    "  referrer          VARCHAR(200)  NOT NULL," +
                    "  ip_address        VARCHAR(15)   NOT NULL," +
                    "  response_time_ms  INT           NOT NULL," +
                    "  bytes_transferred INT           NOT NULL," +
                    "  is_error          TINYINT(1)    NOT NULL," +
                    "  INDEX idx_country(country)," +
                    "  INDEX idx_duration(duration_ms)" +
                    ") ENGINE=InnoDB"
                );

                s.execute(
                    "CREATE TABLE activity_geo (" +
                    "  activity_id  BIGINT        NOT NULL PRIMARY KEY," +
                    "  city         VARCHAR(100)  NOT NULL," +
                    "  region       VARCHAR(50)   NOT NULL," +
                    "  latitude     FLOAT         NOT NULL," +
                    "  longitude    FLOAT         NOT NULL," +
                    "  isp          VARCHAR(50)   NOT NULL," +
                    "  INDEX idx_city(city)," +
                    "  INDEX idx_isp(isp)" +
                    ") ENGINE=InnoDB"
                );

                s.execute(
                    "CREATE TABLE activity_device (" +
                    "  activity_id     BIGINT       NOT NULL PRIMARY KEY," +
                    "  screen_width    SMALLINT     NOT NULL," +
                    "  screen_height   SMALLINT     NOT NULL," +
                    "  language        VARCHAR(10)  NOT NULL," +
                    "  timezone        VARCHAR(50)  NOT NULL," +
                    "  connection_type VARCHAR(20)  NOT NULL," +
                    "  INDEX idx_language(language)," +
                    "  INDEX idx_connection(connection_type)" +
                    ") ENGINE=InnoDB"
                );
            }
            conn.commit();
            System.out.println("[MySQL] Schema ready (3 tables).");

            // ── user_activity ─────────────────────────────────────────────────
            System.out.println("[MySQL] Seeding user_activity...");
            String uaInsert =
                "INSERT INTO user_activity " +
                "(user_id, country, event_type, duration_ms, created_at," +
                " session_id, device_type, os, browser, page_url, referrer," +
                " ip_address, response_time_ms, bytes_transferred, is_error)" +
                " VALUES (?,?,?,?,?, ?,?,?,?,?,?, ?,?,?,?)";
            Random rng1 = new Random(42);
            LocalDateTime now = LocalDateTime.now();
            try (PreparedStatement ps = conn.prepareStatement(uaInsert)) {
                for (int i = 0; i < TOTAL_ROWS; i++) {
                    bindUserActivity(ps, rng1, now, false);
                    ps.addBatch();
                    if ((i + 1) % BATCH_SIZE == 0) {
                        ps.executeBatch();
                        conn.commit();
                        System.out.printf("  user_activity: %,d / %,d%n", i + 1, TOTAL_ROWS);
                    }
                }
                ps.executeBatch();
                conn.commit();
            }

            // ── activity_geo ──────────────────────────────────────────────────
            System.out.println("[MySQL] Seeding activity_geo...");
            seedGeoTable(conn,
                "INSERT INTO activity_geo (activity_id, city, region, latitude, longitude, isp)" +
                " VALUES (?,?,?,?,?,?)", true);

            // ── activity_device ───────────────────────────────────────────────
            System.out.println("[MySQL] Seeding activity_device...");
            seedDeviceTable(conn,
                "INSERT INTO activity_device (activity_id, screen_width, screen_height, language, timezone, connection_type)" +
                " VALUES (?,?,?,?,?,?)", true);
        }
        System.out.println("[MySQL] Done.\n");
    }

    // ════════════════════════════════════════════════════════════════════════
    // ClickHouse
    // ════════════════════════════════════════════════════════════════════════

    private static void seedClickHouse() throws Exception {
        String url = "jdbc:clickhouse://localhost:8123/default?compress=0";

        System.out.println("[ClickHouse] Connecting...");
        try (Connection conn = DriverManager.getConnection(url, "default", "")) {

            // ── Schema ───────────────────────────────────────────────────────
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE IF NOT EXISTS reports_db");
                s.execute("DROP TABLE IF EXISTS reports_db.activity_device");
                s.execute("DROP TABLE IF EXISTS reports_db.activity_geo");
                s.execute("DROP TABLE IF EXISTS reports_db.user_activity");

                s.execute(
                    "CREATE TABLE reports_db.user_activity (" +
                    "  id                UInt64,"   +
                    "  user_id           String,"   +
                    "  country           String,"   +
                    "  event_type        String,"   +
                    "  duration_ms       UInt32,"   +
                    "  created_at        DateTime," +
                    "  session_id        String,"   +
                    "  device_type       String,"   +
                    "  os                String,"   +
                    "  browser           String,"   +
                    "  page_url          String,"   +
                    "  referrer          String,"   +
                    "  ip_address        String,"   +
                    "  response_time_ms  UInt32,"   +
                    "  bytes_transferred UInt32,"   +
                    "  is_error          UInt8,"    +
                    "  event_date        Date,"     +   // dedup key component
                    "  updated_at        DateTime," +   // version column for ReplacingMergeTree
                    "  is_deleted        UInt8"     +   // 1 = soft-deleted
                    ") ENGINE = ReplacingMergeTree(updated_at)" +
                    " ORDER BY (user_id, event_date)"
                );

                s.execute(
                    "CREATE TABLE reports_db.activity_geo (" +
                    "  activity_id  UInt64,"  +
                    "  city         String,"  +
                    "  region       String,"  +
                    "  latitude     Float32," +
                    "  longitude    Float32," +
                    "  isp          String"   +
                    ") ENGINE = MergeTree() ORDER BY activity_id"
                );

                s.execute(
                    "CREATE TABLE reports_db.activity_device (" +
                    "  activity_id     UInt64," +
                    "  screen_width    UInt16," +
                    "  screen_height   UInt16," +
                    "  language        String," +
                    "  timezone        String," +
                    "  connection_type String"  +
                    ") ENGINE = MergeTree() ORDER BY activity_id"
                );
            }
            System.out.println("[ClickHouse] Schema ready (3 tables).");

            // ── user_activity ─────────────────────────────────────────────────
            System.out.println("[ClickHouse] Seeding user_activity...");
            String uaInsert =
                "INSERT INTO reports_db.user_activity " +
                "(id, user_id, country, event_type, duration_ms, created_at," +
                " session_id, device_type, os, browser, page_url, referrer," +
                " ip_address, response_time_ms, bytes_transferred, is_error," +
                " event_date, updated_at, is_deleted)" +
                " VALUES (?,?,?,?,?,?, ?,?,?,?,?,?, ?,?,?,?, ?,?,?)";
            Random rng1 = new Random(42);   // same seed → identical data
            LocalDateTime now = LocalDateTime.now();
            try (PreparedStatement ps = conn.prepareStatement(uaInsert)) {
                for (int i = 0; i < TOTAL_ROWS; i++) {
                    ps.setLong(1, i + 1L);
                    bindUserActivity(ps, rng1, now, true);
                    ps.addBatch();
                    if ((i + 1) % BATCH_SIZE == 0) {
                        ps.executeBatch();
                        System.out.printf("  user_activity: %,d / %,d%n", i + 1, TOTAL_ROWS);
                    }
                }
                ps.executeBatch();
            }

            // ── activity_geo ──────────────────────────────────────────────────
            System.out.println("[ClickHouse] Seeding activity_geo...");
            seedGeoTable(conn,
                "INSERT INTO reports_db.activity_geo (activity_id, city, region, latitude, longitude, isp)" +
                " VALUES (?,?,?,?,?,?)", false);

            // ── activity_device ───────────────────────────────────────────────
            System.out.println("[ClickHouse] Seeding activity_device...");
            seedDeviceTable(conn,
                "INSERT INTO reports_db.activity_device (activity_id, screen_width, screen_height, language, timezone, connection_type)" +
                " VALUES (?,?,?,?,?,?)", false);
        }
        System.out.println("[ClickHouse] Done.");
    }

    // ════════════════════════════════════════════════════════════════════════
    // Shared seeders — same Random seed → identical data in both DBs
    // ════════════════════════════════════════════════════════════════════════

    private static void seedGeoTable(Connection conn, String sql,
                                      boolean commitPerBatch) throws Exception {
        Random rng = new Random(43);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < TOTAL_ROWS; i++) {
                ps.setLong  (1, i + 1L);
                ps.setString(2, CITIES[rng.nextInt(CITIES.length)]);
                ps.setString(3, REGIONS[rng.nextInt(REGIONS.length)]);
                ps.setDouble(4, rng.nextDouble() * 180 - 90);    // latitude  -90..90
                ps.setDouble(5, rng.nextDouble() * 360 - 180);   // longitude -180..180
                ps.setString(6, ISPS[rng.nextInt(ISPS.length)]);
                ps.addBatch();
                if ((i + 1) % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    if (commitPerBatch) conn.commit();
                    System.out.printf("  activity_geo: %,d / %,d%n", i + 1, TOTAL_ROWS);
                }
            }
            ps.executeBatch();
            if (commitPerBatch) conn.commit();
        }
    }

    private static void seedDeviceTable(Connection conn, String sql,
                                         boolean commitPerBatch) throws Exception {
        Random rng = new Random(44);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < TOTAL_ROWS; i++) {
                int[] sz = SCREEN_SIZES[rng.nextInt(SCREEN_SIZES.length)];
                ps.setLong  (1, i + 1L);
                ps.setInt   (2, sz[0]);
                ps.setInt   (3, sz[1]);
                ps.setString(4, LANGUAGES[rng.nextInt(LANGUAGES.length)]);
                ps.setString(5, TIMEZONES[rng.nextInt(TIMEZONES.length)]);
                ps.setString(6, CONNECTION_TYPES[rng.nextInt(CONNECTION_TYPES.length)]);
                ps.addBatch();
                if ((i + 1) % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    if (commitPerBatch) conn.commit();
                    System.out.printf("  activity_device: %,d / %,d%n", i + 1, TOTAL_ROWS);
                }
            }
            ps.executeBatch();
            if (commitPerBatch) conn.commit();
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // bindUserActivity — keeps RNG call order identical for MySQL & ClickHouse
    //   withIdOffset=false → MySQL: id is AUTO_INCREMENT, data starts at param 1
    //   withIdOffset=true  → ClickHouse: id already bound at param 1, data at 2
    // ════════════════════════════════════════════════════════════════════════

    private static void bindUserActivity(PreparedStatement ps, Random rng,
                                          LocalDateTime now, boolean withIdOffset)
                                          throws SQLException {
        int p = withIdOffset ? 2 : 1;
        ps.setString(p++, "user_"  + (rng.nextInt(100_000) + 1));
        ps.setString(p++, COUNTRIES[rng.nextInt(COUNTRIES.length)]);
        ps.setString(p++, EVENT_TYPES[rng.nextInt(EVENT_TYPES.length)]);
        ps.setInt   (p++, rng.nextInt(10_000));
        LocalDateTime created = now.minusSeconds(rng.nextInt(30 * 24 * 3600));
        ps.setString(p++, created.format(FMT));                // created_at
        ps.setString(p++, "sess_"  + (rng.nextInt(500_000) + 1));
        ps.setString(p++, DEVICE_TYPES[rng.nextInt(DEVICE_TYPES.length)]);
        ps.setString(p++, OS_LIST[rng.nextInt(OS_LIST.length)]);
        ps.setString(p++, BROWSERS[rng.nextInt(BROWSERS.length)]);
        ps.setString(p++, PAGE_URLS[rng.nextInt(PAGE_URLS.length)]);
        ps.setString(p++, REFERRERS[rng.nextInt(REFERRERS.length)]);
        ps.setString(p++, rng.nextInt(256) + "." + rng.nextInt(256) + "."
                        + rng.nextInt(256) + "." + rng.nextInt(256));
        ps.setInt   (p++, rng.nextInt(2_000));
        ps.setInt   (p++, rng.nextInt(1_000_000));
        ps.setInt   (p++, rng.nextInt(10) == 0 ? 1 : 0);      // is_error
        if (withIdOffset) {
            // ClickHouse-only: ReplacingMergeTree version + soft-delete columns
            ps.setString(p++, created.toLocalDate().toString()); // event_date
            ps.setString(p++, created.format(FMT));              // updated_at = created_at at seed time
            ps.setInt   (p,   0);                                // is_deleted = 0
        }
    }
}
