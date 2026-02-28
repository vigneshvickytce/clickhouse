import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class DataSeeder {

    private static final int TOTAL_ROWS = 1_000_000;
    private static final int BATCH_SIZE = 10_000;

    // ── ADSMUserGeneralDetails (seed: 42) ─────────────────────────────────────
    private static final String[] FIRSTNAMES = {
        "John","Jane","Michael","Sarah","David","Emily","Robert","Lisa",
        "William","Jennifer","James","Amanda","Christopher","Jessica","Matthew",
        "Daniel","Ashley","Andrew","Stephanie","Joshua","Ryan","Nicole",
        "Kevin","Rachel","Tyler","Megan","Brandon","Lauren","Nathan","Hannah"
    };
    private static final String[] LASTNAMES = {
        "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
        "Martinez","Wilson","Anderson","Taylor","Thomas","Hernandez","Moore",
        "Jackson","Martin","Lee","Perez","Thompson","White","Harris","Sanchez",
        "Clark","Ramirez","Lewis","Robinson","Walker","Young","Allen"
    };
    private static final String[] DEPARTMENTS = {
        "IT","HR","Finance","Marketing","Sales","Operations","Legal","Engineering","Support","Management"
    };
    private static final String[] TITLES = {
        "Manager","Director","Developer","Analyst","Engineer",
        "Consultant","Administrator","Specialist","Coordinator","Executive"
    };

    // ── ADSMUserAccountDetails (seed: 43) ─────────────────────────────────────
    private static final String[] DOMAINS = {
        "example.com","corp.local","company.org","enterprise.net","business.com"
    };
    private static final String[] MACHINE_PREFIXES = {
        "DESKTOP","LAPTOP","WORKSTATION","THIN-CLIENT"
    };

    // ── ADSMUserExchangeDetails (seed: 44) ────────────────────────────────────
    private static final String[] MAILBOX_DBS = {
        "MBDB01","MBDB02","MBDB03","MBDB04","MBDB05"
    };
    private static final String[] MAILBOX_PROPS = {
        "ActiveSync,OWA,MAPI",
        "OWA,MAPI",
        "ActiveSync,MAPI",
        "MAPI only",
        "ActiveSync,OWA,MAPI,POP3",
        "OWA only"
    };
    private static final int[] QUOTA_OPTIONS = {1024, 2048, 5120, 10240, 20480};

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // ════════════════════════════════════════════════════════════════════════
    // main
    // ════════════════════════════════════════════════════════════════════════

    public static void main(String[] args) throws Exception {
        System.out.println("=== DataSeeder: seeding " + TOTAL_ROWS + " rows (3 AD tables) into MySQL and ClickHouse ===\n");
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

            try (Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE IF NOT EXISTS reports_db");
                s.execute("USE reports_db");
                // drop in reverse dependency order
                s.execute("DROP TABLE IF EXISTS ADSMUserAccountDetails");
                s.execute("DROP TABLE IF EXISTS ADSMUserExchangeDetails");
                s.execute("DROP TABLE IF EXISTS ADSMUserGeneralDetails");

                s.execute(
                    "CREATE TABLE ADSMUserGeneralDetails (" +
                    "  id                BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                    "  unique_id         VARCHAR(36)   NOT NULL," +
                    "  object_guid       VARCHAR(36)   NOT NULL," +
                    "  sam_account_name  VARCHAR(50)   NOT NULL," +
                    "  name              VARCHAR(100)  NOT NULL," +
                    "  firstname         VARCHAR(50)   NOT NULL," +
                    "  lastname          VARCHAR(50)   NOT NULL," +
                    "  initial           VARCHAR(5)    NOT NULL," +
                    "  display_name      VARCHAR(100)  NOT NULL," +
                    "  distinguished_name VARCHAR(255) NOT NULL," +
                    "  department        VARCHAR(100)  NOT NULL," +
                    "  title             VARCHAR(100)  NOT NULL," +
                    "  UNIQUE KEY uk_unique_id(unique_id)," +
                    "  INDEX idx_department(department)," +
                    "  INDEX idx_name(name)" +
                    ") ENGINE=InnoDB"
                );

                s.execute(
                    "CREATE TABLE ADSMUserAccountDetails (" +
                    "  unique_id          VARCHAR(36)   NOT NULL PRIMARY KEY," +
                    "  logon_name         VARCHAR(100)  NOT NULL," +
                    "  logon_to_machine   VARCHAR(50)   NOT NULL," +
                    "  last_logon_time    DATETIME      NOT NULL," +
                    "  account_expires    DATETIME      NOT NULL," +
                    "  pwd_last_set       DATETIME      NOT NULL," +
                    "  account_enabled    TINYINT(1)    NOT NULL," +
                    "  INDEX idx_logon_name(logon_name)," +
                    "  INDEX idx_enabled(account_enabled)" +
                    ") ENGINE=InnoDB"
                );

                s.execute(
                    "CREATE TABLE ADSMUserExchangeDetails (" +
                    "  unique_id              VARCHAR(36)   NOT NULL PRIMARY KEY," +
                    "  mailbox_name           VARCHAR(100)  NOT NULL," +
                    "  mailbox_database_name  VARCHAR(50)   NOT NULL," +
                    "  email_address          VARCHAR(100)  NOT NULL," +
                    "  quota_mb               INT           NOT NULL," +
                    "  mailbox_properties     VARCHAR(255)  NOT NULL," +
                    "  INDEX idx_mailbox_db(mailbox_database_name)," +
                    "  INDEX idx_email(email_address)" +
                    ") ENGINE=InnoDB"
                );
            }
            conn.commit();
            System.out.println("[MySQL] Schema ready (3 AD tables).");

            LocalDateTime now = LocalDateTime.now();

            System.out.println("[MySQL] Seeding ADSMUserGeneralDetails...");
            seedGeneralDetails(conn,
                "INSERT INTO ADSMUserGeneralDetails " +
                "(unique_id, object_guid, sam_account_name, name, firstname, lastname, initial," +
                " display_name, distinguished_name, department, title)" +
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                false, true);

            System.out.println("[MySQL] Seeding ADSMUserAccountDetails...");
            seedAccountDetails(conn,
                "INSERT INTO ADSMUserAccountDetails " +
                "(unique_id, logon_name, logon_to_machine, last_logon_time," +
                " account_expires, pwd_last_set, account_enabled)" +
                " VALUES (?,?,?,?,?,?,?)",
                true, now);

            System.out.println("[MySQL] Seeding ADSMUserExchangeDetails...");
            seedExchangeDetails(conn,
                "INSERT INTO ADSMUserExchangeDetails " +
                "(unique_id, mailbox_name, mailbox_database_name, email_address, quota_mb, mailbox_properties)" +
                " VALUES (?,?,?,?,?,?)",
                true);
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

            try (Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE IF NOT EXISTS reports_db");
                s.execute("DROP TABLE IF EXISTS reports_db.ADSMUserAccountDetails");
                s.execute("DROP TABLE IF EXISTS reports_db.ADSMUserExchangeDetails");
                s.execute("DROP TABLE IF EXISTS reports_db.ADSMUserGeneralDetails");

                // Primary table: ReplacingMergeTree for upsert/soft-delete support
                s.execute(
                    "CREATE TABLE reports_db.ADSMUserGeneralDetails (" +
                    "  id                UInt64,"   +
                    "  unique_id         String,"   +
                    "  object_guid       String,"   +
                    "  sam_account_name  String,"   +
                    "  name              String,"   +
                    "  firstname         String,"   +
                    "  lastname          String,"   +
                    "  initial           String,"   +
                    "  display_name      String,"   +
                    "  distinguished_name String,"  +
                    "  department        String,"   +
                    "  title             String,"   +
                    "  is_deleted        UInt8,"    +
                    "  updated_at        DateTime"  +
                    ") ENGINE = ReplacingMergeTree(updated_at)" +
                    " ORDER BY (unique_id)"
                );

                // Reference tables: plain MergeTree (read-only in benchmark)
                s.execute(
                    "CREATE TABLE reports_db.ADSMUserAccountDetails (" +
                    "  unique_id          String,"   +
                    "  logon_name         String,"   +
                    "  logon_to_machine   String,"   +
                    "  last_logon_time    DateTime," +
                    "  account_expires    DateTime," +
                    "  pwd_last_set       DateTime," +
                    "  account_enabled    UInt8"     +
                    ") ENGINE = MergeTree() ORDER BY (unique_id)"
                );

                s.execute(
                    "CREATE TABLE reports_db.ADSMUserExchangeDetails (" +
                    "  unique_id              String,"  +
                    "  mailbox_name           String,"  +
                    "  mailbox_database_name  String,"  +
                    "  email_address          String,"  +
                    "  quota_mb               UInt32,"  +
                    "  mailbox_properties     String"   +
                    ") ENGINE = MergeTree() ORDER BY (unique_id)"
                );
            }
            System.out.println("[ClickHouse] Schema ready (3 AD tables).");

            LocalDateTime now = LocalDateTime.now();

            System.out.println("[ClickHouse] Seeding ADSMUserGeneralDetails...");
            seedGeneralDetails(conn,
                "INSERT INTO reports_db.ADSMUserGeneralDetails " +
                "(id, unique_id, object_guid, sam_account_name, name, firstname, lastname, initial," +
                " display_name, distinguished_name, department, title, is_deleted, updated_at)" +
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                true, false);

            System.out.println("[ClickHouse] Seeding ADSMUserAccountDetails...");
            seedAccountDetails(conn,
                "INSERT INTO reports_db.ADSMUserAccountDetails " +
                "(unique_id, logon_name, logon_to_machine, last_logon_time," +
                " account_expires, pwd_last_set, account_enabled)" +
                " VALUES (?,?,?,?,?,?,?)",
                false, now);

            System.out.println("[ClickHouse] Seeding ADSMUserExchangeDetails...");
            seedExchangeDetails(conn,
                "INSERT INTO reports_db.ADSMUserExchangeDetails " +
                "(unique_id, mailbox_name, mailbox_database_name, email_address, quota_mb, mailbox_properties)" +
                " VALUES (?,?,?,?,?,?)",
                false);
        }
        System.out.println("[ClickHouse] Done.");
    }

    // ════════════════════════════════════════════════════════════════════════
    // Shared seeders — fixed Random seeds → identical data in both DBs
    // ════════════════════════════════════════════════════════════════════════

    private static void seedGeneralDetails(Connection conn, String sql,
                                            boolean isClickHouse, boolean commitPerBatch)
                                            throws Exception {
        Random rng = new Random(42);
        LocalDateTime seedTime = LocalDateTime.now();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < TOTAL_ROWS; i++) {
                bindGeneralDetails(ps, rng, i + 1L, isClickHouse, seedTime);
                ps.addBatch();
                if ((i + 1) % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    if (commitPerBatch) conn.commit();
                    System.out.printf("  ADSMUserGeneralDetails: %,d / %,d%n", i + 1, TOTAL_ROWS);
                }
            }
            ps.executeBatch();
            if (commitPerBatch) conn.commit();
        }
    }

    private static void seedAccountDetails(Connection conn, String sql,
                                             boolean commitPerBatch, LocalDateTime now)
                                             throws Exception {
        Random rng = new Random(43);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < TOTAL_ROWS; i++) {
                bindAccountDetails(ps, rng, i + 1L, now);
                ps.addBatch();
                if ((i + 1) % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    if (commitPerBatch) conn.commit();
                    System.out.printf("  ADSMUserAccountDetails: %,d / %,d%n", i + 1, TOTAL_ROWS);
                }
            }
            ps.executeBatch();
            if (commitPerBatch) conn.commit();
        }
    }

    private static void seedExchangeDetails(Connection conn, String sql,
                                              boolean commitPerBatch) throws Exception {
        Random rng = new Random(44);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < TOTAL_ROWS; i++) {
                bindExchangeDetails(ps, rng, i + 1L);
                ps.addBatch();
                if ((i + 1) % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    if (commitPerBatch) conn.commit();
                    System.out.printf("  ADSMUserExchangeDetails: %,d / %,d%n", i + 1, TOTAL_ROWS);
                }
            }
            ps.executeBatch();
            if (commitPerBatch) conn.commit();
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Row binders
    // ════════════════════════════════════════════════════════════════════════

    private static void bindGeneralDetails(PreparedStatement ps, Random rng,
                                            long rowNum, boolean isClickHouse,
                                            LocalDateTime seedTime) throws SQLException {
        String uniqueId   = String.format("uid-%010d", rowNum);
        String objectGuid = String.format("%08x-0000-0000-0000-%012x", rowNum, rowNum);
        String fname      = FIRSTNAMES[rng.nextInt(FIRSTNAMES.length)];
        String lname      = LASTNAMES[rng.nextInt(LASTNAMES.length)];
        String dept       = DEPARTMENTS[rng.nextInt(DEPARTMENTS.length)];
        String title      = TITLES[rng.nextInt(TITLES.length)];
        String samName    = (Character.toLowerCase(fname.charAt(0)) + lname).toLowerCase();
        String fullName   = fname + " " + lname;
        String dn         = "CN=" + fullName + ",OU=" + dept + ",DC=example,DC=com";

        int p = 1;
        if (isClickHouse) ps.setLong(p++, rowNum);       // id (CH only)
        ps.setString(p++, uniqueId);
        ps.setString(p++, objectGuid);
        ps.setString(p++, samName);
        ps.setString(p++, fullName);
        ps.setString(p++, fname);
        ps.setString(p++, lname);
        ps.setString(p++, String.valueOf(fname.charAt(0)));
        ps.setString(p++, fullName);                     // display_name = full name
        ps.setString(p++, dn);
        ps.setString(p++, dept);
        ps.setString(p++, title);
        if (isClickHouse) {
            ps.setInt   (p++, 0);                        // is_deleted = 0
            ps.setString(p,   seedTime.format(FMT));     // updated_at
        }
    }

    private static void bindAccountDetails(PreparedStatement ps, Random rng,
                                            long rowNum, LocalDateTime now) throws SQLException {
        String uniqueId = String.format("uid-%010d", rowNum);
        String fname    = FIRSTNAMES[rng.nextInt(FIRSTNAMES.length)];
        String domain   = DOMAINS[rng.nextInt(DOMAINS.length)];
        String machine  = MACHINE_PREFIXES[rng.nextInt(MACHINE_PREFIXES.length)]
                        + "-" + String.format("%06X", rng.nextInt(0x1000000));
        LocalDateTime lastLogon   = now.minusSeconds(rng.nextInt(90 * 24 * 3600));
        LocalDateTime acctExpires = now.plusDays(rng.nextInt(365) + 1);
        LocalDateTime pwdLastSet  = now.minusSeconds(rng.nextInt(180 * 24 * 3600));
        int enabled = rng.nextInt(10) < 9 ? 1 : 0;   // 90% enabled

        ps.setString(1, uniqueId);
        ps.setString(2, fname.toLowerCase() + "@" + domain);
        ps.setString(3, machine);
        ps.setString(4, lastLogon.format(FMT));
        ps.setString(5, acctExpires.format(FMT));
        ps.setString(6, pwdLastSet.format(FMT));
        ps.setInt   (7, enabled);
    }

    private static void bindExchangeDetails(PreparedStatement ps, Random rng,
                                             long rowNum) throws SQLException {
        String uniqueId = String.format("uid-%010d", rowNum);
        String fname    = FIRSTNAMES[rng.nextInt(FIRSTNAMES.length)];
        String lname    = LASTNAMES[rng.nextInt(LASTNAMES.length)];
        String domain   = DOMAINS[rng.nextInt(DOMAINS.length)];
        String mbxName  = fname.toLowerCase() + "." + lname.toLowerCase();
        String mbxDb    = MAILBOX_DBS[rng.nextInt(MAILBOX_DBS.length)];
        String email    = mbxName + "@" + domain;
        int quotaMb     = QUOTA_OPTIONS[rng.nextInt(QUOTA_OPTIONS.length)];
        String mbxProps = MAILBOX_PROPS[rng.nextInt(MAILBOX_PROPS.length)];

        ps.setString(1, uniqueId);
        ps.setString(2, mbxName);
        ps.setString(3, mbxDb);
        ps.setString(4, email);
        ps.setInt   (5, quotaMb);
        ps.setString(6, mbxProps);
    }
}
