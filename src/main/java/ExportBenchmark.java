import java.io.*;
import java.sql.*;

/**
 * ExportBenchmark — simulates a report-module CSV export for both MySQL and ClickHouse.
 *
 * Flow (mirrors production export pattern):
 *   1. Execute: SELECT ... WHERE id > {cursor} ORDER BY id LIMIT 5000
 *   2. Write the batch to CSV file.
 *   3. Advance cursor to the last id in the batch.
 *   4. Repeat until the query returns 0 rows.
 *   5. Print per-batch progress and a final side-by-side comparison.
 *
 * Run:
 *   mvn exec:java -Dexec.mainClass=ExportBenchmark
 */
public class ExportBenchmark {

    // ── Connection config ─────────────────────────────────────────────────────
    private static final String MYSQL_URL  =
        "jdbc:mysql://localhost:3306/reports_db" +
        "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASS = "";

    private static final String CH_URL  = "jdbc:clickhouse://localhost:8123/reports_db?compress=0";
    private static final String CH_USER = "default";
    private static final String CH_PASS = "";

    // ── Export config ─────────────────────────────────────────────────────────
    private static final int    BATCH_SIZE   = 5_000;
    private static final String MYSQL_OUTPUT = "export_mysql.csv";
    private static final String CH_OUTPUT    = "export_clickhouse.csv";

    // 16 common columns (no ReplacingMergeTree internals)
    private static final String ALL_COLS =
        "id, user_id, country, event_type, duration_ms, created_at, " +
        "session_id, device_type, os, browser, page_url, referrer, " +
        "ip_address, response_time_ms, bytes_transferred, is_error";

    // %d = cursor (last id from previous batch); starts at 0
    private static final String MYSQL_SQL_TPL =
        "SELECT " + ALL_COLS +
        " FROM user_activity WHERE id > %d ORDER BY id LIMIT " + BATCH_SIZE;

    // ClickHouse: FINAL deduplicates ReplacingMergeTree; exclude soft-deleted rows
    private static final String CH_SQL_TPL =
        "SELECT " + ALL_COLS +
        " FROM user_activity FINAL WHERE is_deleted = 0 AND id > %d ORDER BY id LIMIT " + BATCH_SIZE;

    private static final String CSV_HEADER = ALL_COLS.replace(" ", "");

    // ── Result container ──────────────────────────────────────────────────────
    static class ExportResult {
        final String db;
        final long   totalMs;
        final long   rowsExported;
        final int    batchCount;
        final long   fileSizeBytes;

        ExportResult(String db, long totalMs, long rowsExported,
                     int batchCount, long fileSizeBytes) {
            this.db            = db;
            this.totalMs       = totalMs;
            this.rowsExported  = rowsExported;
            this.batchCount    = batchCount;
            this.fileSizeBytes = fileSizeBytes;
        }

        double rowsPerSec() {
            return totalMs == 0 ? 0 : rowsExported * 1000.0 / totalMs;
        }
    }

    // ── Main ──────────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(72));
        System.out.println("  CSV Export Benchmark — cursor pagination (id > N LIMIT 5000)");
        System.out.println("=".repeat(72));

        ExportResult mysqlResult = null;
        ExportResult chResult    = null;

        // ── MySQL export ──────────────────────────────────────────────────────
        System.out.println("\n[1/2] MySQL export → " + MYSQL_OUTPUT);
        System.out.printf("  SQL template: %s%n%n", String.format(MYSQL_SQL_TPL, 0));
        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASS)) {
            mysqlResult = runExport(conn, MYSQL_SQL_TPL, MYSQL_OUTPUT, "MySQL");
        } catch (Exception e) {
            System.err.println("  MySQL export failed: " + e.getMessage());
            e.printStackTrace();
        }

        // ── ClickHouse export ─────────────────────────────────────────────────
        System.out.println("\n[2/2] ClickHouse export → " + CH_OUTPUT);
        System.out.printf("  SQL template: %s%n%n", String.format(CH_SQL_TPL, 0));
        try (Connection conn = DriverManager.getConnection(CH_URL, CH_USER, CH_PASS)) {
            chResult = runExport(conn, CH_SQL_TPL, CH_OUTPUT, "ClickHouse");
        } catch (Exception e) {
            System.err.println("  ClickHouse export failed: " + e.getMessage());
            e.printStackTrace();
        }

        // ── Summary ───────────────────────────────────────────────────────────
        printSummary(mysqlResult, chResult);
    }

    // ── Core export logic ─────────────────────────────────────────────────────
    private static ExportResult runExport(Connection conn, String sqlTemplate,
                                           String outFile, String dbLabel)
            throws Exception {

        long exportStart = System.currentTimeMillis();
        long totalRows   = 0;
        int  batchNum    = 0;
        long cursor      = 0;   // last id seen; next batch fetches id > cursor

        try (PrintWriter writer = new PrintWriter(
                new BufferedWriter(new FileWriter(outFile), 1 << 20))) {

            writer.println(CSV_HEADER);

            while (true) {
                String sql = String.format(sqlTemplate, cursor);
                long batchStart = System.currentTimeMillis();
                long rowsInBatch = 0;
                long lastId = cursor;

                try (Statement stmt = conn.createStatement();
                     ResultSet  rs   = stmt.executeQuery(sql)) {

                    int colCount = rs.getMetaData().getColumnCount();
                    StringBuilder batchBuffer = new StringBuilder(BATCH_SIZE * 120);

                    while (rs.next()) {
                        rowsInBatch++;
                        lastId = rs.getLong("id");

                        for (int c = 1; c <= colCount; c++) {
                            batchBuffer.append(escapeCsv(rs.getString(c)));
                            if (c < colCount) batchBuffer.append(',');
                        }
                        batchBuffer.append('\n');
                    }

                    // Write batch to file
                    if (batchBuffer.length() > 0) {
                        writer.print(batchBuffer);
                    }
                }

                // No rows returned → all pages exhausted
                if (rowsInBatch == 0) break;

                batchNum++;
                totalRows += rowsInBatch;
                cursor = lastId;

                long batchMs = System.currentTimeMillis() - batchStart;
                long totalMs = System.currentTimeMillis() - exportStart;
                double rps   = totalRows * 1000.0 / totalMs;

                System.out.printf(
                    "  Batch %4d | id > %-10d | rows: %,5d | batch: %5d ms" +
                    " | total: %6d ms | %.0f rows/sec%n",
                    batchNum, (cursor - rowsInBatch), rowsInBatch,
                    batchMs, totalMs, rps);

                // Last partial batch (fewer rows than BATCH_SIZE) → done
                if (rowsInBatch < BATCH_SIZE) break;
            }
        }

        long totalMs      = System.currentTimeMillis() - exportStart;
        long fileSizeBytes = new File(outFile).length();

        System.out.printf("%n  [%s] Done. %,d rows → %s (%.1f MB) in %,d ms%n",
            dbLabel, totalRows, outFile, fileSizeBytes / 1_048_576.0, totalMs);

        return new ExportResult(dbLabel, totalMs, totalRows, batchNum, fileSizeBytes);
    }

    // ── CSV escaping (RFC 4180) ───────────────────────────────────────────────
    private static String escapeCsv(String value) {
        if (value == null) return "";
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    // ── Summary table ─────────────────────────────────────────────────────────
    private static void printSummary(ExportResult mysql, ExportResult ch) {
        System.out.println("\n" + "=".repeat(72));
        System.out.println("  Export Benchmark Summary");
        System.out.println("=".repeat(72));
        System.out.printf("  %-24s  %-16s  %-16s  %s%n",
            "Metric", "MySQL", "ClickHouse", "Winner");
        System.out.println("-".repeat(72));

        printSummaryRow("Total time (ms)",
            mysql != null ? mysql.totalMs           : -1,
            ch    != null ? ch.totalMs              : -1, true);

        printSummaryRow("Rows exported",
            mysql != null ? mysql.rowsExported      : -1,
            ch    != null ? ch.rowsExported         : -1, false);

        printSummaryRow("Batches (pages)",
            mysql != null ? mysql.batchCount        : -1,
            ch    != null ? ch.batchCount           : -1, false);

        printSummaryRow("File size (KB)",
            mysql != null ? mysql.fileSizeBytes / 1024 : -1,
            ch    != null ? ch.fileSizeBytes    / 1024 : -1, false);

        System.out.printf("  %-24s  %-16s  %-16s  %s%n",
            "Throughput (rows/sec)",
            mysql != null ? String.format("%.0f", mysql.rowsPerSec()) : "N/A",
            ch    != null ? String.format("%.0f", ch.rowsPerSec())    : "N/A",
            speedupLabel(
                mysql != null ? (long) mysql.rowsPerSec() : -1,
                ch    != null ? (long) ch.rowsPerSec()    : -1, false));

        System.out.println("=".repeat(72));
        System.out.println();
        System.out.println("Notes:");
        System.out.println("  Both DBs use cursor pagination:  WHERE id > {lastId} ORDER BY id LIMIT 5000");
        System.out.println("  ClickHouse adds:                 FINAL WHERE is_deleted = 0");
        System.out.println("    FINAL forces ReplacingMergeTree dedup before scan.");
        System.out.println("  Each page is an independent query — no open cursor/stream held.");
    }

    private static void printSummaryRow(String label, long mysql, long ch,
                                         boolean lowerIsBetter) {
        String mysqlStr = mysql < 0 ? "N/A" : String.format("%,d", mysql);
        String chStr    = ch    < 0 ? "N/A" : String.format("%,d", ch);
        System.out.printf("  %-24s  %-16s  %-16s  %s%n",
            label, mysqlStr, chStr, speedupLabel(mysql, ch, lowerIsBetter));
    }

    private static String speedupLabel(long mysql, long ch, boolean lowerIsBetter) {
        if (mysql <= 0 || ch <= 0) return "--";
        if (lowerIsBetter) {
            if (mysql >= ch) return String.format("CH %.1fx faster",    (double) mysql / ch);
            else             return String.format("MySQL %.1fx faster", (double) ch / mysql);
        } else {
            if (ch >= mysql) return String.format("CH %.1fx higher",    (double) ch / mysql);
            else             return String.format("MySQL %.1fx higher", (double) mysql / ch);
        }
    }
}
