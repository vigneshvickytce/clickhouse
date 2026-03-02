from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER

OUTPUT = "MySQL_vs_ClickHouse_Benchmark_Documentation.pdf"

doc = SimpleDocTemplate(
    OUTPUT,
    pagesize=A4,
    leftMargin=2*cm, rightMargin=2*cm,
    topMargin=2*cm, bottomMargin=2*cm,
)

W = A4[0] - 4*cm  # usable width

styles = getSampleStyleSheet()

# ── Custom styles ──────────────────────────────────────────────────────────
H1 = ParagraphStyle("H1", parent=styles["Heading1"], fontSize=18, spaceAfter=6,
                    textColor=colors.HexColor("#1a1a2e"))
H2 = ParagraphStyle("H2", parent=styles["Heading2"], fontSize=13, spaceBefore=14,
                    spaceAfter=4, textColor=colors.HexColor("#16213e"))
H3 = ParagraphStyle("H3", parent=styles["Heading3"], fontSize=11, spaceBefore=8,
                    spaceAfter=3, textColor=colors.HexColor("#0f3460"))
BODY = ParagraphStyle("BODY", parent=styles["Normal"], fontSize=9.5, leading=14,
                      spaceAfter=4)
NOTE = ParagraphStyle("NOTE", parent=styles["Normal"], fontSize=8.5, leading=13,
                      textColor=colors.HexColor("#444444"), leftIndent=12)
CODE = ParagraphStyle("CODE", parent=styles["Normal"], fontSize=8,
                      fontName="Courier", leading=12, leftIndent=12,
                      backColor=colors.HexColor("#f5f5f5"), spaceAfter=4)
CAPTION = ParagraphStyle("CAPTION", parent=styles["Normal"], fontSize=8,
                          textColor=colors.grey, alignment=TA_CENTER, spaceAfter=6)

# ── Table helpers ──────────────────────────────────────────────────────────
HDR_BG   = colors.HexColor("#16213e")
ALT_BG   = colors.HexColor("#eef2f7")
WIN_CH   = colors.HexColor("#d4edda")
WIN_MY   = colors.HexColor("#d1ecf1")
WIN_TIE  = colors.HexColor("#fff3cd")

def base_style():
    return [
        ("BACKGROUND",  (0, 0), (-1, 0), HDR_BG),
        ("TEXTCOLOR",   (0, 0), (-1, 0), colors.white),
        ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, 0), 8),
        ("FONTSIZE",    (0, 1), (-1, -1), 8),
        ("FONTNAME",    (0, 1), (-1, -1), "Helvetica"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, ALT_BG]),
        ("GRID",        (0, 0), (-1, -1), 0.3, colors.HexColor("#cccccc")),
        ("VALIGN",      (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",  (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING",(0,0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING",(0, 0), (-1, -1), 5),
    ]

def make_table(data, col_widths, winner_col=None, winner_map=None):
    """winner_map: dict of row_index -> bg color for winner_col cell."""
    ts = TableStyle(base_style())
    if winner_col is not None and winner_map:
        for row_i, bg in winner_map.items():
            ts.add("BACKGROUND", (winner_col, row_i), (winner_col, row_i), bg)
    t = Table(data, colWidths=col_widths)
    t.setStyle(ts)
    return t

# ── Story ──────────────────────────────────────────────────────────────────
story = []

# Title
story.append(Spacer(1, 0.3*cm))
story.append(Paragraph("MySQL vs ClickHouse Benchmark", H1))
story.append(Paragraph("Project Documentation", ParagraphStyle(
    "sub", parent=styles["Normal"], fontSize=11,
    textColor=colors.HexColor("#555555"), spaceAfter=4)))
story.append(HRFlowable(width="100%", thickness=1.5, color=colors.HexColor("#16213e"), spaceAfter=10))

# ── 1. Overview ────────────────────────────────────────────────────────────
story.append(Paragraph("1. Overview", H2))
story.append(Paragraph(
    "A Java 11 / Maven project that seeds <b>1,000,000 rows</b> of synthetic Active Directory "
    "user data into both MySQL and ClickHouse, then runs side-by-side performance comparisons "
    "across multiple workload types. Results are written to <i>benchmark_results.txt</i> and "
    "rendered as <i>benchmark_report.html</i>.", BODY))

# ── 2. Database Config ─────────────────────────────────────────────────────
story.append(Paragraph("2. Database Configuration", H2))

cfg_data = [
    ["", "MySQL", "ClickHouse"],
    ["Version", "8.0.33", "26.1.3.52"],
    ["Host", "localhost:3306", "localhost:8123"],
    ["User", "root (no password)", "default (no password)"],
    ["Database", "reports_db", "reports_db"],
    ["Driver", "mysql-connector-java 8.0.33", "clickhouse-jdbc 0.4.6"],
]
story.append(make_table(cfg_data, [3*cm, 7*cm, 7*cm]))
story.append(Spacer(1, 0.3*cm))

story.append(Paragraph("How MySQL is used", H3))
story.append(Paragraph(
    "Standard InnoDB tables with <b>BIGINT AUTO_INCREMENT</b> primary keys and UNIQUE constraints "
    "on <i>unique_id</i>. DML uses conventional UPDATE and DELETE statements. Cursor pagination "
    "uses <b>WHERE id &gt; {lastId} ORDER BY id LIMIT N</b>.", BODY))

story.append(Paragraph("How ClickHouse is used", H3))
story.append(Paragraph(
    "<b>ADSMUserGeneralDetails</b> uses <b>ReplacingMergeTree(updated_at) ORDER BY (unique_id)</b> — "
    "an append-only upsert pattern where updates are new INSERTs with a newer <i>updated_at</i>, "
    "and deletes set <i>is_deleted = 1</i>. Reads use <b>FINAL WHERE is_deleted = 0</b> to get "
    "the deduplicated view.", BODY))
story.append(Paragraph(
    "<b>ADSMUserAccountDetails</b> and <b>ADSMUserExchangeDetails</b> use plain "
    "<b>MergeTree ORDER BY (unique_id)</b>, with updates done via INSERT (overwrite). "
    "Cursor pagination for export uses <b>WHERE unique_id &gt; '{lastUniqueId}'</b> to leverage "
    "the sparse primary index.", BODY))

# ── 3. Data Schema ─────────────────────────────────────────────────────────
story.append(Paragraph("3. Data Schema", H2))
story.append(Paragraph(
    "Three tables mirror Active Directory attributes, joined by <b>unique_id</b> "
    "(<i>uid-0000000001 … uid-1000000000</i>):", BODY))

schema_data = [
    ["Table", "Columns", "ClickHouse Engine"],
    ["ADSMUserGeneralDetails\n(primary)",
     "id, unique_id, object_guid, sam_account_name, name,\nfirstname, lastname, initial, display_name,\ndistinguished_name, department, title\n+ is_deleted, updated_at (CH only)",
     "ReplacingMergeTree(updated_at)\nORDER BY (unique_id)"],
    ["ADSMUserAccountDetails",
     "unique_id, logon_name, logon_to_machine,\nlast_logon_time, account_expires,\npwd_last_set, account_enabled",
     "MergeTree\nORDER BY (unique_id)"],
    ["ADSMUserExchangeDetails",
     "unique_id, mailbox_name, mailbox_database_name,\nemail_address, quota_mb, mailbox_properties",
     "MergeTree\nORDER BY (unique_id)"],
]
story.append(make_table(schema_data, [4.5*cm, 8.5*cm, 4.5*cm]))
story.append(Spacer(1, 0.2*cm))

story.append(Paragraph("Seeding", H3))
story.append(Paragraph(
    "1,000,000 rows inserted into each table in both databases. Fixed Random seeds "
    "(42 / 43 / 44 per table) ensure both DBs receive identical data.", BODY))
story.append(Paragraph(
    "Run once: <font name='Courier'>mvn exec:java -Dexec.mainClass=DataSeeder</font>", NOTE))

# ── 4. Performance Metrics ─────────────────────────────────────────────────
story.append(Paragraph("4. Performance Metrics Compared", H2))
story.append(Paragraph(
    "All timings are in <b>milliseconds (ms)</b> unless noted. Each benchmark runs "
    "1 warmup + 3 timed rounds; the average is stored.", BODY))

# 4.1 Aggregation
story.append(Paragraph("4.1  Aggregation (full-table scans)", H3))
story.append(Paragraph(
    "ClickHouse dominates due to columnar storage — reads only the columns needed.", BODY))
agg_data = [
    ["Query", "MySQL (ms)", "ClickHouse (ms)", "Winner"],
    ["COUNT(*) all rows",               "95",    "2",   "CH 48×"],
    ["GROUP BY department",             "8,315", "29",  "CH 287×"],
    ["WHERE department = 'IT'",         "12",    "13",  "~Tie"],
    ["Count enabled accounts",          "100",   "3",   "CH 33×"],
    ["GROUP BY mailbox DB + AVG(quota)","3,359", "10",  "CH 336×"],
]
wm = {1: WIN_CH, 2: WIN_CH, 3: WIN_TIE, 4: WIN_CH, 5: WIN_CH}
story.append(make_table(agg_data, [7.5*cm, 2.5*cm, 3*cm, 4.5*cm], 3, wm))

story.append(Spacer(1, 0.4*cm))

# 4.2 IN Filter
story.append(Paragraph("4.2  IN-filter lookups (10,000 random IDs across 1M rows)", H3))
story.append(Paragraph(
    "Point-lookup style — MySQL's B-Tree index wins. "
    "ClickHouse must scatter-read across many data parts.", BODY))
in_data = [
    ["Query", "MySQL (ms)", "ClickHouse (ms)", "Winner"],
    ["COUNT(*) for 10k IDs",          "50",  "92",  "MySQL 1.8×"],
    ["Fetch all 12 cols for 10k",     "57",  "184", "MySQL 3.2×"],
    ["GROUP BY dept for 10k",         "55",  "85",  "MySQL 1.5×"],
    ["3-table JOIN fetch for 10k",    "160", "343", "MySQL 2.1×"],
]
wm2 = {1: WIN_MY, 2: WIN_MY, 3: WIN_MY, 4: WIN_MY}
story.append(make_table(in_data, [7.5*cm, 2.5*cm, 3*cm, 4.5*cm], 3, wm2))

story.append(Spacer(1, 0.4*cm))

# 4.3 Sorting & LIKE
story.append(Paragraph("4.3  Sorting & LIKE search", H3))
story.append(Paragraph(
    "ClickHouse faster on large sorts; MySQL faster on full-table LIKE due to columnar "
    "scan overhead for string predicates.", BODY))
sort_data = [
    ["Query", "MySQL (ms)", "ClickHouse (ms)", "Winner"],
    ["Sort 50k by department",       "588",   "290",   "CH 2×"],
    ["Sort 50k by sam_account_name", "1,194", "282",   "CH 4.2×"],
    ["LIKE name '%John%' (full tbl)","7",     "20",    "MySQL 2.9×"],
    ["LIKE department '%IT%'",       "4",     "21",    "MySQL 5.3×"],
    ["JOIN sort 50k by department",  "997",   "1,057", "~Tie"],
    ["JOIN LIKE email '%example%'",  "5",     "186",   "MySQL 37×"],
]
wm3 = {1: WIN_CH, 2: WIN_CH, 3: WIN_MY, 4: WIN_MY, 5: WIN_TIE, 6: WIN_MY}
story.append(make_table(sort_data, [7.5*cm, 2.5*cm, 3*cm, 4.5*cm], 3, wm3))

story.append(Spacer(1, 0.4*cm))

# 4.4 Cursor navigation
story.append(Paragraph("4.4  Cursor navigation (keyset pagination, 5 pages × 100 rows)", H3))
story.append(Paragraph(
    "Timings in <b>microseconds (µs)</b>. MySQL's indexed integer <i>id</i> cursor is far faster "
    "for simple navigation. ClickHouse JOIN cursor is extremely slow because the primary index "
    "is on <i>unique_id</i>, not <i>id</i>.", BODY))
cur_data = [
    ["Cursor type", "MySQL avg (µs)", "ClickHouse avg (µs)", "Winner"],
    ["Simple: WHERE id > N",          "551",     "11,177",    "MySQL ~20×"],
    ["JOIN: WHERE id > N",            "765",     "509,485",   "MySQL ~665×"],
    ["IN-filter cursor (temp table)", "174",     "107,425",   "MySQL ~617×"],
]
wm4 = {1: WIN_MY, 2: WIN_MY, 3: WIN_MY}
story.append(make_table(cur_data, [6*cm, 3*cm, 3.5*cm, 5*cm], 3, wm4))

story.append(Spacer(1, 0.4*cm))

# 4.5 DML
story.append(Paragraph("4.5  DML operations", H3))
story.append(Paragraph(
    "ClickHouse is slower for single-row writes (append-log overhead + no true UPDATE/DELETE). "
    "Batch operations favour ClickHouse due to columnar bulk ingestion.", BODY))
dml_data = [
    ["Operation", "MySQL (ms)", "ClickHouse (ms)", "Winner"],
    ["100 single inserts",                   "12",  "527", "MySQL 44×"],
    ["Batch insert 1,000 rows",              "202", "26",  "CH 7.8×"],
    ["Update 50 rows",                       "16",  "246", "MySQL 15×"],
    ["Delete rows",                          "3",   "233", "MySQL 78×"],
    ["AD sync batch (1,000 users, 3 tables)","401", "39",  "CH 10.3×"],
]
wm5 = {1: WIN_MY, 2: WIN_CH, 3: WIN_MY, 4: WIN_MY, 5: WIN_CH}
story.append(make_table(dml_data, [7.5*cm, 2.5*cm, 3*cm, 4.5*cm], 3, wm5))

story.append(Spacer(1, 0.4*cm))

# 4.6 CSV Export
story.append(Paragraph("4.6  CSV Export (ExportBenchmark)", H3))
story.append(Paragraph(
    "Paginated export of all 1M rows in 5,000-row batches. MySQL wins because ClickHouse's "
    "<b>FINAL</b> keyword forces a deduplication pass per query, adding overhead absent in MySQL.", BODY))
exp_data = [
    ["Metric", "MySQL", "ClickHouse"],
    ["Pagination cursor", "WHERE id > {N}", "WHERE unique_id > '{N}' FINAL"],
    ["Batch size", "5,000 rows", "5,000 rows"],
    ["Total batches", "200", "200"],
    ["FINAL / dedup overhead", "None", "Per-query dedup of ReplacingMergeTree"],
    ["Export winner", "✓ Faster overall", "Slower due to FINAL overhead"],
]
ts_exp = TableStyle(base_style())
ts_exp.add("BACKGROUND", (1, 5), (1, 5), WIN_MY)
t_exp = Table(exp_data, colWidths=[5*cm, 5.5*cm, 7*cm])
t_exp.setStyle(ts_exp)
story.append(t_exp)

# ── 5. Key Takeaway ────────────────────────────────────────────────────────
story.append(Spacer(1, 0.5*cm))
story.append(HRFlowable(width="100%", thickness=0.8, color=colors.HexColor("#16213e"), spaceAfter=8))
story.append(Paragraph("5. Key Takeaway", H2))

takeaway_data = [
    ["Database", "Excels at", "Struggles with"],
    ["ClickHouse",
     "Full-table aggregations, large sorts,\ncolumnar scans, batch writes,\nbulk AD sync",
     "Point lookups (IN filter), row-level DML\n(update/delete), cursor pagination,\nsingle-row inserts"],
    ["MySQL",
     "Point lookups by ID, row-level DML,\ncursor-based pagination,\nfull-table LIKE search",
     "Full-table aggregations, large-scale\nGROUP BY, analytics scans"],
]
ts_tk = TableStyle(base_style())
ts_tk.add("BACKGROUND", (0, 1), (0, 1), colors.HexColor("#e8f4f8"))
ts_tk.add("BACKGROUND", (0, 2), (0, 2), colors.HexColor("#fef9e7"))
t_tk = Table(takeaway_data, colWidths=[3*cm, 7.5*cm, 7*cm])
t_tk.setStyle(ts_tk)
story.append(t_tk)

story.append(Spacer(1, 0.6*cm))
story.append(Paragraph(
    "Use ClickHouse for analytics/reporting workloads. Use MySQL for OLTP, "
    "record navigation, and row-level mutations.", NOTE))

# ── Build ──────────────────────────────────────────────────────────────────
doc.build(story)
print(f"PDF written: {OUTPUT}")
