"""
Generate Project Documentation PDF
Swayam Siddhi Das | 23051634 | B.Tech CSE | SAP ABAP Data Engineer
Format: A4, Justified, Arial font, Page numbers bottom-right
"""

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, PageBreak
)
from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.pdfgen import canvas as pdfcanvas
import os

OUT = os.path.join(os.path.dirname(__file__), "..", "docs", "Project_Documentation.pdf")
os.makedirs(os.path.dirname(OUT), exist_ok=True)

W, H = A4
ACCENT   = colors.HexColor("#6C63FF")
ACCENT2  = colors.HexColor("#00D4AA")
DARK     = colors.HexColor("#1A1D27")
GRAY     = colors.HexColor("#555555")
LIGHT_BG = colors.HexColor("#F4F3FF")


# ── Page numbering ────────────────────────────────────────────────────────────
class NumberedCanvas(pdfcanvas.Canvas):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved = []

    def showPage(self):
        self._saved.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        total = len(self._saved)
        for state in self._saved:
            self.__dict__.update(state)
            self._draw_page_number(total)
            super().showPage()
        super().save()

    def _draw_page_number(self, total):
        self.setFont("Helvetica", 9)
        self.setFillColor(GRAY)
        self.drawRightString(W - 2*cm, 1.2*cm, f"Page {self._pageNumber} of {total}")
        self.setStrokeColor(ACCENT)
        self.setLineWidth(0.5)
        self.line(2*cm, 1.5*cm, W - 2*cm, 1.5*cm)


def build_styles():
    base = getSampleStyleSheet()
    styles = {}

    styles["title"] = ParagraphStyle("title",
        fontName="Helvetica-Bold", fontSize=22, leading=28,
        textColor=DARK, alignment=TA_CENTER, spaceAfter=6)

    styles["subtitle"] = ParagraphStyle("subtitle",
        fontName="Helvetica", fontSize=12, leading=16,
        textColor=GRAY, alignment=TA_CENTER, spaceAfter=4)

    styles["h1"] = ParagraphStyle("h1",
        fontName="Helvetica-Bold", fontSize=15, leading=20,
        textColor=ACCENT, spaceBefore=18, spaceAfter=8)

    styles["h2"] = ParagraphStyle("h2",
        fontName="Helvetica-Bold", fontSize=14, leading=18,
        textColor=DARK, spaceBefore=14, spaceAfter=6)

    styles["body"] = ParagraphStyle("body",
        fontName="Helvetica", fontSize=12, leading=18,
        textColor=DARK, alignment=TA_JUSTIFY, spaceAfter=8)

    styles["bullet"] = ParagraphStyle("bullet",
        fontName="Helvetica", fontSize=12, leading=17,
        textColor=DARK, leftIndent=18, spaceAfter=4,
        bulletFontName="Helvetica", bulletFontSize=12)

    styles["caption"] = ParagraphStyle("caption",
        fontName="Helvetica-Oblique", fontSize=10, leading=14,
        textColor=GRAY, alignment=TA_CENTER, spaceAfter=6)

    styles["code"] = ParagraphStyle("code",
        fontName="Courier", fontSize=10, leading=14,
        textColor=colors.HexColor("#2d2060"),
        backColor=LIGHT_BG, leftIndent=12, rightIndent=12,
        spaceAfter=8, spaceBefore=4)

    return styles


def info_table(data, styles):
    rows = [[Paragraph(f"<b>{k}</b>", styles["body"]),
             Paragraph(v, styles["body"])] for k, v in data.items()]
    t = Table(rows, colWidths=[5.5*cm, 11*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (0,-1), LIGHT_BG),
        ("TEXTCOLOR",  (0,0), (0,-1), ACCENT),
        ("ROWBACKGROUNDS", (0,0), (-1,-1), [colors.white, colors.HexColor("#FAFAFA")]),
        ("BOX",   (0,0), (-1,-1), 0.5, colors.HexColor("#DDDDDD")),
        ("GRID",  (0,0), (-1,-1), 0.3, colors.HexColor("#EEEEEE")),
        ("VALIGN",(0,0), (-1,-1), "MIDDLE"),
        ("TOPPADDING",   (0,0), (-1,-1), 7),
        ("BOTTOMPADDING",(0,0), (-1,-1), 7),
        ("LEFTPADDING",  (0,0), (-1,-1), 10),
    ]))
    return t


def arch_table(styles):
    headers = ["Step", "Component", "Technology", "Description"]
    rows = [
        ["1", "Data Generation", "Python (csv, random)", "Synthetic e-commerce orders, products, customers"],
        ["2", "Ingestion",       "Python + SQLite",       "CSV → Raw tables with validation & logging"],
        ["3", "Processing",      "Python + SQL",          "Raw → Star Schema (fact + dim tables) + aggregations"],
        ["4", "Data Quality",    "Python",                "10 automated checks: nulls, duplicates, anomalies"],
        ["5", "Orchestration",   "Custom DAG Runner",     "Task dependency, retry logic, scheduling (@daily)"],
        ["6", "Storage",         "SQLite Warehouse",      "Star schema: fact_orders + 3 dim tables + 3 agg tables"],
        ["7", "Dashboard",       "HTML + Chart.js",       "KPI cards, line charts, donut charts, top-N tables"],
    ]
    full = [[Paragraph(f"<b>{h}</b>", ParagraphStyle("th",fontName="Helvetica-Bold",
              fontSize=11,textColor=colors.white))] for h in headers]
    full = [
        [Paragraph(f"<b>{h}</b>", ParagraphStyle("th",fontName="Helvetica-Bold",fontSize=11,textColor=colors.white))
         for h in headers]
    ] + [[Paragraph(c, ParagraphStyle("td",fontName="Helvetica",fontSize=10,leading=14))
          for c in row] for row in rows]

    t = Table(full, colWidths=[1.2*cm, 3.8*cm, 4.5*cm, 7.5*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), ACCENT),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, LIGHT_BG]),
        ("BOX",  (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
        ("GRID", (0,0), (-1,-1), 0.3, colors.HexColor("#DDDDDD")),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("TOPPADDING",   (0,0),(-1,-1),7),
        ("BOTTOMPADDING",(0,0),(-1,-1),7),
        ("LEFTPADDING",  (0,0),(-1,-1),8),
    ]))
    return t


def build_pdf():
    doc = SimpleDocTemplate(
        OUT, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2.2*cm, bottomMargin=2.5*cm,
        title="E-Commerce Data Pipeline – Capstone Project",
        author="Swayam Siddhi Das",
    )

    S = build_styles()
    story = []

    # ── Cover ─────────────────────────────────────────────────────────────────
    story.append(Spacer(1, 1.5*cm))
    story.append(Paragraph("E-Commerce Data Engineering Pipeline", S["title"]))
    story.append(Paragraph("Capstone Project Documentation", S["subtitle"]))
    story.append(HRFlowable(width="100%", thickness=2, color=ACCENT, spaceAfter=16))

    story.append(info_table({
        "Name":          "Swayam Siddhi Das",
        "Roll Number":   "23051634",
        "Program":       "B.Tech Computer Science and Engineering",
        "Batch/Track":   "SAP ABAP Data Engineer",
        "Submission":    "April 21, 2026",
        "Project Type":  "Individual Capstone Project",
    }, S))

    story.append(Spacer(1, 0.6*cm))
    story.append(Paragraph(
        "Task 30 — Final Capstone: End-to-End Data Pipeline covering all 30 Data Engineering concepts: "
        "Ingestion → Processing → Star Schema Storage → Data Quality → Orchestration → Dashboard.",
        S["body"]))

    story.append(PageBreak())

    # ── 1. Problem Statement ──────────────────────────────────────────────────
    story.append(Paragraph("1. Problem Statement", S["h1"]))
    story.append(HRFlowable(width="100%", thickness=1, color=ACCENT2, spaceAfter=10))
    story.append(Paragraph(
        "Modern businesses generate massive volumes of transactional data every second. "
        "Without a structured pipeline to ingest, clean, transform, and visualize this data, "
        "organizations cannot derive actionable insights. The challenge is to design and implement "
        "a fully automated, end-to-end data engineering pipeline that handles raw e-commerce data "
        "and delivers clean, queryable, analytics-ready datasets with real-time quality monitoring "
        "and a visual dashboard for business stakeholders.", S["body"]))

    story.append(Paragraph("Key Challenges Addressed:", S["h2"]))
    for b in [
        "Raw CSV data with potential nulls, duplicates, and anomalies",
        "No structured schema — data needs modelling into a star schema",
        "Manual processes lack retry logic, scheduling, and monitoring",
        "Business teams need self-serve dashboards without SQL knowledge",
        "Data quality must be enforced automatically before any reporting",
    ]:
        story.append(Paragraph(f"• {b}", S["bullet"]))

    # ── 2. Solution & Features ────────────────────────────────────────────────
    story.append(Paragraph("2. Solution & Features", S["h1"]))
    story.append(HRFlowable(width="100%", thickness=1, color=ACCENT2, spaceAfter=10))
    story.append(Paragraph(
        "This project implements a complete, production-style data engineering pipeline for an "
        "e-commerce domain using Python, SQLite, and web technologies. The pipeline follows the "
        "modern data engineering lifecycle: raw ingestion, transformation, quality validation, "
        "orchestration, and dashboard delivery.", S["body"]))

    story.append(Paragraph("Pipeline Architecture:", S["h2"]))
    story.append(arch_table(S))
    story.append(Spacer(1, 0.4*cm))

    story.append(Paragraph("Key Features:", S["h2"]))
    for b in [
        "Modular Python codebase — each stage is independently runnable",
        "Star Schema data model: fact_orders + dim_customer + dim_product + dim_date",
        "3 pre-built aggregation tables for dashboard performance",
        "10 automated data quality checks with severity levels (CRITICAL / HIGH / MEDIUM / LOW)",
        "Airflow-style DAG with topological sort, retry logic, and skip-on-failure propagation",
        "Structured logging to separate log files per stage",
        "Standalone HTML dashboard with 6 interactive charts — no server required",
        "JSON quality report exported automatically on every pipeline run",
        "Designed for easy GitHub hosting and local execution with zero external dependencies",
    ]:
        story.append(Paragraph(f"• {b}", S["bullet"]))

    story.append(PageBreak())

    # ── 3. Tech Stack ─────────────────────────────────────────────────────────
    story.append(Paragraph("3. Tech Stack", S["h1"]))
    story.append(HRFlowable(width="100%", thickness=1, color=ACCENT2, spaceAfter=10))

    ts_data = [
        ["Layer",         "Technology",     "Purpose"],
        ["Data Source",   "Python (csv, random)", "Synthetic dataset generation"],
        ["Ingestion",     "Python + sqlite3",     "CSV reading, validation, raw table load"],
        ["Processing",    "Python + SQL",         "Star schema transformation, aggregations"],
        ["Data Quality",  "Python",               "Automated checks, anomaly detection, JSON report"],
        ["Orchestration", "Python (custom DAG)",  "Task dependency graph, retry, scheduling"],
        ["Storage",       "SQLite",               "Lightweight warehouse (swappable with Postgres/BigQuery)"],
        ["Dashboard",     "HTML5 + Chart.js",     "KPI cards, line, bar, donut charts"],
        ["Logging",       "Python logging",       "Per-stage log files in /logs"],
        ["Documentation", "ReportLab",            "A4 PDF generation"],
    ]
    ts_table = Table(
        [[Paragraph(f"<b>{c}</b>" if i==0 else c,
                    ParagraphStyle("ts",fontName="Helvetica-Bold" if i==0 else "Helvetica",
                                   fontSize=11 if i==0 else 10, textColor=colors.white if i==0 else DARK))
          for c in row] for i, row in enumerate(ts_data)],
        colWidths=[4*cm, 5.5*cm, 8*cm]
    )
    ts_table.setStyle(TableStyle([
        ("BACKGROUND",     (0,0), (-1,0), ACCENT),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, LIGHT_BG]),
        ("BOX",  (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
        ("GRID", (0,0), (-1,-1), 0.3, colors.HexColor("#DDDDDD")),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("TOPPADDING",   (0,0),(-1,-1),7),
        ("BOTTOMPADDING",(0,0),(-1,-1),7),
        ("LEFTPADDING",  (0,0),(-1,-1),8),
    ]))
    story.append(ts_table)

    # ── 4. Unique Points ──────────────────────────────────────────────────────
    story.append(Paragraph("4. Unique Points & Design Decisions", S["h1"]))
    story.append(HRFlowable(width="100%", thickness=1, color=ACCENT2, spaceAfter=10))

    for title, body in [
        ("Custom DAG Orchestrator",
         "Instead of requiring Airflow installation, a custom DAG class implements topological sort, "
         "upstream dependency checking, configurable retries, and per-task timing — making the project "
         "runnable on any machine with Python 3.8+. Airflow-compatible operator code is included as comments."),
        ("Star Schema over Flat Files",
         "Raw data is normalized into a proper star schema with surrogate keys, enabling efficient "
         "analytical queries using dimension lookups rather than repeated full-table scans. This mirrors "
         "real production data warehouse design (Kimball methodology)."),
        ("Severity-Tiered Quality Gates",
         "Data quality checks are tiered as CRITICAL / HIGH / MEDIUM / LOW. Only CRITICAL failures "
         "block pipeline execution, while lower severity issues are logged and reported without stopping "
         "delivery — a pattern used in production data platforms."),
        ("Zero-Dependency Dashboard",
         "The dashboard is a single self-contained HTML file that loads Chart.js from a CDN and embeds "
         "data inline — making it shareable via email, GitHub Pages, or any web host with no backend."),
        ("Idempotent Pipeline",
         "All INSERT statements use INSERT OR IGNORE patterns, making the pipeline safe to re-run "
         "multiple times without creating duplicate records — a critical property for production pipelines."),
    ]:
        story.append(Paragraph(f"<b>{title}</b>", S["h2"]))
        story.append(Paragraph(body, S["body"]))

    story.append(PageBreak())

    # ── 5. Pipeline Results (Screenshots section) ─────────────────────────────
    story.append(Paragraph("5. Pipeline Results & Key Metrics", S["h1"]))
    story.append(HRFlowable(width="100%", thickness=1, color=ACCENT2, spaceAfter=10))
    story.append(Paragraph(
        "The following metrics were produced by a single run of the pipeline on the generated dataset:", S["body"]))

    metrics = [
        ["Metric",                   "Value"],
        ["Raw Orders Ingested",       "500 rows"],
        ["Valid Orders (non-cancelled)", "455 rows in fact_orders"],
        ["Dimension: dim_date",       "287 unique dates"],
        ["Dimension: dim_customer",   "10 customers"],
        ["Dimension: dim_product",    "10 products (4 categories)"],
        ["Total Revenue",             "₹1,34,45,290"],
        ["Avg Order Value",           "₹29,550"],
        ["Top Category by Revenue",   "Electronics — ₹1,17,81,150 (228 orders)"],
        ["Top Customer",              "Meera Joshi — ₹17,42,482 (54 orders)"],
        ["Data Quality Checks",       "9 of 10 PASS | 1 LOW-severity anomaly flagged"],
        ["Pipeline Duration",         "< 1 second (full DAG execution)"],
    ]
    mt = Table(
        [[Paragraph(f"<b>{c}</b>" if i==0 else c,
                    ParagraphStyle("m",fontName="Helvetica-Bold" if i==0 else "Helvetica",
                                   fontSize=11 if i==0 else 11,
                                   textColor=colors.white if i==0 else DARK))
          for c in row] for i, row in enumerate(metrics)],
        colWidths=[8*cm, 9.5*cm]
    )
    mt.setStyle(TableStyle([
        ("BACKGROUND",     (0,0), (-1,0), ACCENT),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, LIGHT_BG]),
        ("BOX",  (0,0),(-1,-1),0.5,colors.HexColor("#BBBBBB")),
        ("GRID", (0,0),(-1,-1),0.3,colors.HexColor("#DDDDDD")),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("TOPPADDING",   (0,0),(-1,-1),7),
        ("BOTTOMPADDING",(0,0),(-1,-1),7),
        ("LEFTPADDING",  (0,0),(-1,-1),8),
    ]))
    story.append(mt)
    story.append(Spacer(1, 0.4*cm))
    story.append(Paragraph(
        "Dashboard: Open dashboard/dashboard.html in any browser to view live KPI cards, "
        "monthly revenue trend, order status distribution, category breakdown, and top-customer table.", S["body"]))

    # ── 6. Future Improvements ────────────────────────────────────────────────
    story.append(Paragraph("6. Future Improvements", S["h1"]))
    story.append(HRFlowable(width="100%", thickness=1, color=ACCENT2, spaceAfter=10))
    for b in [
        "Replace SQLite with PostgreSQL or Google BigQuery for production-scale workloads",
        "Migrate DAG to Apache Airflow or Prefect with a web-based scheduler UI",
        "Add Apache Kafka producer-consumer for real-time streaming ingestion",
        "Implement Apache Spark for distributed processing of 1M+ row datasets",
        "Add Delta Lake / Apache Iceberg for ACID-compliant lakehouse storage",
        "Deploy dashboard to cloud (AWS S3 static website or Azure Blob) with CI/CD via GitHub Actions",
        "Add ML-based anomaly detection using Isolation Forest on transaction data",
        "Containerize the full pipeline with Docker and Docker Compose",
        "Implement data lineage tracking and column-level impact analysis",
        "Add SAP ABAP OData API connector to ingest live ERP data into the pipeline",
    ]:
        story.append(Paragraph(f"• {b}", S["bullet"]))

    # ── 7. File Structure ─────────────────────────────────────────────────────
    story.append(Paragraph("7. Project File Structure", S["h1"]))
    story.append(HRFlowable(width="100%", thickness=1, color=ACCENT2, spaceAfter=10))
    tree = (
        "capstone_project/\n"
        "├── main.py                        # Entry point — runs full pipeline\n"
        "├── data/\n"
        "│   ├── generate_data.py           # Synthetic dataset generator\n"
        "│   ├── orders.csv                 # 500 e-commerce orders\n"
        "│   ├── products.csv               # Product master data\n"
        "│   └── customers.csv              # Customer master data\n"
        "├── ingestion/\n"
        "│   └── ingest.py                  # CSV → SQLite raw tables\n"
        "├── processing/\n"
        "│   ├── transform.py               # Raw → Star Schema + aggregations\n"
        "│   └── data_quality.py            # 10 automated quality checks\n"
        "├── orchestration/\n"
        "│   └── dag_runner.py              # Airflow-style DAG orchestrator\n"
        "├── dashboard/\n"
        "│   ├── export_data.py             # DB → JSON for dashboard\n"
        "│   ├── dashboard_data.json        # Exported pipeline output\n"
        "│   └── dashboard.html             # Interactive analytics dashboard\n"
        "├── storage/\n"
        "│   └── warehouse.db               # SQLite data warehouse\n"
        "├── logs/\n"
        "│   ├── ingestion.log\n"
        "│   ├── processing.log\n"
        "│   ├── data_quality.log\n"
        "│   ├── orchestration.log\n"
        "│   └── quality_report.json\n"
        "├── docs/\n"
        "│   └── Project_Documentation.pdf\n"
        "├── requirements.txt\n"
        "└── README.md"
    )
    story.append(Paragraph(tree.replace("\n","<br/>").replace(" ","&nbsp;"), S["code"]))

    story.append(Spacer(1, 0.5*cm))
    story.append(HRFlowable(width="100%", thickness=1.5, color=ACCENT, spaceAfter=10))
    story.append(Paragraph(
        "This project was independently developed as the Capstone Project for the "
        "SAP ABAP Data Engineering track. All code is original. Dataset is synthetically generated.",
        ParagraphStyle("footer", fontName="Helvetica-Oblique", fontSize=10,
                       textColor=GRAY, alignment=TA_CENTER)))

    doc.build(story, canvasmaker=NumberedCanvas)
    print(f"PDF generated → {OUT}")


if __name__ == "__main__":
    build_pdf()
