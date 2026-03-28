from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "pension_pe.db"
DOWNLOAD_DIR = BASE_DIR / "data" / "downloads"

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Source URLs
SOURCES = {
    "calpers": {
        "name": "CalPERS",
        "url": "https://www.calpers.ca.gov/investments/about-investment-office/investment-organization/pep-fund-performance-print",
        "type": "html",
    },
    "calstrs": {
        "name": "CalSTRS",
        "url": "https://www.calstrs.com/files/3c2ee490a/CalSTRSPrivateEquityPerformanceReportFYE2025.pdf",
        "type": "pdf",
    },
    "oregon_operf": {
        "name": "Oregon OPERF",
        "url": "https://www.oregon.gov/treasury/invested-for-oregon/Documents/Invested-for-OR-Performance-and-Holdings/2024/OPERF_Private_Equity_Portfolio_-_Quarter_4_2024.pdf",
        "type": "pdf",
    },
    "florida_sba": {
        "name": "Florida SBA",
        "url": "https://www.sbafla.com/fsb/Portals/FSB/Content/Performance/Quarterly/2024/q4-2024-private-equity-performance-report.pdf",
        "type": "pdf",
    },
    "wsib": {
        "name": "Washington WSIB",
        "url": "https://www.sib.wa.gov/docs/reports/quarterly/ir093024.pdf",
        "type": "pdf",
    },
    "sdcers": {
        "name": "SDCERS",
        "url": "https://content.sdcers.org/wp-content/uploads/2025/03/stepstone-private-equity-full-performance-report-q2-2024.pdf",
        "type": "pdf",
    },
}

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) PensionPEResearch/1.0"
