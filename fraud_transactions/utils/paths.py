from pathlib import Path

BASE_PATH = Path(__file__).resolve().parents[1]

SOURCE_FILE = BASE_PATH / "source_data" / "df_fraud_credit.csv"

LANDING_PATH = BASE_PATH / "sink_data" / "landing" / "fraud_transactions"
RAW_PATH = BASE_PATH / "sink_data" / "raw" / "fraud_transactions"
CURATED_PATH = BASE_PATH / "sink_data" / "curated" / "fraud_transactions"

ANALYTICS_PATH = BASE_PATH / "sink_data" / "analytics"
AVG_RISK_SCORE_PATH = ANALYTICS_PATH / "avg_risk_score_by_region"
TOP3_RECEIVING_ADDRESS_PATH = ANALYTICS_PATH / "top3_receiving_address_by_latest_sale_amount"

QUALITY_PATH = BASE_PATH / "sink_data" / "quality"
QUALITY_REPORT_PATH = QUALITY_PATH / "fraud_transactions_quality_report.json"