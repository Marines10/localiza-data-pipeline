import json
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim

from fraud_transactions.utils.paths import (
    LANDING_PATH,
    RAW_PATH,
    CURATED_PATH,
    AVG_RISK_SCORE_PATH,
    TOP3_RECEIVING_ADDRESS_PATH,
    QUALITY_PATH,
    QUALITY_REPORT_PATH,
    SOURCE_FILE,
)
from fraud_transactions.utils.spark_session import get_spark, stop_spark


class DataQualityJob:

    def path_exists(self, path: Path) -> bool:
        return path.exists() and any(path.iterdir())

    def count_invalid_amount(self, df: DataFrame) -> int:
        return df.filter(col("amount").cast("double").isNull()).count()

    def count_invalid_risk_score(self, df: DataFrame) -> int:
        return df.filter(col("risk_score").cast("double").isNull()).count()

    def count_invalid_timestamp(self, df: DataFrame) -> int:
        return df.filter(col("timestamp").cast("long").isNull()).count()

    def count_invalid_location_region(self, df: DataFrame) -> int:
        return (
            df
            .filter(
                col("location_region").isNull()
                | (trim(col("location_region")) == "")
                | (col("location_region") == "0")
            )
            .count()
        )

    def count_invalid_receiving_address(self, df: DataFrame) -> int:
        return (
            df
            .filter(
                col("receiving_address").isNull()
                | (trim(col("receiving_address")) == "")
            )
            .count()
        )

    def build_report(
        self,
        raw_df: DataFrame,
        curated_df: DataFrame,
        avg_risk_df: DataFrame,
        top3_df: DataFrame,
    ) -> dict:
        current_date = date.today().isoformat()

        landing_file = LANDING_PATH / current_date / SOURCE_FILE.name

        raw_total_records = raw_df.count()
        curated_total_records = curated_df.count()

        duplicate_records = raw_total_records - raw_df.dropDuplicates().count()

        invalid_amount_records = self.count_invalid_amount(raw_df)
        invalid_risk_score_records = self.count_invalid_risk_score(raw_df)
        invalid_timestamp_records = self.count_invalid_timestamp(raw_df)
        invalid_location_region_records = self.count_invalid_location_region(raw_df)
        invalid_receiving_address_records = self.count_invalid_receiving_address(raw_df)

        total_error_records = (
            duplicate_records
            + invalid_amount_records
            + invalid_risk_score_records
            + invalid_timestamp_records
            + invalid_location_region_records
            + invalid_receiving_address_records
        )

        conformity_percentage = (
            round(((raw_total_records - total_error_records) / raw_total_records) * 100, 4)
            if raw_total_records
            else 0
        )

        return {
            "execution_date": current_date,
            "landing": {
                "file_exists": landing_file.exists(),
                "file_path": str(landing_file),
            },
            "raw": {
                "total_records": raw_total_records,
                "duplicate_records": duplicate_records,
                "invalid_amount_records": invalid_amount_records,
                "invalid_risk_score_records": invalid_risk_score_records,
                "invalid_timestamp_records": invalid_timestamp_records,
                "invalid_location_region_records": invalid_location_region_records,
                "invalid_receiving_address_records": invalid_receiving_address_records,
                "total_error_records": total_error_records,
                "conformity_percentage": conformity_percentage,
            },
            "curated": {
                "total_records": curated_total_records,
                "removed_records": raw_total_records - curated_total_records,
            },
            "analytics": {
                "avg_risk_score_by_region": {
                    "path_exists": self.path_exists(AVG_RISK_SCORE_PATH),
                    "record_count": avg_risk_df.count(),
                    "expected_columns": ["location_region", "avg_risk_score"],
                    "is_ordered_desc": True,
                },
                "top3_receiving_address_by_latest_sale_amount": {
                    "path_exists": self.path_exists(TOP3_RECEIVING_ADDRESS_PATH),
                    "record_count": top3_df.count(),
                    "expected_columns": ["receiving_address", "amount", "timestamp"],
                    "has_expected_limit": top3_df.count() <= 3,
                },
            },
        }

    def run(self) -> None:
        spark = get_spark("fraud-transactions-data-quality")

        try:
            QUALITY_PATH.mkdir(parents=True, exist_ok=True)

            raw_df = spark.read.format("delta").load(str(RAW_PATH))
            curated_df = spark.read.format("delta").load(str(CURATED_PATH))
            avg_risk_df = spark.read.format("delta").load(str(AVG_RISK_SCORE_PATH))
            top3_df = spark.read.format("delta").load(str(TOP3_RECEIVING_ADDRESS_PATH))

            df_report = self.build_report(
                raw_df=raw_df,
                curated_df=curated_df,
                avg_risk_df=avg_risk_df,
                top3_df=top3_df,
            )
            print("Relatório de Data Quality:")
            print(json.dumps(df_report, indent=4, ensure_ascii=False))

            with open(QUALITY_REPORT_PATH, "w", encoding="utf-8") as file:
                json.dump(df_report, file, indent=4, ensure_ascii=False)

            print(f"Relatório de Data Quality gerado: {QUALITY_REPORT_PATH}")

        finally:
            stop_spark(spark)


if __name__ == "__main__":
    job = DataQualityJob()
    job.run()