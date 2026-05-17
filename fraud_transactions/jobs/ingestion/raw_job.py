from datetime import date

from pyspark.sql.functions import lit

from fraud_transactions.utils.paths import LANDING_PATH, RAW_PATH, SOURCE_FILE
from fraud_transactions.utils.spark_session import get_spark, stop_spark


class RawJob:

    def run(self) -> None:
        current_date = date.today().isoformat()

        spark = get_spark("raw-fraud-transactions")

        try:
            landing_file = LANDING_PATH / current_date / SOURCE_FILE.name

            df_landing = (
                spark.read
                .option("header", True)
                .option("inferSchema", False)
                .csv(str(landing_file))
            )

            normalized_columns = [
                column.strip().lower().replace(" ", "_")
                for column in df_landing.columns
            ]

            df_raw = (
                df_landing
                .toDF(*normalized_columns)
                .withColumn("processing_date", lit(current_date))
            )

            (
                df_raw.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("processing_date")
                .save(str(RAW_PATH))
            )

            print(f"Raw salva em Delta particionada por processing_date: {RAW_PATH}")

        finally:
            stop_spark(spark)


if __name__ == "__main__":
    job = RawJob()
    job.run()