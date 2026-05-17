from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_unixtime, trim

from fraud_transactions.utils.paths import RAW_PATH, CURATED_PATH
from fraud_transactions.utils.spark_session import get_spark, stop_spark


class CuratedJob:

    def clean_and_cast_columns(self, df_raw: DataFrame) -> DataFrame:
        return (
            df_raw
            .dropDuplicates()
            .withColumn(
                "timestamp",
                from_unixtime(col("timestamp").cast("long")).cast("timestamp")
            )
            .withColumn("amount", col("amount").cast("double"))
            .withColumn("risk_score", col("risk_score").cast("double"))
            .withColumn("transaction_type", trim(col("transaction_type")))
            .withColumn("location_region", trim(col("location_region")))
            .withColumn("sending_address", trim(col("sending_address")))
            .withColumn("receiving_address", trim(col("receiving_address")))
            .withColumn("processing_date", col("processing_date").cast("date"))
        )

    def run(self) -> None:
        spark = get_spark("curated-fraud-transactions")

        try:
            df_raw = spark.read.format("delta").load(str(RAW_PATH))

            df_curated = self.clean_and_cast_columns(df_raw)

            (
                df_curated.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("processing_date")
                .save(str(CURATED_PATH))
            )

            print(f"Curated salva em Delta particionada por processing_date: {CURATED_PATH}")

        finally:
            stop_spark(spark)


if __name__ == "__main__":
    job = CuratedJob()
    job.run()