from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from fraud_transactions.utils.paths import CURATED_PATH, TOP3_RECEIVING_ADDRESS_PATH
from fraud_transactions.utils.spark_session import get_spark, stop_spark


class Top3ReceivingAddressByLatestSaleAmountJob:

    def build_analytics(self, df_curated: DataFrame) -> DataFrame:
        window_latest_transaction = (
            Window
            .partitionBy("receiving_address")
            .orderBy(col("timestamp").desc())
        )

        latest_sales = (
            df_curated
            .filter(col("transaction_type") == "sale")
            .filter(col("receiving_address").isNotNull())
            .filter(col("receiving_address") != "")
            .filter(col("amount").isNotNull())
            .filter(col("timestamp").isNotNull())
            .withColumn("row_number", row_number().over(window_latest_transaction))
            .filter(col("row_number") == 1)
            .drop("row_number")
        )

        return (
            latest_sales
            .select("receiving_address", "amount", "timestamp")
            .orderBy(col("amount").desc())
            .limit(3)
        )

    def run(self) -> None:
        spark = get_spark("top3-receiving-address-by-latest-sale-amount")

        try:
            df_curated = spark.read.format("delta").load(str(CURATED_PATH))

            df_result = self.build_analytics(df_curated)

            (
                df_result.write
                .format("delta")
                .mode("overwrite")
                .save(str(TOP3_RECEIVING_ADDRESS_PATH))
            )
            print(f"Tabela analítica salva em Delta: {TOP3_RECEIVING_ADDRESS_PATH}")

        finally:
            stop_spark(spark)


if __name__ == "__main__":
    job = Top3ReceivingAddressByLatestSaleAmountJob()
    job.run()