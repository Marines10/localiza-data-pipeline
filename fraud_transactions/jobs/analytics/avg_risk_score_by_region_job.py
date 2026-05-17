from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, round, trim

from fraud_transactions.utils.paths import CURATED_PATH, AVG_RISK_SCORE_PATH
from fraud_transactions.utils.spark_session import get_spark, stop_spark


class AvgRiskScoreByRegionJob:

    def generate_avg_risk_score_by_region(self, df_curated: DataFrame) -> DataFrame:
        return (
            df_curated
            .filter(col("location_region").isNotNull())
            .filter(trim(col("location_region")) != "")
            .filter(col("location_region") != "0")
            .filter(col("risk_score").isNotNull())
            .groupBy("location_region")
            .agg(round(avg("risk_score"), 4).alias("avg_risk_score"))
            .orderBy(col("avg_risk_score").desc())
        )

    def run(self) -> None:
        spark = get_spark("avg-risk-score-by-region")

        try:
            df_curated = spark.read.format("delta").load(str(CURATED_PATH))

            df_result = self.generate_avg_risk_score_by_region(df_curated)

            (
                df_result.write
                .format("delta")
                .mode("overwrite")
                .save(str(AVG_RISK_SCORE_PATH))
            )
            print("Data Analitics")
            df_result.show(truncate=False)
            print(f"Tabela analítica salva em Delta: {AVG_RISK_SCORE_PATH}")

        finally:
            stop_spark(spark)


if __name__ == "__main__":
    job = AvgRiskScoreByRegionJob()
    job.run()