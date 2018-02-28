import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object GSMCalls {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("GSMCalls")
      .getOrCreate()

    val callsDF = getCallsDF(spark)

    topOutgoingParties(5, callsDF)

  }

  def getCallsDF(spark: SparkSession): DataFrame = {
    val callsDF = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("data/calls_log.csv")

    callsDF
  }

  def topOutgoingParties(topNum: Int, df: DataFrame): Unit = {
    val groupedCalls = df.
      groupBy(df.col("region_id"), df.col("out_number"))
      .agg(sum(df.col("call_duration")).alias("calls_duration"))
//      .sort(asc("region_id"), desc("calls_duration"))

    val windowSpec = Window
      .partitionBy(groupedCalls.col("region_id"))
      .orderBy(groupedCalls.col("calls_duration").desc)

    val ranks = dense_rank().over(windowSpec)

    val callsWithRanks = groupedCalls.withColumn("rank", ranks)

    val topsDF = callsWithRanks
      .select("region_id", "out_number", "calls_duration")
      .where(callsWithRanks.col("rank") <= topNum)
      .sort(asc("region_id"), desc("calls_duration"))

    topsDF.show()
  }
}
