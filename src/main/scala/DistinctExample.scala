import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DistinctExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DistinctExample")
      .master("local[2]")
      .getOrCreate()

    val df = loadDF(spark)
    df.createTempView("testdf")

    df.show()
    println("Original size: " + df.count())

    var startTime = System.nanoTime()
    val distinctDF = getDistinctDF(df)
    println("Distinct size: " + distinctDF.count())
    var endTime = System.nanoTime()
    println("Elapsed time: " + (endTime - startTime) / 1e9d)  // 28.02s - 30.16s - 28.11s = 28.76s - III

    val distinctRDD = getDistinctRDD(df)
    startTime = endTime
    println("Distinct RDD size: " + distinctRDD.count())
    endTime = System.nanoTime()
    println("Elapsed time: " + (endTime - startTime) / 1e9d)  // 39.55s - 39.42s - 38.35s = 39.11s - BOO

    val distinctGroupedDF = getDistinctByGrouping(df)
    startTime = endTime
    println("Distinct Grouped DF size: " + distinctGroupedDF.count())
    endTime = System.nanoTime()
    println("Elapsed time: " + (endTime - startTime) / 1e9d)  // 26.81s - 27.13s - 26.43s = 26.79s - II

    val distinctSqlDF = getDistinctSQL(spark)
    startTime = endTime
    println("Distinct Grouped DF size: " + distinctSqlDF.count())
    endTime = System.nanoTime()
    println("Elapsed time: " + (endTime - startTime) / 1e9d)  // 26.32s - 26.58s - 26.34s = 26.41s - I (WINNER)
  }

  def loadDF(spark: SparkSession): DataFrame = {
    spark
      .read
      .format("csv")
      .option("header", "true")
      .load("../convertcsv.csv")
  }

  def getDistinctDF(df: DataFrame): DataFrame = df.dropDuplicates()

  def getDistinctRDD(df: DataFrame): RDD[Row] = df.rdd.distinct

  def getDistinctByGrouping(df: DataFrame): DataFrame = {
    df.groupBy("id", "gender", "age", "good", "educated").count()
  }

  def getDistinctSQL(spark: SparkSession): DataFrame = spark.sql("SELECT DISTINCT * FROM testdf")
}
