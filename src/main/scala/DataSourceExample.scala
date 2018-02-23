import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions

object DataSourceExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Source Example")
      .getOrCreate()

    val postgresDF: DataFrame = postgresExample(spark)
    val csvDF: DataFrame = csvExample(spark)

    val country = postgresDF
      .withColumnRenamed("code", "countryCode")
      .as("country")

    val groupedAthletes = csvDF
      .groupBy("nationality")
      .agg(
        functions.count("id").as("athletesNum"),
        functions.sum("gold"),
        functions.sum("silver"),
        functions.sum("bronze")
      )

    val athlete = groupedAthletes
      .withColumnRenamed("count", "athletesNum")
      .withColumnRenamed("nationality", "countryCode")
      .as("athlete")

    athlete.show()

    val countryAthletes = country.join(athlete, Seq("countryCode"), "leftouter")
    countryAthletes.filter(countryAthletes.col("countryCode") === "BLR").show()

    spark.stop
  }

  def postgresExample(spark: SparkSession): DataFrame = {
    val postgresDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql:world")
      .option("dbtable", "country")
      .option("user", "postgres")
      .option("password", "123123")
      .load

    postgresDF.show()

    postgresDF
  }

  def csvExample(spark: SparkSession) : DataFrame = {
    val csvDF = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("data/athletes.csv")

    csvDF.show()

    csvDF
  }
}
