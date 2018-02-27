import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object EShopExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("EShop")
      .getOrCreate()

    val preparedDF = getPreparedDataFrame(spark)

    topCategoryProducts(2, preparedDF)

    spark.stop
  }

  def getPreparedDataFrame(spark: SparkSession): DataFrame = {
    val orderDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql:eshop")
      .option("dbtable", "cust_order")
      .option("user", "postgres")
      .option("password", "123123")
      .load

    val orderItemDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql:eshop")
      .option("dbtable", "order_item")
      .option("user", "postgres")
      .option("password", "123123")
      .load

    val productDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql:eshop")
      .option("dbtable", "product")
      .option("user", "postgres")
      .option("password", "123123")
      .load

    val orderAggProductsDF = orderItemDF.groupBy("product_id").agg(functions.sum("quantity").alias("full_quantity"))
    val productsCategoriesDF = orderAggProductsDF.join(productDF, Seq("product_id"), "inner")

    val preparedDF = productsCategoriesDF
      .withColumn("sum_price", functions.round(productsCategoriesDF.col("full_quantity") * productsCategoriesDF.col("product_price"), 2))
      .select("category_id", "product_id", "product_name", "sum_price").sort(functions.asc("category_id"), functions.desc("sum_price"))

    preparedDF
  }

  def topCategoryProducts(topNum: Int, df: DataFrame): Unit = {
    val windowSpec = Window
      .partitionBy(df.col("category_id"))
      .orderBy(df.col("sum_price").desc)

    val ranks = functions.dense_rank().over(windowSpec)

    val dfWithRanksDF = df.withColumn("ranks", ranks)

    val topsDF = dfWithRanksDF
      .select("category_id", "product_id", "product_name", "sum_price")
      .where(dfWithRanksDF.col("ranks") <= topNum)
      .sort(functions.asc("category_id"), functions.desc("sum_price"))
    topsDF.show
  }
}
