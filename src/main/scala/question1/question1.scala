package question1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number, sum}
import org.apache.spark.sql.types._

object question1 extends  App {

  println("Enter the year to analyze:")
  val inputYear = scala.io.StdIn.readLine().toInt
  println("Enter the country to analyze:")
  val inputCountry = scala.io.StdIn.readLine()
  val spark = SparkSession
    .builder()
    .appName("Question_1")
    .master("local[*]")
    .getOrCreate()

  val tradeSchema = StructType(Array(
    StructField("HSCode", IntegerType, true),
    StructField("Commodity", StringType, true),
    StructField("Value", DoubleType, true),
    StructField("Country", StringType, true),
    StructField("Year", IntegerType, true)
  ))

  val export_df = spark.read.option("header", "true")
    .schema(tradeSchema).csv("/Users/shubhamchandel/Downloads/Spark_Assignment/src/main/data/india-trade-data/2018-2010_export.csv")

  val filtered_df = export_df
    .filter(col("Country") === inputCountry && col("Year") === inputYear)

  val most_export_df = filtered_df.groupBy("Commodity")
    .agg(sum("Value").as("TotalValue"))
    .withColumn("Rank", row_number().over(Window.orderBy(desc("TotalValue"))))
    .filter(col("Rank") === 1)
    .select("Commodity", "TotalValue")

  most_export_df.show()

}
