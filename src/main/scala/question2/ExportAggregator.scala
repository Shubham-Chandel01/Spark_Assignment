package question2
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExportAggregator {

  def aggregateData(df: DataFrame,year:Int): DataFrame = {
    df.filter(col("year")===year).groupBy("Commodity", "country") ///update
      .agg(sum("value").alias("total_value"))
      .withColumn("rank", rank().over(Window.partitionBy("Commodity").orderBy(desc("total_value"))))
  }

  def main(args: Array[String]): Unit = {
    // Check if the year argument is provided
    if (args.length != 1) {
      println("Please provide the year as a command-line argument.")
      System.exit(1)
    }

    val year = args(0)

    // Create Spark session
    val spark = SparkSession.builder()
      .appName("Export Aggregator")
      .getOrCreate()

    // Load your DataFrame
    val df = spark.read.option("header", "true").csv("path/to/your/input.csv")

    // Perform aggregation
    val aggregatedDF = aggregateData(df,year.toInt)

    // Show the aggregated DataFrame
    aggregatedDF.show()

    // Write the aggregated DataFrame to CSV
    aggregatedDF.write
      .option("header", "true")
      .csv("/Users/manishkumartyagi/Downloads/bada project/output/output.csv")

    // Stop the Spark session
    spark.stop()
  }
}