package question2
import org.apache.spark.sql.SparkSession

object ExportAggregatorDriver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ExportAggregator")
      .master("local[*]")
      .getOrCreate()

    if (args.length < 1) {
      println("Please provide the year as a command-line argument.")
      sys.exit(1)
    }

    val year = args(0)
    val filePath = "/Users/shubhamchandel/Downloads/Spark_Assignment/src/main/data/india-trade-data/2018-2010_export.csv"

    val df = ExportExtractor.readData(spark, filePath)
    val aggregatedDF = ExportAggregator.aggregateData(df,year.toInt)
    ExportWriter.writeData(aggregatedDF, year)

    spark.stop()
  }
}