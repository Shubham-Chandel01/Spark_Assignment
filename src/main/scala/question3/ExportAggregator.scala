package question3
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
object ExportAggregator {
  def categorize(value: Double): String = {
    if (value >= 1000) "big"
    else if (value >= 100) "medium"
    else "small"
  }

  val categorizeUDF = udf(categorize _)

  def aggregateData(inputData: DataFrame, year: Int, commodity: String): DataFrame = {
    inputData
      .filter(col("year") === year && col("commodity") === commodity)
      .groupBy("commodity", "country")
      .agg(sum("value").alias("total_value"))
  }
}