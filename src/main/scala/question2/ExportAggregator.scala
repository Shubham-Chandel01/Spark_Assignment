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

}