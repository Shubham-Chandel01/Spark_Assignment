package question2
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object ExportWriter {
  def writeData(df: DataFrame, year: String): Unit = {
    val commodities = df.select("Commodity").distinct().collect().map(_.getString(0))

    commodities.foreach { commodity =>
      df.filter(col("Commodity") === commodity)
        .coalesce(1)
        .write
        .option("header", "true").mode("overwrite")
        .csv(s"/Users/shubhamchandel/Downloads/Spark_Assignment/src/main/scala/question2/output/$year/$commodity/exportRanking.csv")
    }
  }
}