package question2
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExportExtractor {
  def readData(spark: SparkSession, filePath: String): DataFrame = {
    spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)
  }
}