package question2.b

import org.apache.spark.sql.SparkSession

class ExportAggregatorDriver {
  val spark = SparkSession.builder()
    .appName("ExportAggregator")
    .master("local[*]")
    .getOrCreate()

  val read_df = ExportExtractor.readData(spark)
  
  spark.stop()
}
