package question2.b

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExportExtractor {

  def readData(spark:SparkSession): DataFrame = {

    val tradeSchema = StructType(Array(
      StructField("HSCode", IntegerType, true),
      StructField("Commodity", StringType, true),
      StructField("Value", DoubleType, true),
      StructField("Country", StringType, true),
      StructField("Year", IntegerType, true)
    ))

    val df = spark.read.option("header",true)
      .schema(tradeSchema)
      .csv("/Users/shubhamchandel/Downloads/Spark_Assignment/src/main/data/india-trade-data/2018-2010_import.csv")

    df
  }



}
