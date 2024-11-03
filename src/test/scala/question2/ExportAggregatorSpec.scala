package question2
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfter

class ExportAggregatorSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {
  private var spark: SparkSession = _

  before {
    spark = SparkSession.builder()
      .appName("Export Aggregator Test")
      .master("local[*]")
      .getOrCreate()
  }

  "aggregateData" should "aggregate export values and rank correctly" in {
    val schema = StructType(Array(
      StructField("year", IntegerType, true),
      StructField("Commodity", StringType, true),
      StructField("country", StringType, true),
      StructField("value", DoubleType, true)
    ))

    val data = Seq(
      Row(2018, "MEAT AND EDIBLE MEAT OFFAL.", "CountryA", 1_500_000.0),
      Row(2018, "MEAT AND EDIBLE MEAT OFFAL.", "CountryB", 300_000.0),
      Row(2018, "MEAT AND EDIBLE MEAT OFFAL.", "CountryC", 50_000.0),
      Row(2018, "MEAT AND EDIBLE MEAT OFFAL.", "CountryD", 10_000.0)
    )

    val inputData: DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    val result = ExportAggregator.aggregateData(inputData, 2018)

    result.show()
    result.count() should be (4)

    val expectedSchema = StructType(Array(
      StructField("Commodity", StringType, true),
      StructField("country", StringType, true),
      StructField("total_value", DoubleType, true),
      StructField("rank", IntegerType, true)
    ))

    val expectedData = Seq(
      Row("MEAT AND EDIBLE MEAT OFFAL.", "CountryA", 1_500_000.0, 1),
      Row("MEAT AND EDIBLE MEAT OFFAL.", "CountryB", 300_000.0, 2),
      Row("MEAT AND EDIBLE MEAT OFFAL.", "CountryC", 50_000.0, 3),
      Row("MEAT AND EDIBLE MEAT OFFAL.", "CountryD", 10_000.0, 4)
    )

    val expectedDF: DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema
    )

    assert(result.collect() sameElements expectedDF.collect())
  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }
}