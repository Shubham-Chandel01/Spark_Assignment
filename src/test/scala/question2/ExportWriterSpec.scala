package question2
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfter
import java.io.File

class ExportWriterSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {
  private var spark: SparkSession = _

  before {
    spark = SparkSession.builder()
      .appName("Export Writer Test")
      .master("local[*]")
      .getOrCreate()
  }

  "writeData" should "write the data to CSV files" in {
    val schema = StructType(Array(
      StructField("Commodity", StringType, true),
      StructField("country", StringType, true),
      StructField("total_value", DoubleType, true),
      StructField("rank", IntegerType, true)
    ))

    val data = Seq(
      Row("MEAT AND EDIBLE MEAT OFFAL.", "CountryA", 1_500_000.0, 1),
      Row("MEAT AND EDIBLE MEAT OFFAL.", "CountryB", 300_000.0, 2),
      Row("MEAT AND EDIBLE MEAT OFFAL.", "CountryC", 50_000.0, 3),
      Row("MEAT AND EDIBLE MEAT OFFAL.", "CountryD", 10_000.0, 4)
    )

    val inputData: DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    ExportWriter.writeData(inputData, "2018")

    val outputPath = new File("output/2018/MEAT AND EDIBLE MEAT OFFAL./exportRanking.csv")

    outputPath.exists() should be (true)
  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }
}