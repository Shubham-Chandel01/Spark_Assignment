package question2
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfter

class ExportAggregatorDriverSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {
  private var spark: SparkSession = _

  before {
    spark = SparkSession.builder()
      .appName("Export Aggregator Driver Test")
      .master("local[*]")
      .getOrCreate()
  }

  "main" should "run without errors" in {
    val args = Array("2018")
    ExportAggregatorDriver.main(args)
  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }
}