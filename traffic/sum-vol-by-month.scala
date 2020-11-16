
import org.apache.spark.sql.{SaveMode, SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SumVolByMonth {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Merge")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val trafficFile = "/traffic-data/combine-vol-ave-speed-with-limits/*.csv"
    val outfile = "/traffic-data/sum-volume-by-month"

    // read csv file into dataframe
    val traffic_data = spark.read.option("header", true).csv(trafficFile)

    // cast columns
    var casted = traffic_data
        .withColumn("total_volume", col("total_volume").cast(DoubleType))

    // Create new column "month" to group on
    var casted_with_month = casted.withColumn("month", substring(col("read_date"), 1, 7))

    // Sum volume group by month
    var total_vol_month = casted_with_month
        .groupBy("month")
        .sum("total_volume")
        .withColumnRenamed("sum(total_volume)","total_volume_for_month")

    //Write dataframe back to single csv file
    val countDetectorMerged = total_vol_month
      .coalesce(1)
      .write
      .option("header", "true")
      .option("sep",",")
      .mode("overwrite")
      .csv(outfile)
  }
}