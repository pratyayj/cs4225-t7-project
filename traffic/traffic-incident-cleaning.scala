import org.apache.spark.sql.{SaveMode, SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.year

object TrafficCountDetectorMerge {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("TrafficCountDetectorMerge")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //spark read csv file (note: needs to be repeated for 2020)
    val accidents_data_2019 = spark.read.option("header", true).csv("/traffic-data/accidents/accidents-2019.csv")
    val ad_2019_droppedCols = accidents_data_2019.drop("traffic_report_status_date_time", "traffic_report_status",
      "address", "location", "latitude", "longitude")
    val ad_2019_cast = ad_2019_droppedCols.withColumn("published_date", $"published_date".cast("timestamp"))
    val ad_2019_withYearMonth = ad_2019_cast.withColumn("year", year($"published_date")).withColumn("month", month($"published_date"))
      .withColumn("year-month", (concat((col("year")), lit("-"), (col("month")))))
      .drop("month", "year")
    val ad_2019_filtered = ad_2019_withYearMonth.filter("issue_reported != 'BLOCKED DRIV/ HWY'")
      .filter("issue_reported != 'BOAT ACCIDENT'")
      .filter("issue_reported != 'LOOSE LIVESTOCK'")
      .filter("issue_reported != 'N / HZRD TRFC VIOL'")

     val traffic_volumes = spark.read.option("header", true).csv("/traffic-data/sum-volume-by-month/traffic-volume-sums.csv")
     val traffic_volumes_cast = traffic_volumes.withColumn("total_volume_for_month", $"total_volume_for_month".cast("Double"))
  }
}