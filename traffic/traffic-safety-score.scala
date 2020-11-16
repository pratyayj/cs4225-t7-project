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
    val accidents_data_2019 = spark.read.option("header", true).csv("/traffic-data/accidents/accidents-2020.csv")
    val ad_2019_droppedCols = accidents_data_2019.drop("traffic_report_status_date_time", "traffic_report_status",
      "address", "location", "latitude", "longitude")
    val ad_2019_cast = ad_2019_droppedCols.withColumn("published_date", $"published_date".cast("timestamp"))
    val ad_2019_withYearMonth = ad_2019_cast.withColumn("year", year($"published_date")).withColumn("month", format_string("%02d", month($"published_date")))
      .withColumn("year_month", (concat((col("year")), lit("-"), (col("month")))))
      .drop("month", "year")
    val ad_2019_filtered = ad_2019_withYearMonth.filter("issue_reported != 'BLOCKED DRIV/ HWY'")
      .filter("issue_reported != 'BOAT ACCIDENT'")
      .filter("issue_reported != 'LOOSE LIVESTOCK'")
      .filter("issue_reported != 'N / HZRD TRFC VIOL'")

    val traffic_volumes = spark.read.option("header", true).csv("/traffic-data/sum-volume-by-month/traffic-volume-sums.csv")
    val traffic_volumes_cast = traffic_volumes.withColumn("total_volume_for_month", $"total_volume_for_month".cast("Double"))

    def get_severity_level(issue: String): Int = {
      if (issue == "COLLISION WITH INJURY" 
        || issue == "COLLISN/ LVNG SCN" || issue == "FLEET ACC/ INJURY") {
        2
      } else if (issue == "TRAFFIC FATALITY" || issue == "FLEET ACC/ FATAL"
        || issue == "AUTO/ PED" || issue == "Crash Urgent") {
        3
      } else {
        1
      }
    }

    val func_severity = udf(get_severity_level _)
    val ad_2019_withSeverity = ad_2019_filtered.withColumn("severity_level", func_severity(ad_2019_filtered("issue_reported")))
    val ad_2019_groupedByYearMonth = ad_2019_withSeverity.groupBy("year_month")
    val ad_2019_severitySum = ad_2019_groupedByYearMonth.sum("severity_level")

    val ad_2019_mergedVolume = ad_2019_severitySum.join(traffic_volumes_cast, ad_2019_severitySum("year_month") === traffic_volumes_cast("month")).drop(col("month"))
    val traffic_safety_score = ad_2019_mergedVolume.withColumn("safety_score", (col("sum(severity_level)") / col("total_volume_for_month") * 10000))

    val result = traffic_safety_score.coalesce(1)
      .write
      .option("header", "true")
      .option("sep",",")
      .mode("overwrite")
      .csv("/traffic-data/accidents/traffic-safety-2020.csv")
  }
}