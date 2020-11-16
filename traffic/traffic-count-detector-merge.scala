import org.apache.spark.sql.{SaveMode, SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object TrafficCountDetectorMerge {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("TrafficCountDetectorMerge")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //spark read csv file (note: needs to be repeated for all months)
    val traffic_data = spark.read.option("header", true).csv("/traffic-data/clean/march-2019-clean.csv")
    val traffic_detectors = spark.read.option("header", true).csv("/traffic-data/traffic-detectors.csv")
    val td_droppedCols = traffic_detectors.drop("detector_type", "detector_direction", "detector_movement", "location_name",
      "atd_location_id", "signal_id", "created_date", "modified_date", "ip_comm_status", "comm_status_datetime_utc")

    var resultDf = traffic_data.join(td_droppedCols, traffic_data("atd_device_id") === td_droppedCols("detector_id"))
    resultDf = resultDf.drop("detector_id", "sum_speed")

    //Write dataframe back to single csv file
    val countDetectorMerged = resultDf
      .coalesce(1)
      .write
      .option("header", "true")
      .option("sep",",")
      .mode("overwrite")
      .csv("/traffic-data/merged/march-2019-merged.csv")
  }
}