import org.apache.spark.sql.{SaveMode, SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object IntervalCombineVolumeSpeedWithLimits {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("IntervalCombineCalc")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //spark read csv file
    val clean_traffic_data = spark.read.option("header", true).csv("/traffic-data/clean/*-clean.csv")
    
    // sort data by time stamp and device id
    var ordered_data = clean_traffic_data.orderBy("read_date","atd_device_id")
    
    // cast volume and speed to double
    var ordered_dataCast = ordered_data.withColumn("sum_volume", col("sum_volume").cast(DoubleType)).withColumn("sum_speed", col("sum_speed").cast(DoubleType))

    // group by device id and interval timing for a particular intersection
    var grouped_data = ordered_dataCast.groupBy("atd_device_id", "read_date", "intersection_name")

    // find the total volume and sum of speeds across all vehicles (total speeds = vol * avg speed for that direction)
    var grouped_data_with_sums = grouped_data.sum("sum_volume", "sum_speed")
    grouped_data_with_sums = grouped_data_with_sums.withColumnRenamed("sum(sum_volume)","total_volume").withColumnRenamed("sum(sum_speed)","total_speeds")

    // gives the weighted average speed for that intersection for one interval
    var overall_grouped_data = grouped_data_with_sums.withColumn("weighted_avg_speed", col("total_speeds") / col("total_volume"))

    // SPEED LIMITS
    val speed_limits_file = "/traffic-data/speed-limits.csv"
    val speedlimit_df = spark.read.option("header", true).csv(speed_limits_file)

    // Join traffic data with speed limits
    var overall_grouped_with_limits = overall_grouped_data.join(speedlimit_df, "atd_device_id")

    //Write dataframe back to single csv file
    val intersectionCombined = overall_grouped_with_limits
      .write
      .option("header", "true")
      .option("sep",",")
      .mode("overwrite")
      .csv("/traffic-data/combine-vol-ave-speed-with-limits")
  }
}
