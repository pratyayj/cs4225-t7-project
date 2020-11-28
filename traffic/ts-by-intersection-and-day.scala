
import org.apache.spark.sql.{SaveMode, SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object TSByIntersectionAndDay {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Merge")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val trafficFile = "/traffic-data/combine-vol-ave-speed-with-limits/*.csv"
    val outfile = "/traffic-data/ts-by-intersection-day"

    // read csv file into dataframe
    val traffic_data = spark.read.option("header", true).csv(trafficFile)

    // cast columns
    var casted = traffic_data
        .withColumn("total_volume", col("total_volume").cast(DoubleType))
        .withColumn("weighted_avg_speed", col("weighted_avg_speed").cast(DoubleType))
        .withColumn("speed_limit", col("speed_limit").cast(DoubleType))

    // Create new column "day" to group on
    var casted_with_day = casted.withColumn("day", split(col("read_date"),"T").getItem(0))

    // Calculate the nominator & demonimator
    // nom: weighted_avg_speed * total_volume
    // denom: speed_limit * total_volume
    var speed_vol_calc = casted_with_day
        .withColumn("nom", col("weighted_avg_speed") * col("total_volume"))
        .withColumn("denom", col("speed_limit") * col("total_volume"))

    // Calculate traffic score for ONE intersection for ONE day
    // group by device id and day
    var traffic_scores = speed_vol_calc
        .groupBy("atd_device_id", "day", "intersection_name")
        .sum("nom", "denom")
        .withColumn("traffic_score", col("sum(nom)") / col("sum(denom)"))

    // Calculate total volume for ONE intersection for ONE day
    // group by device id and day
    var vol_per_intersection_day = casted_with_day
        .groupBy("atd_device_id", "day", "intersection_name")
        .sum("total_volume")
        .withColumn("vol_per_xn_day", col("sum(total_volume)"))
    
    // Join to keep volume
    var result = traffic_scores.join(vol_per_intersection_day, Seq("atd_device_id", "day", "intersection_name"))

    // Clean
    result = result
        .drop("nom", "denom", "sum(nom)", "sum(denom)")
        .orderBy("atd_device_id", "day")

    //Write dataframe back to single csv file
    val countDetectorMerged = result
      .write
      .option("header", "true")
      .option("sep",",")
      .mode("overwrite")
      .csv(outfile)
  }
}