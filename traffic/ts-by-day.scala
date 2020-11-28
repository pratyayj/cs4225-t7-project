import org.apache.spark.sql.{SaveMode, SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object TSByDays {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("DailyTS")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // input and output file paths
    val inputFiles = "/traffic-data/ts-by-intersection-day-with-vol/*.csv"
    val outputFiles = "/traffic-data/ts-by-day"

    //spark read csv file
    val traffic_data = spark.read.option("header", true).csv(inputFiles)

    // cast column
    var casted_data = traffic_data.withColumn("traffic_score", col("traffic_score").cast(DoubleType)).withColumn("vol_per_xn_day", col("vol_per_xn_day").cast(DoubleType))

    // calculate vol*TS to get weighted volume
    val data_with_TS_vol = casted_data.withColumn("TS_with_vol", col("traffic_score") * col("vol_per_xn_day"))
    
    // group data by day and find overall TS across all intersections
    var grouped_data = data_with_TS_vol.groupBy("day").sum("TS_with_vol", "vol_per_xn_day")

    // calculate the weighted TS
    var weigthed_TS_data = grouped_data.withColumn("weighted_TS", col("sum(TS_with_vol)") / col("sum(vol_per_xn_day)"))

    // clean data
    weigthed_TS_data = weigthed_TS_data.orderBy("day").drop("TS_with_vol", "vol_per_xn_day", "sum(TS_with_vol)", "sum(vol_per_xn_day)")

    //Write dataframe back to single csv file
    val intersectionCombinedByDay = weigthed_TS_data.coalesce(1)
      .write
      .option("header", "true")
      .option("sep",",")
      .mode("overwrite")
      .csv(outputFiles)
  }
}