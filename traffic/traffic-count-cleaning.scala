val df = spark.read.option("header", true).csv("/traffic-data/march-2019.csv")

import org.apache.spark.sql.{SaveMode, SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object TrafficCountCleaning {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("TrafficCountCleaning")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //spark read csv file
    val df = spark.read.option("header", true).csv("/traffic-data/march-2019.csv")
    val df_noBin_filterThru = df.drop(df.col("bin_duration")).filter("movement == 'THRU'")
    
    // total speed to recalculate combined average speed
    val df_wTotalSpeed = df_noBin_filterThru.withColumn("total_speed", (col("volume") * col("speed_average")))
    
    // cast volume to double
    var df_wTotalSpeedCast = df_wTotalSpeed.withColumn("volume", col("volume").cast(DoubleType))
    
    // to get sum speed and volume for heavy and non-heavy vehicles
    var grouped = df_wTotalSpeedCast.groupBy("read_date", "intersection_name", "direction")
    var grouped_sum = grouped.sum("volume", "total_speed") // referenced as sum(volume) and sum(total_speed)
    grouped_sum = grouped_sum.withColumnRenamed("sum(volume)","sum_volume").withColumnRenamed("sum(total_speed)","sum_speed")
    
    // average speed across both heavy and non-heavy vehicles
    val overall_average_speed = grouped_sum.withColumn("overall_average_speed", col("sum_speed") / col("sum_volume"))

    //read with custom schema
    /*
    import org.apache.spark.sql.types._
    val schema = new StructType()
      .add("RecordNumber",IntegerType,true)
    */

    //Write dataframe back to single csv file
    val df_prepared = overall_average_speed
      .coalesce(1)
      .write
      .option("header", "true")
      .option("sep",",")
      .mode("overwrite")
      .csv("/traffic-data/march-2019-clean.csv")
  }
}