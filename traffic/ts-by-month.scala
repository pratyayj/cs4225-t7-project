import org.apache.spark.sql.{SaveMode, SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object TSByMonth {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("MonthTS")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // input and output file paths
    val inputFiles = "/traffic-data/ts-by-day/*.csv"
    val outputFiles = "/traffic-data/ts-by-month"

    //spark read csv file
    val traffic_data = spark.read.option("header", true).csv(inputFiles)

    // cast column
    var casted_data = traffic_data.withColumn("weighted_TS", col("weighted_TS").cast(DoubleType))

    // create month and year columns
    val data = casted_data.withColumn("Month", split(col("day"),"-").getItem(1)).withColumn("Year", split(col("day"),"-").getItem(0))
    
    // group data by month and year and find overall TS
    var grouped_data = data.groupBy("Month", "Year").avg("weighted_TS")

    // clean data
    grouped_data = grouped_data.orderBy("Year", "Month").withColumnRenamed("avg(weighted_TS)","Traffic Score")

    // Write dataframe back to single csv file
    val TSCombinedByMonth = grouped_data.coalesce(1)
      .write
      .option("header", "true")
      .option("sep",",")
      .mode("overwrite")
      .csv(outputFiles)
  }
}
