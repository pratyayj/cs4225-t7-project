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
    val inputFiles = "/traffic-data/ts-by-intersection-day.csv/*.csv"
    val outputFiles = "/traffic-data/ts-by-month.csv"

    //spark read csv file
    val traffic_data = spark.read.option("header", true).csv(inputFiles)

    // cast column
    var casted_data = traffic_data.withColumn("traffic_score", col("traffic_score").cast(DoubleType))

    // create month and year columns
    val data = casted_data.withColumn("Month", split(col("day"),"-").getItem(1)).withColumn("Year", split(col("day"),"-").getItem(0))
    
    // group data by month and year and find overall TS
    var grouped_data = data.groupBy("Month", "Year").avg("traffic_score")

    // clean data
    grouped_data = grouped_data.orderBy("Year", "Month").withColumnRenamed("avg(traffic_score)","Traffic Score")

    //Write dataframe back to single csv file
    val intersectionCombinedByMonth = grouped_data.coalesce(1)
      .write
      .option("header", "true")
      .option("sep",",")
      .mode("overwrite")
      .csv(outputFiles)
  }
}
