val df = spark.read.option("header", true).csv("/traffic-data/march-2019.csv")

import org.apache.spark.sql.{SaveMode, SparkSession}

object TrafficCountCleaning {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("TrafficCountCleaning")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //spark read csv file
    val df = spark.read.option("header", true).csv("/traffic-data/march-2019.csv")
    // df.show()
    // df.printSchema()
    val df_noBin_filterThru = df.drop(df.col("bin_duration")).filter("movement == 'THRU'")

    //read with custom schema
    /*
    import org.apache.spark.sql.types._
    val schema = new StructType()
      .add("RecordNumber",IntegerType,true)
    */

    //Write dataframe back to single csv file
    val df_cleaned = df_noBin_filterThru
      .coalesce(1)
      .write
      .option("header", "true")
      .option("sep",",")
      .mode("overwrite")
      .csv("/traffic-data/march-2019-clean.csv")
  }
}