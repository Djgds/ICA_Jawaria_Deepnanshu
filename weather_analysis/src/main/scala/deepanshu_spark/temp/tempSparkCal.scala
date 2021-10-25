package deepanshu_spark.temp
import deepanshu_spark.helper.SparkConfiguration
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructType}
import java.io.FileNotFoundException

object tempSparkCal extends SparkConfiguration {

  // logger
  val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {

    // Naming spark program
    spark.conf.set("spark.app.name", "Temp Data Cleaning")
    log.info("Temperature data cleaning started")
    tempAnalysis()
    stopSpark()
  }

  def tempAnalysis(): Unit = {

    try {
/** manual temp data */
      val ManualSchema = new StructType()
        .add("year", StringType)
        .add("month", StringType)
        .add("day", StringType)
        .add("morning", StringType)
        .add("noon", StringType)
        .add("evening", StringType)
        .add("tmin", StringType)
        .add("tmax", StringType)
        .add("estimatedDiurnalMean", StringType)

      val mStationTempDF=spark.read.schema(ManualSchema).textFile("temperature.manual.input.dir")
      // Add station column
      val mStationDF = mStationTempDF.withColumn("station", lit("manual"))

     /**automatic temp data */
     val autoSchema = new StructType()
       .add("year", StringType)
       .add("month", StringType)
       .add("day", StringType)
       .add("morning", StringType)
       .add("noon", StringType)
       .add("evening", StringType)
       .add("tmin", StringType)
       .add("tmax", StringType)
       .add("estimatedDiurnalMean", StringType)

      val aStationTempDf=spark.read.schema(autoSchema).textFile("temperature.automatic.input.dir")
      // Add station column
      val aStationDF = aStationTempDf.withColumn("station", lit("automatic"))

 /**temperature data containing spaces*/
  val uncleanSchema = new StructType()
   .add("extra", StringType)
   .add("year", StringType)
   .add("month", StringType)
   .add("day", StringType)
   .add("morning", StringType)
   .add("evening", StringType)

val sTempDataDf=spark.read.schema(uncleanSchema).textFile("temperature.space.input.dir")
      // Add necessary columns to unify all the input data
      val uncleanTempCleansedDF = sTempDataDf
        .drop("extra")
        .withColumn("tmin", lit("NaN"))
        .withColumn("tmax", lit("NaN"))
        .withColumn("estimatedDiurnalMean", lit("NaN"))
        .withColumn("station", lit("NaN"))

   /** Read  unchanged  data */

      val actualSchema = new StructType()
        .add("year", StringType)
        .add("month", StringType)
        .add("day", StringType)
        .add("morning", StringType)
        .add("noon", StringType)
        .add("evening", StringType)

      val tempDataDf = spark.read.schema(actualSchema).textFile("temperature.actual.input.dir")

      val tempDataDF = tempDataDf
        .withColumn("tmin", lit("NaN"))
        .withColumn("tmax", lit("NaN"))
        .withColumn("estimatedDiurnalMean", lit("NaN"))
        .withColumn("station", lit("NaN"))

      // Joining all the input data to make as one data frame
      val tempDF = mStationDF
        .union(aStationDF)
        .union(uncleanTempCleansedDF)
        .union(tempDataDF)


      // Create hive table query
      import spark.sql
      sql("""CREATE TABLE TempData(
        year String, 
        month String, 
        day String, 
        morning String, 
        noon String, 
        evening String, 
        tmin String, 
        tmax String, 
        estimated_diurnal_mean String, 
        station String)
      STORED AS PARQUET""")

      // Write to hive table
      tempDF.write.mode(SaveMode.Overwrite).saveAsTable("TempData")


      //Data Reconcilation

      val totalInputCount = mStationDF.count() +
        aStationDF.count() +
        uncleanTempCleansedDF.count() +
        tempDataDF.count()
      log.info("Input data count is " + totalInputCount)
      log.info("Transformed input data count is " + tempDF.count())

      log.info("Hive data count " + sql("SELECT count(*) FROM TempData").show(false))
    }
    catch {
      case fileNotFoundException: FileNotFoundException => {
        log.error("Input file not found")

      }
      case exception: Exception => {
        log.error("Exception found " + exception)

      }
    }
  }
}