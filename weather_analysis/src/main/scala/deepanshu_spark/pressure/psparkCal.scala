package deepanshu_spark.pressure
import deepanshu_spark.helper.SparkConfiguration
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructType}
import java.io.FileNotFoundException

object psparkCal extends SparkConfiguration {

  // logger
  val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {

    // Naming spark program
    spark.conf.set("spark.app.name", " transforming pressure data  ")
    log.info("pressure transformation started")
    Cleanup()
    stopSpark()
  }

  def Cleanup(): Unit = {
    try {
      val defaultSchema = new StructType()
        .add("year", StringType)
        .add("month", StringType)
        .add("day", StringType)
        .add("pressure_morning", StringType)
        .add("pressure_noon", StringType)
        .add("pressure_evening", StringType)

      val pressureDf = spark.read.schema(defaultSchema).textFile("pressure.manual.input.dir")

      val mPressureSchemaDF = pressureDf
        .withColumn("station", lit("manual"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      val pressureDataDF = mPressureSchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      /** automatic presssure data */
      val aPressureDf = spark.read.schema(defaultSchema).textFile("pressure.automatic.input.dir")

      val aPressureSchemaDF = aPressureDf
        .withColumn("station", lit("Automatic"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      val aPressureDataDF = aPressureSchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      //    Cleaned  Pressure Data
      val schema1938 = new StructType()
        .add("extra", StringType)
        .add("year", StringType)
        .add("month", StringType)
        .add("day", StringType)
        .add("pressure_morning", StringType)
        .add("pressure_noon", StringType)
        .add("pressure_evening", StringType)

      val pressure1938Df = spark.read.schema(schema1938).textFile("pressure.1938.input.dir")

      val pressureTemp1938SchemaDF = pressure1938Df
        .drop("extra")
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      val pressureFinal1938DF = pressureTemp1938SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      // Cleaned  Pressure Data (1862_mmhg)
      val schema1862 = new StructType()
        .add("year", StringType)
        .add("month", StringType)
        .add("day", StringType)
        .add("pressure_morning", StringType)
        .add("pressure_noon", StringType)
        .add("pressure_evening", StringType)

      val pressureRaw1862DF = spark.read.schema(schema1862).textFile("pressure.1862.input.dir")

      val pressure1862SchemaDF = pressureRaw1862DF
        .drop("extra")
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("mmhg"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      val pressureFinal1862DF = pressure1862SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      //  Pressure Data (1756)
      val Schema1756 = new StructType()
        .add("year", StringType)
        .add("month", StringType)
        .add("day", StringType)
        .add("pressure_morning", StringType)
        .add("barometer_temperature_observations_1", StringType)
        .add("pressure_noon", StringType)
        .add("barometer_temperature_observations_2", StringType)
        .add("pressure_evening", StringType)
        .add("barometer_temperature_observations_3", StringType)

      val pressureData1756TempDF = spark.read.schema(Schema1756).textFile("pressure.1756.input.dir")
      val pressureData1756SchemaDF = pressureData1756TempDF
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("Swedish inches (29.69 mm)"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      val pressureFinal1756DF = pressureData1756SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      /** pressure data  1859 */
      val Schema1859 = new StructType()
        .add("year", StringType)
        .add("month", StringType)
        .add("day", StringType)
        .add("pressure_morning", StringType)
        .add("thermometer_observations_1", StringType)
        .add("air_pressure_reduced_to_0_degC_1", StringType)
        .add("pressure_noon", StringType)
        .add("thermometer_observations_2", StringType)
        .add("air_pressure_reduced_to_0_degC_2", StringType)
        .add("pressure_evening", StringType)
        .add("thermometer_observations_3", StringType)
        .add("air_pressure_reduced_to_0_degC_3", StringType)

      val pressureRaw1859Df = spark.read.schema(Schema1859).textFile("presssure.1859.input.dir")

      val pressure1859SchemaDF = pressureRaw1859Df
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("0.1*Swedish inches (2.969 mm)"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))

      val pressureFinal1859DF = pressure1859SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      //                Pressure Data (1961)
      val schema1961 = new StructType()
        .add("year", StringType)
        .add("month", StringType)
        .add("day", StringType)
        .add("pressure_morning", StringType)
        .add("pressure_noon", StringType)
        .add("pressure_evening", StringType)
      val pressureRaw1961Df = spark.read.schema(schema1961).textFile("presssure.1961.input.dir")

      val pressure1961SchemaDF = pressureRaw1961Df
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      val pressureFinal1961DF = pressure1961SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      val pressureDF = pressureFinal1961DF.union(pressureFinal1859DF).union()
      // Final transformed pressure data
      val pressureDF = pressureDataDF
        .union(aPressureDataDF)
        .union(pressureFinal1938DF)
        .union(pressureFinal1862DF)
        .union(pressureFinal1756DF)
        .union(pressureFinal1859DF)
        .union(pressureFinal1961DF)

      /** saving to hive table */
      import spark.sql
      sql(
        """CREATE TABLE PressureData(
        year String,
        month String,
        day String,
        pressure_morning String,
        pressure_noon String,
        pressure_evening String,
        station String,
        pressure_unit String,
        barometer_temperature_observations_1 String,
        barometer_temperature_observations_2 String,
        barometer_temperature_observations_3 String,
        thermometer_observations_1 String,
        thermometer_observations_2 String,
        thermometer_observations_3 String,
        air_pressure_reduced_to_0_degC_1 String,
        air_pressure_reduced_to_0_degC_2 String,
        air_pressure_reduced_to_0_degC_3 String)
      STORED AS PARQUET""")
      pressureDF.write.mode(SaveMode.Overwrite).saveAsTable("PresData")

      /** data reconcilation */
      val totalInputCount = pressureDataDF.count() +
        aPressureDataDF.count() +
        pressureFinal1938DF.count() +
        pressureFinal1862DF.count() +
        pressureFinal1756DF.count() +
        pressureFinal1859DF.count() +
        pressureFinal1961DF.count()

      log.info("Input data count is " + totalInputCount)
      log.info("Transformed input data count is " + pressureDF.count())
      log.info("Hive data count " + sql("SELECT count(*) as count FROM PresData").show(false))

    } catch {
      case fileNotFoundException: FileNotFoundException => {
        log.error("Input file not found")

      }
      case exception: Exception => {
        log.error("Exception found " + exception)

      }
    }
  }
}
