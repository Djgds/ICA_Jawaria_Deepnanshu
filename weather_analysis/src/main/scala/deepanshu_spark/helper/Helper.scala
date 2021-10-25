package deepanshu_spark.helper
import org.apache.spark.sql.SparkSession

trait SparkConfiguration {

 val spark =SparkSession.builder().enableHiveSupport().getOrCreate()

  def stopSpark(): Unit = spark.stop()
}