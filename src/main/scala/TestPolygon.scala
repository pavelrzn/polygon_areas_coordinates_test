import UdfLib.defineArea
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, from_unixtime, lit}
import org.apache.spark.storage.StorageLevel

import java.awt.Polygon


object TestPolygon extends App {

  val spark = SparkSession.builder()
    .appName("air")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val comments = spark.read.option("header", value = true).csv("./ecoMon2_without_fio.csv")
    .withColumn("date", from_unixtime(col("comment_time"), "yyyyMMdd"))
    .withColumn("time", from_unixtime(col("comment_time"), "HHmmss"))
    .drop("comment_time")
    .withColumn("area", defineArea(col("latitude"), col("longitude")))
    .where(col("area").isNotNull)
    .persist(StorageLevel.MEMORY_ONLY)

  comments.show(5)
//  comments.write.csv("./areas")

  comments.where(col("area") === "Северо-Запад")
    .select("latitude", "longitude")
    .withColumn("weight", lit("1"))
    .write.csv("./South")

  comments.groupBy(col("area")).agg(count("area")).show(false)

  spark.close()

}

class TestPolygon() extends Polygon {

  def addPoint(dx: Double, dy: Double): Unit = {
    val xy = d2Int(dx, dy)
    super.addPoint(xy._1, xy._2)
  }

  def isContains(x: Double, y: Double): Boolean = {
    val xy = d2Int(x, y)
    super.contains(xy._1, xy._2)
  }

  private def d2Int(x: Double, y: Double): (Int, Int) = {
    ((x * 100000).toInt, (y * 100000).toInt)
  }
}
