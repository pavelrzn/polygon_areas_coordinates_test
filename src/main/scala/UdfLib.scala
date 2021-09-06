import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object UdfLib {

  // дашки, часть кального, олимпийский, часть соколовки
  val polygonSE: TestPolygon = new TestPolygon()
  polygonSE.addPoint(54.626823, 39.793990)
  polygonSE.addPoint(54.606077, 39.776514)
  polygonSE.addPoint(54.589725, 39.816530)
  polygonSE.addPoint(54.610113, 39.855568)
  // канищево, приокский, московский от м5 молла до дягилево
  val polygonNW: TestPolygon = new TestPolygon()
  polygonNW.addPoint(54.710203, 39.600976)
  polygonNW.addPoint(54.670848, 39.705156)
  polygonNW.addPoint(54.634902, 39.620188)
  polygonNW.addPoint(54.662467, 39.549083)
  // центр
  val polygonCenter: TestPolygon = new TestPolygon()
  polygonCenter.addPoint(54.638579, 39.736846)
  polygonCenter.addPoint(54.627778, 39.766358)
  polygonCenter.addPoint(54.615365, 39.744175)
  polygonCenter.addPoint(54.623411, 39.701591)


  def defineArea: UserDefinedFunction = udf((x: Double, y: Double) => {
    if (polygonCenter.isContains(x, y)) "Центр"
    else if (polygonNW.isContains(x, y)) "Северо-Запад"
    else if (polygonSE.isContains(x, y)) "Юго-Восток"
    else null

  })

}
