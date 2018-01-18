/**
  * Created by olga on 17/11/17.
  */


import com.quantifind.charts.Highcharts._


object graphics2 {

  def main(args: Array[String]): Unit = {

    val topWords = Array(("alpha", 14), ("beta", 23), ("omega", 18))
    val numberedColumns = column(topWords.map(_._2).toList)
    val axisType: com.quantifind.charts.highcharts.AxisType.Type = "category"

    val namedColumns = numberedColumns.copy(xAxis = numberedColumns.xAxis.map {
      axisArray => axisArray.map { _.copy(axisType = Option(axisType),
        categories = Option(topWords.map(_._1))) }
    })
    delete()
    plot(namedColumns)


  }

}
