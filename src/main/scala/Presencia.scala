import java.io.{File, PrintWriter}
import java.util.{Calendar, GregorianCalendar}

import com.quantifind.charts.Highcharts._
import com.quantifind.charts.highcharts.{Highchart, Title}
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by olga on 2/6/17.
  */
object Presencia{


  def weekendsOfYear(year: Int): List[String] = {
    val m = 0
    var lista: List[String] = List()
    var mday: String = ""
    var month: String = ""
    for (m <- 1 to 12) {
      val cal = new GregorianCalendar(year, m, 1)
      do {
        // get the day of the week for the current day
        val day = cal.get(Calendar.DAY_OF_WEEK)
        // check if it is a Saturday or Sunday
        if (day == Calendar.SATURDAY || day == Calendar.SUNDAY) {
          // print the day - but you could add them to a list or whatever
          val monthDay = cal.get(Calendar.DAY_OF_MONTH)
          if (monthDay.toString.length == 1) {
            mday = "0" + monthDay.toString
          } else {
            mday = monthDay.toString
          }
          if (m.toString.length == 1) {
            month = "0" + m.toString
          } else {
            month = m.toString
          }
          lista = lista :+ (month + "/" + mday)
        }
        // advance to the next day
        cal.add(Calendar.DAY_OF_YEAR, 1)
      } while (cal.get(Calendar.MONTH) == m)
      // stop when we reach the start of the next month
    }
    return lista
  }


  def weekdayOfYear(year: Int, dayOfWeek: Int): List[String] = {
    val m = 0
    var lista: List[String] = List()
    var mday: String = ""
    var month: String = ""
    for (m <- 1 to 12) {
      val cal = new GregorianCalendar(year, m, 1)
      do {
        // get the day of the week for the current day
        val day = cal.get(Calendar.DAY_OF_WEEK)
        // check if it is a Saturday or Sunday
        var calendarDayOfWeek: Int = 0

        dayOfWeek match {
          case 1 => calendarDayOfWeek = Calendar.MONDAY
          case 2 => calendarDayOfWeek = Calendar.TUESDAY
          case 3 => calendarDayOfWeek = Calendar.WEDNESDAY
          case 4 => calendarDayOfWeek = Calendar.THURSDAY
          case 5 => calendarDayOfWeek = Calendar.FRIDAY
          case 6 => calendarDayOfWeek = Calendar.SATURDAY
          case 7 => calendarDayOfWeek = Calendar.SUNDAY
        }
        if (day == calendarDayOfWeek) {
          // print the day - but you could add them to a list or whatever
          val monthDay = cal.get(Calendar.DAY_OF_MONTH)
          if (monthDay.toString.length == 1) {
            mday = "0" + monthDay.toString
          } else {
            mday = monthDay.toString
          }
          if (m.toString.length == 1) {
            month = "0" + m.toString
          } else {
            month = m.toString
          }
          lista = lista :+ (month + "/" + mday)
        }
        // advance to the next day
        cal.add(Calendar.DAY_OF_YEAR, 1)
      } while (cal.get(Calendar.MONTH) == m)
      // stop when we reach the start of the next month
    }
    return lista
  }

  def plotColumns(variable: RDD[(String,Double)] , categoryType: String, titulo: String): Highchart ={
    val topWords = variable.collect().toList.toArray
    val numberedColumns = column(topWords.map(_._2).toList)
    delete()
    val axisType: com.quantifind.charts.highcharts.AxisType.Type = categoryType
    val title = Option(new Title(text = titulo))
    val namedColumns = numberedColumns.copy(xAxis = numberedColumns.xAxis.map {
      axisArray => axisArray.map { _.copy(axisType = Option(axisType),
        categories = Option(topWords.map(_._1)))}
    }, title = title)

    plot(namedColumns)
  }




  def main(args: Array[String]): Unit = {
   Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //System.setProperty("hadoop.home.dir", "c:\\Winutil\\")

    val conf = new SparkConf().setAppName("Presencia").setMaster("local")
    val sc = new SparkContext(conf)

    val zeroHour = List("0:00")

    val minutes15 = List("0:00", "0:15", "0:30", "0:45","1:00", "1:15","1:30", "1:45","2:00", "2:15", "2:30", "2:45","3:00","3:15", "3:30", "3:45","4:00","4:15", "4:30", "4:45","5:00", "5:15", "5:30", "5:45","6:00","6:15", "6:30", "6:45","7:00", "7:15", "7:30", "7:45","8:00", "8:15", "8:30", "8:45","9:00", "9:15", "9:30", "9:45","10:00", "10:15", "10:30", "10:45","11:00", "11:15", "11:30", "11:45","12:00", "12:15", "12:30", "12:45","13:00", "13:15", "13:30", "13:45","14:00", "14:15", "14:30", "14:45","15:00", "15:15", "15:30", "15:45","16:00", "16:15", "16:30", "16:45","17:00", "17:15", "17:30", "17:45","18:00", "18:15", "18:30", "18:45","19:00", "19:15", "19:30", "19:45","20:00", "20:15", "20:30", "20:45","21:00", "21:15", "21:30", "21:45","22:00", "22:15", "22:30", "22:45","23:00", "23:15", "23:30", "23:45")
    val minutesZero15 = List("00:00", "00:15", "00:30", "00:45","01:00", "01:15","01:30", "01:45","02:00", "02:15", "02:30", "02:45","03:00","03:15", "03:30", "03:45","04:00","04:15", "04:30", "04:45","05:00", "05:15", "05:30", "05:45","06:00","06:15", "06:30", "06:45","07:00", "07:15", "07:30", "07:45","08:00", "08:15", "08:30", "08:45","09:00", "09:15", "09:30", "09:45","10:00", "10:15", "10:30", "10:45","11:00", "11:15", "11:30", "11:45","12:00", "12:15", "12:30", "12:45","13:00", "13:15", "13:30", "13:45","14:00", "14:15", "14:30", "14:45","15:00", "15:15", "15:30", "15:45","16:00", "16:15", "16:30", "16:45","17:00", "17:15", "17:30", "17:45","18:00", "18:15", "18:30", "18:45","19:00", "19:15", "19:30", "19:45","20:00", "20:15", "20:30", "20:45","21:00", "21:15", "21:30", "21:45","22:00", "22:15", "22:30", "22:45","23:00", "23:15", "23:30", "23:45")

    val hours = List(
      "0:00", "0:05", "0:10", "0:15", "0:20", "0:25", "0:30", "0:35", "0:40", "0:45", "0:50", "0:55",
      "1:00", "1:05", "1:10", "1:15", "1:20", "1:25", "1:30", "1:35", "1:40", "1:45", "1:50", "1:55",
      "2:00", "2:05", "2:10", "2:15", "2:20", "2:25", "2:30", "2:35", "2:40", "2:45", "2:50", "2:55",
      "3:00", "3:05", "3:10", "3:15", "3:20", "3:25", "3:30", "3:35", "3:40", "3:45", "3:50", "3:55",
      "4:00", "4:05", "4:10", "4:15", "4:20", "4:25", "4:30", "4:35", "4:40", "4:45", "4:50", "4:55",
      "5:00", "5:05", "5:10", "5:15", "5:20", "5:25", "5:30", "5:35", "5:40", "5:45", "5:50", "5:55",
      "6:00", "6:05", "6:10", "6:15", "6:20", "6:25", "6:30", "6:35", "6:40", "6:45", "6:50", "6:55",
      "7:00", "7:05", "7:10", "7:15", "7:20", "7:25", "7:30", "7:35", "7:40", "7:45", "7:50", "7:55",
      "8:00", "8:05", "8:10", "8:15", "8:20", "8:25", "8:30", "8:35", "8:40", "8:45", "8:50", "8:55",
      "9:00", "9:05", "9:10", "9:15", "9:20", "9:25", "9:30", "9:35", "9:40", "9:45", "9:50", "9:55",
      "10:00", "10:05", "10:10", "10:15", "10:20", "10:25", "10:30", "10:35", "10:40", "10:45", "10:50", "10:55",
      "11:00", "11:05", "11:10", "11:15", "11:20", "11:25", "11:30", "11:35", "11:40", "11:45", "11:50", "11:55",
      "12:00", "12:05", "12:10", "12:15", "12:20", "12:25", "12:30", "12:35", "12:40", "12:45", "12:50", "12:55",
      "13:00", "13:05", "13:10", "13:15", "13:20", "13:25", "13:30", "13:35", "13:40", "13:45", "13:50", "13:55",
      "14:00", "14:05", "14:10", "14:15", "14:20", "14:25", "14:30", "14:35", "14:40", "14:45", "14:50", "14:55",
      "15:00", "15:05", "15:10", "15:15", "15:20", "15:25", "15:30", "15:35", "15:40", "15:45", "15:50", "15:55",
      "16:00", "16:05", "16:10", "16:15", "16:20", "16:25", "16:30", "16:35", "16:40", "16:45", "16:50", "16:55",
      "17:00", "17:05", "17:10", "17:15", "17:20", "17:25", "17:30", "17:35", "17:40", "17:45", "17:50", "17:55",
      "18:00", "18:05", "18:10", "18:15", "18:20", "18:25", "18:30", "18:35", "18:40", "18:45", "18:50", "18:55",
      "19:00", "19:05", "19:10", "19:15", "19:20", "19:25", "19:30", "19:35", "19:40", "19:45", "19:50", "19:55",
      "20:00", "20:05", "20:10", "20:15", "20:20", "20:25", "20:30", "20:35", "20:40", "20:45", "20:50", "20:55",
      "21:00", "21:05", "21:10", "21:15", "21:20", "21:25", "21:30", "21:35", "21:40", "21:45", "21:50", "21:55",
      "22:00", "22:05", "22:10", "22:15", "22:20", "22:25", "22:30", "22:35", "22:40", "22:45", "22:50", "22:55",
      "23:00", "23:05", "23:10", "23:15", "23:20", "23:25", "23:30", "23:35", "23:40", "23:45", "23:50", "23:55")

    val hoursZero = List(
      "00:00","00:05","00:10","00:15","00:20","00:25","00:30","00:35","00:40","00:45","00:50","00:55",
      "01:00","01:05","01:10","01:15","01:20","01:25","01:30","01:35","01:40","01:45","01:50","01:55",
      "02:00","02:05","02:10","02:15","02:20","02:25","02:30","02:35","02:40","02:45","02:50","02:55",
      "03:00","03:05","03:10","03:15","03:20","03:25","03:30","03:35","03:40","03:45","03:50","03:55",
      "04:00","04:05","04:10","04:15","04:20","04:25","04:30","04:35","04:40","04:45","04:50","04:55",
      "05:00","05:05","05:10","05:15","05:20","05:25","05:30","05:35","05:40","05:45","05:50","05:55",
      "06:00","06:05","06:10","06:15","06:20","06:25","06:30","06:35","06:40","06:45","06:50","06:55",
      "07:00","07:05","07:10","07:15","07:20","07:25","07:30","07:35","07:40","07:45","07:50","07:55",
      "08:00","08:05","08:10","08:15","08:20","08:25","08:30","08:35","08:40","08:45","08:50","08:55",
      "09:00","09:05","09:10","09:15","09:20","09:25","09:30","09:35","09:40","09:45","09:50","09:55",
      "10:00","10:05","10:10","10:15","10:20","10:25","10:30","10:35","10:40","10:45","10:50","10:55",
      "11:00","11:05","11:10","11:15","11:20","11:25","11:30","11:35","11:40","11:45","11:50","11:55",
      "12:00","12:05","12:10","12:15","12:20","12:25","12:30","12:35","12:40","12:45","12:50","12:55",
      "13:00","13:05","13:10","13:15","13:20","13:25","13:30","13:35","13:40","13:45","13:50","13:55",
      "14:00","14:05","14:10","14:15","14:20","14:25","14:30","14:35","14:40","14:45","14:50","14:55",
      "15:00","15:05","15:10","15:15","15:20","15:25","15:30","15:35","15:40","15:45","15:50","15:55",
      "16:00","16:05","16:10","16:15","16:20","16:25","16:30","16:35","16:40","16:45","16:50","16:55",
      "17:00","17:05","17:10","17:15","17:20","17:25","17:30","17:35","17:40","17:45","17:50","17:55",
      "18:00","18:05","18:10","18:15","18:20","18:25","18:30","18:35","18:40","18:45","18:50","18:55",
      "19:00","19:05","19:10","19:15","19:20","19:25","19:30","19:35","19:40","19:45","19:50","19:55",
      "20:00","20:05","20:10","20:15","20:20","20:25","20:30","20:35","20:40","20:45","20:50","20:55",
      "21:00","21:05","21:10","21:15","21:20","21:25","21:30","21:35","21:40","21:45","21:50","21:55",
      "22:00","22:05","22:10","22:15","22:20","22:25","22:30","22:35","22:40","22:45","22:50","22:55",
      "23:00","23:05","23:10","23:15","23:20","23:25","23:30","23:35","23:40","23:45","23:50","23:55")


    val dates365 = sc.textFile("365_ndformat.csv")
    val dates366 = sc.textFile("366_ndformat.csv")

    val avaibleHours = hours

    val id = "48"

    val part = ""

    //val anyo = List("2016","2017")
    val anyo = List("2015","2016","2017")
    //val anyo = List("2010","2011","2012","2013","2014","2015","2016", "2017")

    val file = sc.textFile("Datos Presencia/" + id + "/Edificio " + id + " " +" presencia" + ".csv")

    for (a <- anyo) {

      val pw = new PrintWriter(new File(id + " "+part+" " + a + ".txt"))


      pw.write(a+"\n")

      val data = file.map(line => {


        val Array(d, m, y, h, min, s, percent) = line.replaceAll(" |/|:", ";").split(";").map(_.trim)

        val percentage = percent.toDouble
        val year = y.toInt
        val month = m.toInt
        val day = d.toInt
        val hour = h.toInt
        val minute = min.toInt

        val date = m + "/" + d
        val time = h + ":" + min


        (id, percentage, year, month, day, hour, minute, date, time)

      })


      val year = data.collect().apply(1)._3
      //Take the year of first data register (is the same of whole file)


      //Leap-year?

      var leapYear = false
      if ((year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0))) {
        leapYear = true
      }

      val datetime = data.map(line => (line._8, line._9)).groupByKey().sortBy(_._1)

      //Test if we have missing days

      pw.write("\n----------MISSING VALUES-----------\n")

      def missingDays(year: Int, leapYear: Boolean, mapDateTime: RDD[(String, Iterable[String])]): List[String] = {
        if (leapYear) {
          pw.write((366 - (mapDateTime.collect().length)) + " days missing\n")
          val list1 = dates366.collect().toList
          val list2 = mapDateTime.keys.collect().toList
          val list = list1.diff(list2)
          pw.write(list.toString())
          return list
        } else {
          pw.write((365 - (mapDateTime.collect().length)) + " days missing\n")
          val list1 = dates365.collect().toList
          val list2 = mapDateTime.keys.collect().toList
          val list = list1.diff(list2)
          pw.write(list.toString())
          return list
        }
      }

      //Missing days

      val mDays = missingDays(year, leapYear, datetime)

      /*// Test missing hours per day

      pw.write("\n Days and their missing hours")

      val daysMissing = datetime.map(line => (line._1, avaibleHours.diff(line._2.toList))).sortBy(_._1)

      pw.write(daysMissing.filter(_._2.nonEmpty).collect().toList)

*/
      pw.write("\n----------------------------------\n") // End of Missing Values

      pw.write("\nValues zero\n")

      val consumoBusca0 = data.map(line => (line._8 + " - " + line._9, line._2)).reduceByKey(_ + _).sortBy(_._1)

      val consumo0 = consumoBusca0.collect().toList.filter(s => s._2 == 0)

      pw.write(consumo0.length.toString)

      pw.write("\nValues less than zero\n")

      val consumoless = consumoBusca0.collect().toList.filter(s => s._2 < 0)

      pw.write(consumoless.length.toString)

      pw.write("\n ----------------------------------\n")

/*
      pw.write("Day occupancy")

      val consumoDia = data.map(line => (line._8.toString(), line._2)).groupByKey().sortBy(_._1)

      val consumoPorDia = consumoDia.map(line => (line._1, line._2.sum / line._2.size)).sortBy(_._1)
      pw.write(consumoPorDia.collect().toList)

      // plotColumns(consumoDia,"datetime","Day occupancy of year "+year )

      val average = consumoPorDia.values.sum() / (consumoDia.values.collect().size)

      pw.write("\nAverage occupancy per day: " + average)


      pw.write("\nDay occupancy per hour")

      val consumoPorHora = data.map(line => (line._8 + " - " + line._6, line._2)).groupByKey().sortBy(_._1)

      val consumoPorHora2 = consumoPorHora.map(line => (line._1, line._2.sum / line._2.size)).sortBy(_._1)
      pw.write(consumoPorHora2.collect().toList)

      pw.write("\nAverage occupancy per hour and number of registries")

      val consumoMedioPorHora = data.map(line => (line._8 + " - " + line._6, line._2)).groupByKey().sortBy(_._1)

      val consumoMedioPorHoraRegistros = consumoMedioPorHora.map(line => (line._1, line._2.sum / line._2.size)).sortBy(_._1)

      pw.write(consumoMedioPorHoraRegistros.collect().toList)

      pw.write("\nMonth occupancy")

      val consumoMes2 = data.map(line => (line._4.toString, line._2)).groupByKey().sortBy(_._1)

      val consumoMes = consumoMes2.map(line => (line._1, line._2.sum / line._2.size)).sortBy(_._1)

      pw.write(consumoMes.collect().toList)

      //plotColumns(consumoMes,"datetime","Month occupancy of year "+year )

      pw.write("\nYear average occupancy")

      val consumoAño = consumoMes.values.sum()

      pw.write(consumoAño / 12)

      pw.write("\nWeekends of year and occupancy")

      val weekends = weekendsOfYear(year)

      val weekendsConsumption = consumoPorDia.collect().toMap.filterKeys(k => weekends.contains(k))

      pw.write(weekendsConsumption.toList.sorted)

      pw.write("\nAverage occupancy of weekends")

      pw.write(weekendsConsumption.values.sum / weekendsConsumption.size)

      pw.write("\nWorking days and occupancy")

      val workingDays = consumoPorDia.keys.collect().diff(weekends)

      val workingDaysConsumption = consumoPorDia.collect().toMap.filterKeys(k => workingDays.contains(k))

      pw.write(workingDaysConsumption.toList.sorted)

      pw.write("\nAverage occupancy of working days")

      pw.write(workingDaysConsumption.values.sum / workingDaysConsumption.size)

      var i = 0
      for (i <- 1 to 7) {
        var nameOfDay: String = ""
        i match {
          case 1 => nameOfDay = "Mondays"
          case 2 => nameOfDay = "Tuesdays"
          case 3 => nameOfDay = "Wednesdays"
          case 4 => nameOfDay = "Thursdays"
          case 5 => nameOfDay = "Fridays"
          case 6 => nameOfDay = "Saturdays"
          case 7 => nameOfDay = "Sundays"
        }

        //pw.write("Consumption on " + nameOfDay)

        val weekdays = weekdayOfYear(year, i)

        val occupancy = consumoPorDia.collect().toMap.filterKeys(k => weekdays.contains(k))

        //pw.write(occupancy.toList.sorted)

        pw.write("\nAverage occupancy on " + nameOfDay)

        pw.write(occupancy.values.sum / occupancy.size)
      }


      pw.write("\nMean occupancy of year per hour:minute")

      val consumoHoraMinuto = data.map(line => (line._9, line._2)).groupByKey().sortBy(_._1)

      val chm = consumoHoraMinuto.map(line => (line._1, line._2.sum / line._2.size)).sortBy(_._1)

      pw.write(chm.collect().toList)

      pw.write("\nMean occupancy of month per hour:minute")

      val consumoMesHoraMinuto = data.map(line => (line._4 + "-" + line._9, line._2)).groupByKey().sortBy(_._1)

      val cmhm = consumoMesHoraMinuto.map(line => (line._1, line._2.sum / line._2.size)).sortBy(_._1)

      pw.write(cmhm.collect().toList)

      val max = cmhm.collect().maxBy(_._2)
      val min = cmhm.collect().minBy(_._2)

      pw.write("\nRegister with more occupancy:" + max + " and register with least occupancy: " + min)

      val maxDay = consumoPorDia.collect().maxBy(_._2);
      val minDay = consumoPorDia.collect().minBy(_._2);

      pw.write("\nDay with more occupancy:" + maxDay + " and Day with least occupancy: " + minDay)

      val range = max._2 - min._2
      pw.write("\n Range: " + range)

      var valuesStatistics = new DescriptiveStatistics()
      cmhm.values.collect().foreach(v => valuesStatistics.addValue(v))

      // Get first and third quartiles and then calc IQR
      val Q1 = valuesStatistics.getPercentile(25)
      val Q3 = valuesStatistics.getPercentile(75)
      val IQR = Q3 - Q1

      pw.write("\n IQR: " + IQR)

      //Outliers limits

      val lower = Q1 - 1.5 * IQR
      val upper = Q3 + 1.5 * IQR

      pw.write("\n----- Outliers Limits ----- \n Lower limit: " + lower + " and upper limit: " + upper + "\n----------------------")

      val upperOutliers = cmhm.filter(x => x._2 > upper).collect()
      val lowerOutliers = cmhm.filter(x => x._2 < lower).collect()

      pw.write("----- Outliers ----- \n " + upperOutliers.toList.union(lowerOutliers.toList) + "\n----------------------")

      pw.write("Mean: " + valuesStatistics.getMean())
      pw.write("Geometric Mean: " + valuesStatistics.getGeometricMean())
      pw.write("Max: " + valuesStatistics.getMax())
      pw.write("Min:" + valuesStatistics.getMin())
      if (cmhm.collect().size % 2 == 0) {

        val center1 = cmhm.sortBy(_._2).values.collect.apply(cmhm.collect().size / 2)
        val center2 = cmhm.sortBy(_._2).values.collect.apply((cmhm.collect().size / 2) + 1)
        val median = (center1 + center2) / 2
        pw.write("Median: " + median)

      } else {
        val position = ((cmhm.collect().size / 2) + 0.5).toInt
        val median = cmhm.sortBy(_._2).values.collect.apply(position)
        pw.write("Median: " + median)
      }

      pw.write("Variance :" + valuesStatistics.getVariance())
      pw.write("Population Variance :" + valuesStatistics.getPopulationVariance())
      pw.write("Standard Deviation: " + valuesStatistics.getStandardDeviation())
      pw.write("Kurtosis: " + valuesStatistics.getKurtosis()) // Concentration ( Normal value between : +- 0.5)
      pw.write("Skewness: " + valuesStatistics.getSkewness()) //Oblique - Symmetry Coefficient ( Normal value between : +- 0.5)

      val position1 = ((cmhm.collect().size * 5) / 100.0).round
      val position2 = ((cmhm.collect().size * 95) / 100.0).round


      val interval = cmhm.sortBy(_._2).values.collect().slice(position1.toInt, position2.toInt)
      val croppedAverage = interval.toList.sum / interval.size
      pw.write("Cropped mean: " + croppedAverage)

      val variationCoefficient = valuesStatistics.getStandardDeviation / valuesStatistics.getMean()
      pw.write("Variation Coefficient: " + variationCoefficient)


      pw.write("\n--------With Cropped Mean -----------")

      val variance2 = cmhm.values.map(a => math.pow(a - croppedAverage, 2)).sum() / cmhm.collect().size
      pw.write("Variance " + variance2)

      val standardDeviation2 = math.sqrt(variance2)
      pw.write("Standard Deviation: " + standardDeviation2)

      val variationCoefficient2 = standardDeviation2 / croppedAverage
      pw.write("Variation Coefficient: " + variationCoefficient2)

      pw.write("\n--------With Geometric Mean -----------")

      val variance3 = cmhm.values.map(a => math.pow(a - valuesStatistics.getGeometricMean, 2)).sum() / cmhm.collect().size
      pw.write("Variance " + variance3)

      val standardDeviation3 = math.sqrt(variance3)
      pw.write("Standard Deviation: " + standardDeviation3)

      val variationCoefficient3 = standardDeviation3 / croppedAverage
      pw.write("Variation Coefficient: " + variationCoefficient3)
*/
      pw.write("\n")
      pw.close()


    }//end for

  } //End Main method

} //End object
