import java.util.{Calendar, GregorianCalendar}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by olga on 2/6/17.
  */
object Edificio_4 {


  def weekendsOfYear(year: Int) : List[String] = {
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


  def weekdayOfYear(year: Int, dayOfWeek: Int) : List[String] = {
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
        var calendarDayOfWeek : Int = 0

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


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //System.setProperty("hadoop.home.dir", "c:\\Winutil\\")

    val conf = new SparkConf().setAppName("Edificio_4").setMaster("local")
    val sc = new SparkContext(conf)

    /* ESTRUCTRA DEL CSV:
    ---------------------------
    1. Identificador del edificio.
    2. Consumo en kWh (kilovatio-hora).
    3. Año.
    4. Número de mes (del 1 al 12).
    5. Día del mes.
    6. Hora (de 0 a 23).
    7. Minutos (0, 15, 30 o 45). (Se recogen 4 datos por hora)
    ----------------------------- */

    val dates365 = sc.textFile("365.csv")
    val dates366 = sc.textFile("366.csv")

    val file11 = sc.textFile("E20/consumo_E20.KWH_2011.csv")
    val file12 = sc.textFile("E20/consumo_E20.KWH_2012.csv")
    val file13 = sc.textFile("E20/consumo_E20.KWH_2013.csv")
    val file14 = sc.textFile("E20/consumo_E20.KWH_2014.csv")
    val file15 = sc.textFile("E20/consumo_E20.KWH_2015.csv")

    val data = file12.map(line => {

      val Array(id, kwhour, y, m, d, h, min) = line.split(";").map(_.trim)

      val kwh = kwhour.toDouble
      val year = y.toInt
      val month = m.toInt
      val day = d.toInt
      val hour = h.toInt
      val minute = min.toInt

      val date = m + "/" + d
      val time = h + ":" + min

      (id, kwh, year, month, day, hour, minute, date, time)
      // 1    2    3     4      5    6      7      8     9
    })
    val year = data.collect().apply(1)._3
    //Recoge el año del primer registro de data (es el mismo para el fichero al completo)

    val datetime = data.map(line => (line._8, line._9)).groupByKey().sortBy(_._1)

    //Comprobar que no faltan días

    def missingDays(year: Int, mapDateTime: RDD[(String, Iterable[String])]): List[String] = {
      if ((year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0))) {
        println((366 - (mapDateTime.collect().length)) + " days missing")
        val list1 = dates366.collect().toList
        val list2 = mapDateTime.keys.collect().toList
        val list = list1.diff(list2)
        list.foreach(println)
        return list
      } else {
        println((365 - (mapDateTime.collect().length)) + " days missing")
        val list1 = dates365.collect().toList
        val list2 = mapDateTime.keys.collect().toList
        val list = list1.diff(list2)
        println(list)
        return list
      }
    }

    //Lista de dias del año que faltan

    val mDays = missingDays(year, datetime)


    //Comprobar que no faltan horas

    val avaibleHours = List("00:00", "00:15", "00:30", "00:45", "01:00", "01:15", "01:30", "01:45", "02:00", "02:15", "02:30", "02:45", "03:00", "03:15", "03:30", "03:45", "04:00", "04:15", "04:30", "04:45", "05:00", "05:15", "05:30", "05:45", "06:00", "06:15", "06:30", "06:45", "07:00", "07:15", "07:30", "07:45", "08:00", "08:15", "08:30", "08:45", "09:00", "09:15", "09:30", "09:45", "10:00", "10:15", "10:30", "10:45", "11:00", "11:15", "11:30", "11:45", "12:00", "12:15", "12:30", "12:45", "13:00", "13:15", "13:30", "13:45", "14:00", "14:15", "14:30", "14:45", "15:00", "15:15", "15:30", "15:45", "16:00", "16:15", "16:30", "16:45", "17:00", "17:15", "17:30", "17:45", "18:00", "18:15", "18:30", "18:45", "19:00", "19:15", "19:30", "19:45", "20:00", "20:15", "20:30", "20:45", "21:00", "21:15", "21:30", "21:45", "22:00", "22:15", "22:30", "22:45", "23:00", "23:15", "23:30", "23:45")

    println("Days and their missing hours")

    val daysMissing = datetime.map(line => (line._1, avaibleHours.diff(line._2.toList))).sortBy(_._1)

    println(daysMissing.filter(_._2.nonEmpty).collect().toList)

    println("Day consumption")

    val consumoDia = data.map(line => (line._8, line._2)).reduceByKey(_ + _).sortBy(_._1)

    println(consumoDia.collect().toList)

    println("Day consumption per hour")

    val consumoPorHora = data.map(line => (line._8 + " - " + line._6, line._2)).reduceByKey(_ + _).sortBy(_._1)

    println((consumoPorHora.collect().toList))

    println("Mean consumption per hour and number of registries")

    val consumoMedioPorHora = data.map(line => (line._8 + " - " + line._6, line._2)).groupByKey().sortBy(_._1)

    val consumoMedioPorHoraRegistros = consumoMedioPorHora.map(line => (line._1, line._2.sum / line._2.size)).sortBy(_._1)

    println(consumoMedioPorHoraRegistros.collect().toList)

    println("Month consumption")

    val consumoMes = data.map(line => (line._4, line._2)).reduceByKey(_ + _).sortBy(_._1)

    println(consumoMes.collect().toList)

    println("Year consumption")

    val consumoAño = consumoMes.map(line => (line._2)).reduce(_ + _)

    println(consumoMes.map(line => (line._2)).reduce(_ + _))

    println("Mean consumption per month")

    println(consumoAño / 12)

    println("Weekends of year and consumption")

    val weekends = weekendsOfYear(year)

    val weekendsConsumption = consumoDia.collect().toMap.filterKeys(k=>weekends.contains(k))

    println(weekendsConsumption.toList.sorted)

    println("Mean consumption of weekends")

    println(weekendsConsumption.values.sum / weekendsConsumption.size)

    println("Working days and consumption")

    val workingDays = consumoDia.keys.collect().diff(weekends)

    val workingDaysConsumption = consumoDia.collect().toMap.filterKeys(k=>workingDays.contains(k))

    println(workingDaysConsumption.toList.sorted)

    println("Mean consumption of weekends")

    println(workingDaysConsumption.values.sum / workingDaysConsumption.size)

    var i = 0
    for (i <- 1 to 7) {
      var nameOfDay : String = ""
     i match {
       case 1 => nameOfDay = "Mondays"
       case 2 => nameOfDay = "Tuesdays"
       case 3 => nameOfDay = "Wednesdays"
       case 4 => nameOfDay = "Thursdays"
       case 5 => nameOfDay = "Fridays"
       case 6 => nameOfDay = "Saturdays"
       case 7 => nameOfDay = "Sundays"
     }

      println("Consumption on " + nameOfDay)

      val weekdays = weekdayOfYear(year, i)

      val consumption = consumoDia.collect().toMap.filterKeys(k => weekdays.contains(k))

      println(consumption.toList.sorted)

      println("Mean consumption on " + nameOfDay)

      println(consumption.values.sum / consumption.size)
    }








  } //End Main method

}

//End object
