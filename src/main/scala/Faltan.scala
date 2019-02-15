import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Faltan {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //System.setProperty("hadoop.home.dir", "c:\\Winutil\\")

    val conf = new SparkConf().setAppName("faltan").setMaster("local")
    val sc = new SparkContext(conf)


    val dates365 = sc.textFile("365.csv")
    val dates366 = sc.textFile("366.csv")

    val id = "14"

    val anyo = List("2012")


    for (a <- anyo) {

      val file = sc.textFile("Edificio " + id + " " + a + " .csv") //Next


      println("------------- Edificio " + id + "---------------\n");

      println(a + "\n")

      val data = file.map(line => {


        val Array(d, m, y, kwhour) = line.replaceAll(" |/|:", ";").split(";").map(_.trim)

        val kwh = kwhour.toDouble
        val year = y.toInt
        val month = m.toInt
        val day = d.toInt

        val date = m + "/" + d


        (id, kwh, year, month, day, date)
      })
      val year = data.collect().apply(1)._3
      //Take the year of first data register (is the same of whole file)


      //Leap-year?

      var leapYear = false
      if ((year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0))) {
        leapYear = true
      }
      val datetime = data.map(line => (line._6,line._1)).groupByKey().sortBy(_._1)

      //Test if we have missing days

      println("\n----------MISSING VALUES-----------\n")

      def missingDays(year: Int, leapYear: Boolean, mapDateTime: RDD[(String, Iterable[String])]): List[String] = {
        if (leapYear) {
          println((366 - (mapDateTime.collect().length)) + " days missing\n")
          val list1 = dates366.collect().toList
          val list2 = mapDateTime.keys.collect().toList
          val list = list1.diff(list2)
          list.foreach(println)
          return list
        } else {
          println((365 - (mapDateTime.collect().length)) + " days missing\n")
          val list1 = dates365.collect().toList
          val list2 = mapDateTime.keys.collect().toList
          val list = list1.diff(list2)
          println(list.toString())
          return list
        }
      }


      val mDays = missingDays(year, leapYear, datetime)

    }
  }
}

