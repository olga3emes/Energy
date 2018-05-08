import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Agrupamiento {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //System.setProperty("hadoop.home.dir", "c:\\Winutil\\")

    val conf = new SparkConf().setAppName("Agrupamiento").setMaster("local")
    val sc = new SparkContext(conf)


    val dates365 = sc.textFile("365.csv")
    val dates366 = sc.textFile("366.csv")

    val id = "48"

    val anyo = List("2011","2012","2013","2014","2015","2016","2017")

      val pw = new PrintWriter(new File("Edificio "+id+".csv"))

    for (a <- anyo) {

      val file = sc.textFile("Datos Luz/" + id + "/Edificio " + id + " " + a +
        ".csv") //Next

      pw.write(a + " Edificio " + id + ";Enero;Febrero;Marzo;Abril;Mayo;Junio;Julio;Agosto;Septiembre;Octubre;Noviembre;Diciembre\n");

      val data = file.map(line => {


        val Array(d, m, y, h, min, s, kwhour) = line.replaceAll(" |/|:", ";").split(";").map(_.trim)

        val kwh = kwhour.toDouble
        val year = y.toInt
        val month = m.toInt
        val day = d.toInt
        val hour = h.toInt
        val minute = min.toInt

        val date = m + "/" + d
        val time = h + ":" + min

        (id, kwh, year, month, day, hour, minute, date, time)

        //1,  2,    3.  4.      5     6     7     8       9
      })
      val year = data.collect().apply(1)._3
      //Take the year of first data register (is the same of whole file)


      //Leap-year?

      var leapYear = false
      if ((year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0))) {
        leapYear = true
      }

      //Formato consumo total

      /*     val cdia = data.map(line => (List(line._4, line._5), line._2)).reduceByKey(_ + _)

           var day = 1
           var month = 1
           var cadena = ""

           for (cd <- cdia.sortBy(c => (c._1(1), c._1(0))).collect().toList) {
             day = cd._1(1)
             if (cadena == "") {
               cadena = cadena + cd._1(1) + ";"
             }
             if (cd._1(0) != month) {
               while (month != cd._1(0)) {
                 cadena = cadena + ";"
                 month = month + 1
               }
             }
             if (month == 12) {
               cadena = cadena + cd._2 + "\n"
               println(cadena)
               month = 1
               cadena = ""
             } else {
               cadena = cadena + cd._2 + ";"
               month = month + 1
             }
           }
         }*/

      //Formato consumo cada 5 minutos

      val cdia = data.map(line => (List(line._4, line._5, line._6, line._7), line._2)).reduceByKey(_ + _)

      var day = 1
      var month = 1
      var hora = ""
      var cadena = ""

      for (cd <- cdia.sortBy(c => (c._1(1), c._1(2), c._1(3), c._1(0))).collect().toList) {
        day = cd._1(1)
        if (cadena == "" && hora == "" || hora == cd._1(2) + ":" + cd._1(3)) {
          if(cadena == "" && hora == "")
          cadena = cadena + cd._1(1) + " " + cd._1(2) + ":" + cd._1(3) + ";"

          if (cd._1(0) != month) {
            while (month != cd._1(0)) {
              cadena = cadena + ";"
              month = month + 1
            }
          }
          if (month == 12) {
            cadena = cadena + cd._2 + "\n"
            pw.write(cadena)
            month = 1
            cadena = ""
            hora = ""
          } else {
            cadena = cadena + cd._2 + ";"
            month = month + 1
            hora = cd._1(2) + ":" + cd._1(3)
          }

        } else {
          pw.write(cadena + "\n")
          month = 1
          cadena = ""
          hora = ""
        }
      }

    } //End for (per year)

    pw.close()
  }//End Main


} //End object