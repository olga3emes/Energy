import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Agrupamiento2 {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //System.setProperty("hadoop.home.dir", "c:\\Winutil\\")

    val conf = new SparkConf().setAppName("Agrupamiento2").setMaster("local")
    val sc = new SparkContext(conf)


    val dates365 = sc.textFile("365.csv")
    val dates366 = sc.textFile("366.csv")

    val id = List("1","2","3", "4","5","6","7","8","10","11","12","13","14","15","16","17","20",
    "21","22","23","24","25","26","27","31","32","41","44")

    val anyo = "2010"

    /*val id = List("1","2","3", "4","5","6","7","8","10","11","12","13","14","15","16","17","20",
      "21","22","23","24","25","26","27","31","32","37","41","42","44","45","47","48")

    val anyo = "2012"*/
    /*val id = List("1","2","3", "4","5","6","7","8","10","11","12","13","14","15","16","17","20",
      "21","22","23","24","25","26","27","31","32","37","38","41","42","44","45","47","48")

    val anyo = "2013"*/

   /* val id = List("1","2","3", "4","5","6","7","8","10","11","12","13","14","15","16","17","20",
      "21","22","23","24","25","26","27","31","32","37","38","41","42","44","45","47","48")

    val anyo = "2014"*/

    /*val id = List("1","2","3", "4","5","6","7","8","9","10","11","12","13","14","15","16","17","20",
      "22","23","24","25","26","27","31","32","37","38","41","42","44","48")

    val anyo = "2017"*/

    val pw = new PrintWriter(new File("Edificios "+anyo+".csv"))
    var nombre=""

    for ( i <- id) {

      nombre = nombre +" Edificio " + i.toInt + ";"
    }

    pw.write(anyo+";" + nombre + "\n")

    var datos = Map[List[Int],Double]()

    for (a <- id) {

      val file = sc.textFile("Datos Luz/" + a + "/Edificio " + a + " " + anyo + ".csv") //Next



      val data = file.map(line => {


        val Array(d, m, y, h, min, s, kwhour) = line.replaceAll(" |/|:", ";").split(";").map(_.trim)

        val kwh = kwhour.toDouble
        val year = y.toInt
        val month = m.toInt
        val day = d.toInt
        val hour = h.toInt
        val minute = min.toInt
        val aa = a.toInt
        val date = m + "/" + d
        val time = h + ":" + min

        (aa, kwh, year, month, day, hour, minute, date, time)

        //1,  2,    3.  4.      5     6     7     8       9
      })
      val year = data.collect().apply(1)._3
      //Take the year of first data register (is the same of whole file)


      //Leap-year?

      var leapYear = false
      if ((year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0))) {
        leapYear = true
      }

      //Formato consumo cada 5 minutos

      val cdia = data.map(line => (List(line._5,line._4, line._6, line._7,line._1), line._2)).reduceByKey(_ + _)
      for (cd <- cdia.collect().toList) {
        datos += cd
      }
      println("Edificio "+ a + " procesado")

    } //End for (per year)

    var day = 1
    var month = 1
    var hora = ""
    var cadena = ""
    var edificio = 0

    for (cd<- datos.toList.sortBy(c => (c._1(0),c._1(1),c._1(2),c._1(3),c._1(4)))){

      if(day != cd._1(0)){
        if(!cadena.isEmpty){
          pw.write(cadena+ "\n")
          println(cadena +"\n")
          cadena=""
          hora =""
          edificio=0
        }
      }
      if(hora != (cd._1(2) + ":" + cd._1(3))){
        if(!cadena.isEmpty){
          pw.write(cadena+ "\n")
          println(cadena +"\n")
          cadena=""
          hora =""
          edificio=0
        }
      }


      if (cadena == "" && hora == "" || hora == cd._1(2) + ":" + cd._1(3)) {
        if (cadena == "" && hora == "")
          cadena = cadena + cd._1(0) + "/" + cd._1(1) + " " + cd._1(2) + ":" + cd._1(3) + ";"


        var ind1= edificio
        var ind2= id.indexOf(cd._1(4).toString)
        while(ind1<ind2){
          cadena = cadena + ";"
          ind1= ind1+1
        }

        if (cd._1(4) == 44) {
            cadena = cadena + cd._2 + "\n"
            pw.write(cadena)
            println(cadena)
            edificio = 0
            cadena = ""
            hora = ""

        } else {
          cadena = cadena + cd._2 + ";"
          hora = cd._1(2) + ":" + cd._1(3)
          day = cd._1(0)
          edificio= id.indexOf(cd._1(4).toString)+1
        }
      }


      }




    pw.close()
  }//End Main


} //End object