import java.io.{File, PrintWriter}

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Preprocess {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //System.setProperty("hadoop.home.dir", "c:\\Winutil\\")

    val conf = new SparkConf().setAppName("Preprocess").setMaster("local")
    val sc = new SparkContext(conf)


    val dates365 = sc.textFile("365.csv")
    val dates366 = sc.textFile("366.csv")

    val id = "14"


    val a = 2012

    val file = sc.textFile("Datos Luz/" + id + "/Edificio " + id + " " + a + ".csv") //Next
   // val file = sc.textFile("Datos Luz/" + id + "/prueba.csv") //Next


    //val pw = new PrintWriter(new File("Nuevo " + id + " " + a + ".txt"))


    println("------------- Edificio " + id + "---------------\n");

    println(a + "\n")

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
    })
    val year = data.collect().apply(1)._3

    var leapYear = false
    if ((year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0))) {
      leapYear = true
    }

    val datetime = data.map(line => (line._8, line._9)).groupByKey().sortBy(_._1)

    println("Días Ausentes")

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

    val consumoBusca0 = data.map(line => (line._8 + " - " + line._9, line._2)).reduceByKey(_ + _).sortBy(_._1)

    val consumo0 = consumoBusca0.collect().toList.filter(s => s._2 == 0)
    val consumoNeg = consumoBusca0.collect().toList.filter(s => s._2 < 0)

    println(consumo0.length.toString()+ " Values cero\n")
    println(consumoNeg.length.toString()+ " Values negativos\n")


    var avaibleHours15 = List("00:00", "00:15", "00:30", "00:45", "01:00", "01:15", "01:30", "01:45", "02:00", "02:15", "02:30", "02:45", "03:00", "03:15", "03:30", "03:45", "04:00", "04:15", "04:30", "04:45", "05:00", "05:15", "05:30", "05:45", "06:00", "06:15", "06:30", "06:45", "07:00", "07:15", "07:30", "07:45", "08:00", "08:15", "08:30", "08:45", "09:00", "09:15", "09:30", "09:45", "10:00", "10:15", "10:30", "10:45", "11:00", "11:15", "11:30", "11:45", "12:00", "12:15", "12:30", "12:45", "13:00", "13:15", "13:30", "13:45", "14:00", "14:15", "14:30", "14:45", "15:00", "15:15", "15:30", "15:45", "16:00", "16:15", "16:30", "16:45", "17:00", "17:15", "17:30", "17:45", "18:00", "18:15", "18:30", "18:45", "19:00", "19:15", "19:30", "19:45", "20:00", "20:15", "20:30", "20:45", "21:00", "21:15", "21:30", "21:45", "22:00", "22:15", "22:30", "22:45", "23:00", "23:15", "23:30", "23:45")
    var avaibleHours5 = List("00:00", "00:05", "00:10", "00:15", "00:20", "00:25", "00:30", "00:35", "00:40", "00:45", "00:50", "00:55",
      "01:00", "01:05", "01:10", "01:15", "01:20", "01:25", "01:30", "01:35", "01:40", "01:45", "01:50", "01:55",
      "02:00", "02:05", "02:10", "02:15", "02:20", "02:25", "02:30", "02:35", "02:40", "02:45", "02:50", "02:55",
      "03:00", "03:05", "03:10", "03:15", "03:20", "03:25", "03:30", "03:35", "03:40", "03:45", "03:50", "03:55",
      "04:00", "04:05", "04:10", "04:15", "04:20", "04:25", "04:30", "04:35", "04:40", "04:45", "04:50", "04:55",
      "05:00", "05:05", "05:10", "05:15", "05:20", "05:25", "05:30", "05:35", "05:40", "05:45", "05:50", "05:55",
      "06:00", "06:05", "06:10", "06:15", "06:20", "06:25", "06:30", "06:35", "06:40", "06:45", "06:50", "06:55",
      "07:00", "07:05", "07:10", "07:15", "07:20", "07:25", "07:30", "07:35", "07:40", "07:45", "07:50", "07:55",
      "08:00", "08:05", "08:10", "08:15", "08:20", "08:25", "08:30", "08:35", "08:40", "08:45", "08:50", "08:55",
      "09:00", "09:05", "09:10", "09:15", "09:20", "09:25", "09:30", "09:35", "09:40", "09:45", "09:50", "09:55",
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


    var cdia = data.map(line => (List(line._8, line._9), line._2)).reduceByKey(_ + _).sortBy(c => (c._1(0), c._1(1))).collect()

    var consumo = data.map(line => (List(line._8, line._9), line._2)).reduceByKey(_ + _).sortBy(c => (c._1(0), c._1(1))).collect()
    var consumoDia = data.map(line => (line._8, line._2)).reduceByKey(_ + _).sortBy(_._1).collect()


    var day = ""
    var contador = 0
    var contadorTotal = 0

    for (cd <- consumoDia) {
      if (day == "") {
        day = cd._1
        var actual = consumo.filter(s => s._1(0) == day)
        val tam = actual.size
          if (tam < 288) {
            var horas= new ListBuffer[String]
            var indices= new ListBuffer[Int]
            for (act <- actual) {
              horas+=act._1(1)
            } //end for actual
            var horasFaltantes=avaibleHours5.diff(horas)
            for (hf <-horasFaltantes){
              indices+=avaibleHours5.indexOf(hf)
            }
            var conjunto = new ListBuffer[ListBuffer[Int]]
            var siguiente = 0
            var lista = new ListBuffer[Int]
            for(i <- indices){
              if(siguiente!=0){
                if(i!=siguiente) {
                  conjunto += lista
                  lista = new ListBuffer[Int]
                }
                  lista += i
                  siguiente = i + 1
                }else{
                lista+=i
                siguiente= i+1
              }
              if(indices.last == i){
                if(!conjunto.contains(lista))
                  conjunto+=lista
              }
            }
              if(!consumo.find(s=>s._1(0)==day).isEmpty) {
                for(c <- conjunto){

                if (c.size < 96 && !(c.size >= 48 &&
                  (c.contains(avaibleHours5.indexOf("12:00")) || c.contains(avaibleHours5.indexOf("16:00")) || c.contains(avaibleHours5.indexOf("20:00")))
                  )) { //Condicion Dia - Noche

                  if (!c.contains((avaibleHours5.indexOf("23:55")))) {

                    val prox = avaibleHours5(c(c.size - 1) + 1)
                    var acumulado = actual.find(s => s._1(1) == prox)


                    var valuesStatistics = new DescriptiveStatistics()
                    actual.toMap.values.foreach(v => valuesStatistics.addValue(v))
                    var media = valuesStatistics.getGeometricMean
                    var desvtip = valuesStatistics.getStandardDeviation
                    var rmax= (media+desvtip)*(c.size +1)
                    var rmin= (media-desvtip)*(c.size +1)

                    var valor = acumulado.toList(0)._2/(c.size+1)

                    if(valor >= rmin && valor <= rmax){
                      for(cc<- c) {
                       consumo=consumo.+:(List(day,avaibleHours5(cc)),valor)
                      }
                      consumo=consumo.filter(s=>s._1!=List(day,prox))
                      consumo=consumo.+:(List(day,prox),valor).sortBy(c => (c._1(0), c._1(1)))
                    }else{
                      var indexedSeq= IndexedSeq(Seq(0.0,0.0))
                      for(act<-actual){
                        val i =avaibleHours5.indexOf(act._1(1))
                        indexedSeq=indexedSeq.+:(Seq(i,act._2))
                      }
                      var rl= RegresionLineal.linear(indexedSeq)
                      for(cc<- c) {
                        var y= BigDecimal(rl._1 *cc + rl._2).setScale(3,BigDecimal.RoundingMode.HALF_UP).toDouble
                        consumo=consumo.+:(List(day,avaibleHours5(cc)),y)
                      }
                    }

                  } else {
                    var indexedSeq= IndexedSeq(Seq(0.0,0.0))
                    for(act<-actual){
                      val i =avaibleHours5.indexOf(act._1(1))
                      indexedSeq=indexedSeq.+:(Seq(i,act._2))
                    }
                    var rl= RegresionLineal.linear(indexedSeq)
                    for(cc<- c) {
                      var y= BigDecimal(rl._1 *cc + rl._2).setScale(3,BigDecimal.RoundingMode.HALF_UP).toDouble
                      consumo=consumo.+:(List(day,avaibleHours5(cc)),y)
                    }
                  }
                  contador = 0

                } else {
                  consumo = consumo.filter(s => s._1(0) != day)
                  cdia=cdia.filter(s=>s._1 !=day)
                  println("eliminar" + day)
                  contador = 0
                }
              }
              }
            day = ""
          } else {
            day = ""
          }
        }

    }//end for

    var pw = new PrintWriter(new File("Edificio "+id+" "+ year+" .csv"))

    var total= 0.0
    for(cdia <- consumoDia){
      var con = consumo.filter(s=>s._1(0)==cdia._1)
      total = BigDecimal(con.toMap.values.sum).setScale(3,BigDecimal.RoundingMode.HALF_UP).toDouble
      val mes = cdia._1.split("/")(0)
      val dia = cdia._1.split("/")(1)
      if(total!=0.0)
      pw.write(dia +"/"+mes+"/"+year+";"+total+"\n")

    }
    pw.close()
  }
}










