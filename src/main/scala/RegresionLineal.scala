import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object RegresionLineal {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //System.setProperty("hadoop.home.dir", "c:\\Winutil\\")

    val conf = new SparkConf().setAppName("RegresionLineal").setMaster("local")
    val sc = new SparkContext(conf)

  }
}
