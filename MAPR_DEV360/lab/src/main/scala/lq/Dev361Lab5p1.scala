package lq

import java.lang._ 
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.TypeTag._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

import java.lang.Math




object Dev361Lab5p1 {

def getyear(s:String):String = {
   val year = s.substring(s.lastIndexOf('/')+1)
   year
}

  case class sfpdClass(IncidntNum:Int, Category:String, Descript:String, DayOfWeek:String, Date:String, Time:String, PdDistrict:String, Resolution:String, Address:String, X:String, Y:String, PdId:String)

  def main(args: Array[String]) {

        val sc = new SparkContext(new SparkConf().setAppName("Dev361Lab5p1"))
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._


 	//Loading the data into RDD with split
	val inputRDD =sc.textFile("DEV360_LAB/sfpd.csv").map(_.split(","))
	// Mapping the inputRDD to the case class
	val sfpd = inputRDD.map(a=>sfpdClass(a(0).toInt,a(1),a(2),a(3),a(4),a(5),a(6),a(7),a(8),a(9),a(10),a(11)))

        val sfpdDF = sfpd.toDF
        sfpdDF.registerTempTable("sfpdDF")

        //sqlContext.sql("SELECT <column>, count(incidentnum) AS inccount FROM <DataFrame Table> GROUP BY <column> ORDER BY inccount <SORT ORDER> LIMIT <number of records>")


        sfpd.map(a => (a.PdDistrict,1)).reduceByKey(_+_).map(a => (a._2,a._1) ).sortByKey(false).take(5).foreach(println)
        sfpdDF.groupBy("PdDistrict").count.sort($"count".desc).show(5)
        sqlContext.sql("SELECT PdDistrict, count(IncidntNum) AS inccount FROM sfpdDF GROUP BY PdDistrict ORDER BY inccount DESC LIMIT 5").foreach(println)

        println("\nTop 10 resolution:")
        sfpdDF.groupBy("Resolution").count.sort($"count".desc).show(10)
        sqlContext.sql("SELECT Resolution, count(IncidntNum) AS inccount FROM sfpdDF GROUP BY Resolution ORDER BY inccount DESC LIMIT 10").foreach(println)
        //sqlContext.sql("SELECT Resolution, count(IncidntNum) AS inccount FROM sfpdDF GROUP BY Resolution ORDER BY inccount DESC LIMIT 10").toJSON.saveAsTextFile("/user/mapr/output/Dev361Lap5p1_top10Res.json")

        println("\nTop 3 category:")
        sfpdDF.groupBy("Category").count.sort($"count".desc).show(3)
        sqlContext.sql("SELECT Category, count(IncidntNum) AS inccount FROM sfpdDF GROUP BY Category ORDER BY inccount DESC LIMIT 3").foreach(println)


        //sfpdDF.groupBy(getyear($"Date".toString())).count.show(10)  //DOES NOT WORK FIXME

        sqlContext.udf.register("getyear", getyear _) //(s:String) => s.substring(s.lastIndexOf('/')+1) )
        sqlContext.sql("SELECT getyear(Date), count(IncidntNum) AS inccount FROM sfpdDF GROUP BY getyear(Date) ORDER BY inccount DESC LIMIT 10").foreach(println)

        sqlContext.sql("SELECT Category, Address, Resolution FROM sfpdDF WHERE getyear(Date)='14'").foreach(println)

        sqlContext.sql("SELECT Address, Resolution FROM sfpdDF WHERE getyear(Date)='15' and Category='VANDALISM'").foreach(println)


    System.out.println("All Done")


  }
}
