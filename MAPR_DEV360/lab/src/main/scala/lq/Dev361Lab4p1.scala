package lq

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

import java.lang.Math

object Dev361Lab4p1 {

  case class sfpdClass(IncidntNum:Int, Category:String, Descript:String, DayOfWeek:String, Date:String, Time:String, PdDistrict:String, Resolution:String, Address:String, X:String, Y:String, PdId:String)

  def main(args: Array[String]) {

        val sc = new SparkContext(new SparkConf().setAppName("Dev361Lab4p1"))
//        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//        import sqlContext.implicits._



 	//Loading the data into RDD with split
	val inputRDD =sc.textFile("DEV360_LAB/sfpd.csv").map(_.split(","))
	// Mapping the inputRDD to the case class
	val sfpd = inputRDD.map(a=>sfpdClass(a(0).toInt,a(1),a(2),a(3),a(4),a(5),a(6),a(7),a(8),a(9),a(10),a(11)))

        println("First: " + sfpd.first)
        println("Five first:")
        sfpd.take(5).foreach(println)
        println("\nTotal: " + sfpd.count)
        println("Total Resolution:")
        sfpd.map(l => (l.Resolution,1) ).reduceByKey(_+_).foreach(println)

        println("\nList District:")
        sfpd.map(l => l.PdDistrict).distinct.foreach(println)

        println("\nIncident per District:")
        sfpd.map(l => (l.PdDistrict,1) ).reduceByKey(_+_).foreach(println)

        println("\n5 top district with highest incidents:")
        sfpd.map(l => (l.PdDistrict,1) ).reduceByKey(_+_).map(a => (a._2, a._1)).sortByKey(false).take(5).foreach(println)


        println("\n3 top category with highest incidents:")
        sfpd.map(l => (l.Category,1) ).reduceByKey(_+_).map(a => (a._2, a._1)).sortByKey(false).take(3).foreach(println)

        println("\n Number of incidents per district:")
        sfpd.map(l => (l.PdDistrict,1) ).reduceByKey(_+_).foreach(println)


    System.out.println("All Done")


  }
}
