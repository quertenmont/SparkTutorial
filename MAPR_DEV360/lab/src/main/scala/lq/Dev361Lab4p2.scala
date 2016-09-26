package lq

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

import java.lang.Math

object Dev361Lab4p2 {

  def main(args: Array[String]) {

        val sc = new SparkContext(new SparkConf().setAppName("Dev361Lab4p2"))
//        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//        import sqlContext.implicits._



//        sc.textFile("DEV360_LAB/J_AddCat.csv").map(_.split(",")).take(5).foreach(println)
//        return

 	//Loading the data into RDD with split
	val catRDD =sc.textFile("DEV360_LAB/J_AddCat.csv").map(_.split(",")).map(a=>(a(1),a(0)) ) 
	val distRDD =sc.textFile("DEV360_LAB/J_AddDist.csv").map(_.split(",")).map(a=>(a(1),a(0)) )
        println(catRDD.first())
        println(distRDD.first())

        
        val unionRDD = catRDD.groupWith(distRDD).foreach(println)

        println("")
        println("Counts cat: " + catRDD.count )
        println("Counts dist: " + distRDD.count )
        println("Counts join: " + catRDD.join(distRDD).count )
        println("Counts joinR: " + catRDD.rightOuterJoin(distRDD).count )
        println("Counts joinL: " + catRDD.leftOuterJoin(distRDD).count )
        println("Counts union: " + catRDD.groupWith(distRDD).count )

        


    System.out.println("All Done")


  }
}
