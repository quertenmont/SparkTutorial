package lq

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

import java.lang.Math

object Dev360Lab2p2 {
  case class Auctions(auctionid:String, bid:Float, bidtime:Float, bidder:String,bidrate:Int,openbid:Float,price:Float,itemtype:String,daystolive:Int)
//  case class Auctions(aucid:String, bid:Float,bidtime:Float,bidder:String,bidrate:Int,openbid:Float, price:Float,itemtype:String,dtl:Int)

  def main(args: Array[String]) {

        val sc = new SparkContext(new SparkConf().setAppName("Dev360Lab2p2"))
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._


 	//Loading the data into RDD with split
	val inputRDD =sc.textFile("DEV360_LAB/auctiondata.csv").map(_.split(","))
	// Mapping the inputRDD to the case class
	val auctionsRDD = inputRDD.map(a=>Auctions(a(0),a(1).toFloat,a(2).toFloat,a(3),a(4).toInt, (5).toFloat,a(6).toFloat,a(7),a(8).toInt))

	// converting auctionsRDD to a DataFrame
	val auctionsDF = auctionsRDD.toDF()

	//Registering the auctionsDF as a temporary table with the same name
	auctionsDF.registerTempTable("auctionsDF")

	//8. Check the data in the DataFrame
	auctionsDF.show
        auctionsDF.printSchema()

        println("Total Counts = " + auctionsDF.count())
        println("auction Counts  = " + auctionsDF.select("auctionid").distinct.count())
        println("Item Counts  = " + auctionsDF.select("itemtype").distinct.count())
        println("Bids/Items: ")
        auctionsDF.groupBy("auctionid","itemtype").count().show()

        auctionsDF.groupBy("auctionid","itemtype").count.agg(min("count"), avg("count"), max("count")).show
        auctionsDF.groupBy("auctionid","itemtype").agg(min("bid"), avg("bid"), max("bid")).show

        println("#Auctions with final price > 200: " + auctionsDF.filter(auctionsDF("price")>200).count )

        val xboxes = sqlContext.sql("SELECT auctionid,itemtype,bid,price,openbid FROM auctionsDF WHERE itemtype='xbox'")
        xboxes.describe("price").show

    //////////////////start of part2

/* 
    val auctionMadBids = auctionBids.map(x => x._2).reduce((x,y) => Math.max(x,y))
    println(auctionMadBids)

    val auctionMinBids = auctionBids.map(x => x._2).reduce((x,y) => Math.min(x,y))
    println(auctionMinBids)

    val auctionMeanBids = auctionBids.map(x => x._2).reduce((x,y) =>x+y)
    println(auctionMeanBids/auctionBids.count())
*/

    System.out.println("All Done")


  }
}
