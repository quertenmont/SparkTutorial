package lq

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.lang.Math

object Dev360Lab2p1 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Dev360Lab2p1"))

    val auctionid = 0
    val bid = 1
    val bidtime = 2
    val bidder = 3
    val bidderrate = 4
    val openbid = 5
    val price = 6
    val itemtype = 7
    val daystolive = 8

 
    // split each document into words
    val auctionRDD= sc.textFile("DEV360_LAB/auctiondata.csv").map(l => l.split(',')).cache()    

    val auctionCount = auctionRDD.map(l => l(auctionid) ).distinct().count()
    val itemCount = auctionRDD.map(l => l(itemtype) ).distinct().count()
    val bidsPerItem = auctionRDD.map(l => (l(itemtype),1) ).reduceByKey( (x,y) => x+y).collect()

    println("test")
    println("Total Counts = " + auctionRDD.count())
    println("auction Counts  = " + auctionCount)
    println("Item Counts  = " + itemCount)
    println("Bids/Items  = "  )
    bidsPerItem.foreach(print)
    println("")

    //////////////////start of part2

    val auctionBids = auctionRDD.map(l => (l(auctionid),1) ).reduceByKey(_+_)
    auctionBids.take(5).foreach( println )

 
    val auctionMadBids = auctionBids.map(x => x._2).reduce((x,y) => Math.max(x,y))
    println(auctionMadBids)

    val auctionMinBids = auctionBids.map(x => x._2).reduce((x,y) => Math.min(x,y))
    println(auctionMinBids)

    val auctionMeanBids = auctionBids.map(x => x._2).reduce((x,y) =>x+y)
    println(auctionMeanBids/auctionBids.count())


    System.out.println("All Done")


  }
}
