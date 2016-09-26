/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkWordCount {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("SparkWordCount"))
 
    // split each document into words
    val file= sc.textFile(args(0))

    val map = file.flatMap(line => line.split(" "))
    val pairs = map.map(word => (word, 1))
    val counts = pairs.reduceByKey(_ + _)

//    val counts = file.flatMap(line => line.split(" "))
//                 .map(word => (word, 1))
//                 .reduceByKey(_ + _)



    System.out.println("All Done")

    counts.collect()
    System.out.println("Pairs:"  + pairs.take(5).toString() )
    System.out.println("Map:" + map.take(5).toString() )
    System.out.println("Counts:" + counts.take(5).toString() )

    counts.saveAsTextFile(args(1))
 
  }
}
