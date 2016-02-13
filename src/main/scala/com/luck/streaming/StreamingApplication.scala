/*
 * Copyright (c) 2015 Luciano Resende
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.luck.streaming

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._


object StreamingApplication {
  def main(args: Array[String]): Unit = {

    println("Starting Streaming StreamingApplication") //scalastyle:ignore

    val sparkConf = new SparkConf().setAppName("Spark-Streaming-Application")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val streamingContext = new StreamingContext(sparkContext, Seconds(1))

    println("Will start processing the stream") //scalastyle:ignore

    val socketStream = streamingContext.socketTextStream("localhost", 9999)

    println("Will start processing each record") //scalastyle:ignore

    socketStream.foreachRDD { rdd =>

      rdd.foreach(record => println("### " + record + " ###")) //scalastyle:ignore

      if (! rdd.isEmpty()) {
        println("Processing RDD -> ") //scalastyle:ignore


        val df = sqlContext.read.json(rdd)

        df.printSchema()

        df.groupBy("symbol").max().show()

        df.groupBy("symbol").min().show()
      }
    }

    streamingContext.start();
    streamingContext.awaitTermination();

    println("Ending Streaming StreamingApplication") //scalastyle:ignore
  }

}
