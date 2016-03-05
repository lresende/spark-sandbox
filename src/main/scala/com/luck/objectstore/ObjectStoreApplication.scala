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
package com.luck.objectstore

import org.apache.spark.{SparkContext, SparkConf}

object ObjectStoreApplication {
  def main(args: Array[String]): Unit = {
    println("Starting Streaming StreamingApplication") //scalastyle:ignore

    val sparkConf = new SparkConf().setAppName("Spark-Streaming-Application")
    val sparkContext = new SparkContext(sparkConf)

    val data = Array(1, 2, 3, 4, 5, 6, 7, 8)
    val distData = sparkContext.parallelize(data)
    distData.saveAsTextFile("swift2d://parquet.Object-Storage-lresende/one1.txt")
  }
}
