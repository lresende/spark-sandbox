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
package com.luck.csv

import org.apache.spark._
import org.apache.spark.sql.SparkSession

/**
  * Sample application that reads a CSV
  * and display its contents
  */

case class SomeData(id: Integer, timestamp: String)

object CsvApplication {

  def main(args: Array[String]): Unit = {

    println("Starting CSV Application") //scalastyle:ignore

    var sparkConf: SparkConf = new SparkConf()
      .setAppName("Spark-CSV")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // check Spark configuration for master URL, set it to local if not configured
    if (! sparkConf.contains("spark.master")) {
      println(">>> will set master") //scalastyle:ignore
      sparkConf.setMaster("local[2]")
    }

    val sparkSession: SparkSession = SparkSession.builder
      .config(sparkConf)
      .getOrCreate

    import sparkSession.implicits._

    val df = sparkSession.read
                       .option("header", "true") // Use first line of all files as header
                       .option("inferSchema", "true") // Automatically infer data types
                       .option("delimiter", " ") // define the delimiter to use
                       .csv("/users/lresende/data.csv") // location relative to hdfs root
                       .as[SomeData]  // this will return DataSet[SomeData]
    df.printSchema()

    df.show(50, false)
  }
}
