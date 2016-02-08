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
import org.apache.spark.sql.SQLContext


/**
  * Sample application that reads a CSV
  * and display its contents
  */

object CsvApplication {

  def main(args: Array[String]): Unit = {

    println("Starting CSV Application")

    val sparkConf = new SparkConf()
                        .setAppName("Spark-CSV")
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val df = sqlContext.read.format("com.databricks.spark.csv")
                       .option("header", "false") // Use first line of all files as header
                       .option("inferSchema", "false") // Automatically infer data types
                       .option("delimiter", " ")
                       .load("hdfs://localhost:9000/users/lresende/data.csv")
                       .show(50, false)
  }

}
