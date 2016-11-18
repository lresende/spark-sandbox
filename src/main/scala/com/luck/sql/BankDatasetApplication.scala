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
package com.luck.sql

import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import com.luck.utils.Timer

// "age";       --> 30
// "job";       --> "unemployed"
// "marital";   --> "married"
// "education"; --> "primary"
// "default";   --> "no"
// "balance";   --> 1787
// "housing";   --> "no"
// "loan";      --> "no"
// "contact";   --> "cellular"
// "day";       --> 19
// "month";     --> "oct"
// "duration";  --> 79
// "campaign";  --> 1
// "pdays";     --> -1
// "previous";  --> 0
// "poutcome";  --> "unknown"
// "y"          --> "no"

case class Bank(age: Long, job: String, marital: String, education: String, balance: Long)

/**
  * Sample ingestion application of data set in csv format
  * using Dataset APIs
  */
object BankDatasetApplication {

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]) {

    println("Starting Dataset Application - Bank") //scalastyle:ignore

    var sparkConf: SparkConf = new SparkConf()
      .setAppName("Spark-Bank-Dataset-API")

    // check Spark configuration for master URL, set it to local if not configured
    if (! sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val sparkSession: SparkSession = SparkSession.builder
      .config(sparkConf)
      .getOrCreate

    import sparkSession.implicits._

    val banks = sparkSession.read.format("csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", ";")
      .option("nullValue", "")
      .load("/users/lresende/bank.csv")
      .as[Bank]

    banks.show()
  }
}
