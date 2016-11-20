/*
 * Copyright (c) 2015-2016 Luciano Resende
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
package com.luck.hdfs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

// {
//   "_id" : "91711",
//   "city" : "CLAREMONT",
//   "loc" : [ -117.718293, 34.109167 ],
//   "pop" : 34096,
//   "state" : "CA"
// }

/**
  * Sample ingestion application of data set in json format
  * using Data Frame and Dataset APIs
  */
object WebhdfsApplication {

  def main(args: Array[String]) {

    println("Starting Dataset Application - ZIP") //scalastyle:ignore

    var sparkConf: SparkConf = new SparkConf()
      .setAppName("Spark-Zip-Dataset-API")

    // check Spark configuration for master URL, set it to local if not configured
    if (! sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val spark: SparkSession = SparkSession.builder
      .config(sparkConf)
      .getOrCreate

    val zipSchema = new StructType()
      .add("_id", "string")
      .add("city", "string")
      .add("pop", "long")
      .add("state", "string")

    val zips = spark.read
      .schema(zipSchema)
      .format("json")
      .load("webhdfs://localhost:50070/users/lresende/zips.json")

    zips.printSchema()
    zips.show()
  }
}
