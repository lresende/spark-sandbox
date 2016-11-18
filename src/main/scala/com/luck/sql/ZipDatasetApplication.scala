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
import org.apache.spark.sql.types.StructType

// {
//   "_id" : "91711",
//   "city" : "CLAREMONT",
//   "loc" : [ -117.718293, 34.109167 ],
//   "pop" : 34096,
//   "state" : "CA"
// }
case class Zip(_id: String, city: String, loc: Seq[Double], pop: Long, state: String )

/**
  * Sample ingestion application of data set in json format
  * using Data Frame and Dataset APIs
  */
object ZipDatasetApplication {

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

    import spark.implicits._

    val zipSchema = new StructType()
      .add("_id", "string")
      .add("city", "string")
      .add("pop", "long")
      .add("state", "string")

    val zipsDF = spark.read
      .schema(zipSchema)
      .format("json")
      .load("/users/lresende/zips.json")

    zipsDF.printSchema()
    zipsDF.show()
  }
}
