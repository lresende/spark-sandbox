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

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]) {

    println("Starting Dataset Application - ZIP") //scalastyle:ignore

    var sparkConf: SparkConf = new SparkConf()
      .setAppName("Spark-Zip-Dataset-API")

    // check Spark configuration for master URL, set it to local if not configured
    if (! sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val sparkSession: SparkSession = SparkSession.builder
      .config(sparkConf)
      .getOrCreate

    import sparkSession.implicits._

    for(i <- 1 to 10) {
      val zipsDF = sparkSession.read.json("hdfs://localhost:9000/users/lresende/zips.json")
      // zipsDF.unpersist(true)
      // sqlContext.clearCache()
      println("Zips are:" + manOf(zipsDF)) // scalastyle:ignore
      println("-----") // scalastyle:ignore
    }

    println(">>>>>>>>>>>>>>>") // scalastyle:ignore

    for(i <- 1 to 10) {
      val zipsDS = sparkSession.read.json("hdfs://localhost:9000/users/lresende/zips.json").as[Zip]
      // zipsDS.unpersist(true)
      // sqlContext.clearCache()
      println("Zips are:" + manOf(zipsDS)) // scalastyle:ignore
      println("-----") // scalastyle:ignore
    }

    val zipsDS = sparkSession.read.json("hdfs://localhost:9000/users/lresende/zips.json").as[Zip]
    zipsDS.printSchema

    // zipsDS.groupBy


    // val countPerCity = zips.groupBy("city").count()

    // println(manOf(countPerCity)) // scalastyle:ignore

    // countPerCity.orderBy("count").show(1000)


  }
}
