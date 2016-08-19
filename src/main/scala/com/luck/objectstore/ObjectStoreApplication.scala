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

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ObjectStoreApplication {
  def main(args: Array[String]): Unit = {
    println("Starting Streaming StreamingApplication") // scalastyle:ignore

    var sparkConf: SparkConf = new SparkConf()
      .setAppName("ObjectStore-Application")

    // check Spark configuration for master URL, set it to local if not configured
    if (! sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val sparkSession: SparkSession = SparkSession.builder
      .config(sparkConf)
      .getOrCreate

    // scalastyle:off

    sparkSession.sparkContext.hadoopConfiguration.set("fs.swift2d.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.swift2d.service.Object-Storage-lresende.auth.url", "https://identity.open.softlayer.com/v3/auth/tokens")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.swift2d.service.Object-Storage-lresende.public", "true")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.swift2d.service.Object-Storage-lresende.tenant", "13e6f50b25cf41d795fadd8204dd3896")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.swift2d.service.Object-Storage-lresende.password", "Z#R3R1dbNQWqws#J")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.swift2d.service.Object-Storage-lresende.username", "568d8d81902f4543b36ab1ddc1027aca")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.swift2d.service.Object-Storage-lresende.auth.method", "keystoneV3")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.swift2d.service.Object-Storage-lresende.region", "dallas")
    sparkSession.sparkContext.hadoopConfiguration.set("", "")
    sparkSession.sparkContext.hadoopConfiguration.set("", "")
    // scalastyle:on

    val data = Array(1, 2, 3, 4, 5, 6, 7, 8)
    val distData = sparkSession.sparkContext.parallelize(data)
    for(i <- 1 to 50) {
        distData.saveAsTextFile("swift2d://parquet.Object-Storage-lresende/one" + i + ".txt")
    }
  }
}
