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

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


object BigCsvApplication {
  def TimeTaken[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + " ns") // scalastyle:off
    result
  }

  def main(args: Array[String]): Unit = {

    println("Starting CSV Application") //scalastyle:ignore

    val sparkConf = new SparkConf()
      .setAppName("Spark-BIG-CSV")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    //sqlContext.setConf("spark.sql.shuffle.partitions", "30")

    val df = sqlContext.read.format("csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load ("hdfs://localhost:9000/users/lresende/2008.csv.bz2")

    // clean up the files in HDFS directory first if exist
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:9000"), hadoopConf)
    val output1 = "hdfs://localhost:9000/users/lresende/bigcsv/2008.csv"
    try { hdfs.delete(new org.apache.hadoop.fs.Path(output1), true) } catch { case _ : Throwable => { } }
    val output2 = "hdfs://rhes564:9000/users/lresende/bigcsv/2008_parquet.csv"
    try { hdfs.delete(new org.apache.hadoop.fs.Path(output2), true) } catch { case _ : Throwable => { } }

    println("--- Writting as CSV ==> " + df.count() + " records") //scalastyle:ignore
    TimeTaken(df.write.format("csv").option("header", "true").save("hdfs://localhost:9000/users/lresende/bigcsv/2008.csv"))

    println("--- Writting as PARQUET ==> " + df.count() + " records") //scalastyle:ignore
    TimeTaken(df.write.format("parquet").save("hdfs://localhost:9000/users/lresende/bigcsv/2008_parquet.csv"))

    println ("\nFinished at"); sqlContext.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ").collect.foreach(println)
    sys.exit()
  }
}
