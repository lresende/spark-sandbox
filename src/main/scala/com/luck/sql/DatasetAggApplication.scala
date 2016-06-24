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

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

case class AggData(a: Int, b: String)


object NameAgg extends Aggregator[AggData, String, String] {
  def zero: String = ""
  def reduce(b: String, a: AggData): String = a.b + b
  def merge(b1: String, b2: String): String = b1 + b2
  def finish(r: String): String = r
  override def bufferEncoder: Encoder[String] = Encoders.STRING
  override def outputEncoder: Encoder[String] = Encoders.STRING
}


/**
  * Test scenario to replicate dataset corruption when input data is reordered
  * as described in SPARK-12555
  *
  * https://issues.apache.org/jira/browse/SPARK-12555
  */
object  DatasetAggApplication {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataSetAgg")
    val spark = new SparkContext(conf)
    val sqlContext = new SQLContext(spark)
    import sqlContext.implicits._

    val ds:
      Dataset[AggData] = sqlContext.sql("SELECT 'Tim Preece' AS b, 1279869254 AS a").as[AggData]

    // Spark 1.x
    // ds.groupBy(_.a).agg(NameAgg.toColumn).show()

    // Spark 2.0
    ds.groupByKey(_.a).agg(NameAgg.toColumn).show()
  }
}
