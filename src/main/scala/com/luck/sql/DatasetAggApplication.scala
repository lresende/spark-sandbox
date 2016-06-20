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
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Dataset

case class People(age: Int, name: String)

object nameAgg extends Aggregator[People, String, String] {
  def zero: String = ""
  def reduce(b: String, a: People): String = a.name + b
  def merge(b1: String, b2: String): String = b1 + b2
  def finish(r: String): String = r
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

    val peopleds:
      Dataset[People] = sqlContext.sql("SELECT 'Tim Preece' AS name, 1279869254 AS age").as[People]

    peopleds.groupBy(_.age).agg(nameAgg.toColumn).show()
  }
}
