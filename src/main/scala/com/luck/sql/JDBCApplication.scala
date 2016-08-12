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

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Test Scenario for the following JIRAs related to case sensitive
  * on the table/column when using JDBC
  *
  * https://issues.apache.org/jira/browse/SPARK-6666
  * https://issues.apache.org/jira/browse/SPARK-8377
  */
object JDBCApplication {

  def runDB2(sparkSession: SparkSession): DataFrame = {
    val jdbcUrl = "jdbc:db2://192.168.99.100:50000/foo:user=db2inst1;password=rootpass;retrieveMessagesFromServerOnGetMessage=true;" // scalastyle:ignore

    val df = sparkSession.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", "com.ibm.db2.jcc.DB2Driver")
      .option("dbtable", "DB2INST1.SP500")
      .load()

    return df
  }

  def runPostgres(sparkSession: SparkSession): DataFrame = {
    val jdbcUrl = "jdbc:postgresql://192.168.99.100:5432/foo?user=postgres&password=rootpass" // scalastyle:ignore

    val df = sparkSession.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "sp500")
      .load()

    return df
  }
  def main(args: Array[String]): Unit = {

    println("Starting JDBC Application") //scalastyle:ignore

    var sparkConf: SparkConf = new SparkConf()
      .setAppName("Spark-JDBC")

    // check Spark configuration for master URL, set it to local if not configured
    if (! sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val sparkSession: SparkSession = SparkSession.builder
      .config(sparkConf)
      .getOrCreate

    val df = runPostgres(sparkSession)

    df.createOrReplaceTempView("sp500")

    df.printSchema()

    df.show()

    val avgEPSNamed = sparkSession.sql("SELECT AVG(`Earnings/Share`) as AvgCPI FROM sp500")
    avgEPSNamed.show()

    val all = sparkSession.sql("SELECT * FROM sp500")
    all.show
  }

}
