#!/usr/bin/env bash
#
# Copyright (c) 2015 Luciano Resende
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ps aux |grep "spark-sandbox" | tr -s " " |  cut -d " " -f 2 | xargs kill >/dev/null 2>&1

# using environment variable to find Spark & Hadoop home directory
if [ -z "$SPARK_HOME" ]; then echo "SPARK_HOME is NOT set"; else echo "SPARK_HOME defined as '$SPARK_HOME'"; fi
if [ -z "$HADOOP_HOME" ]; then echo "$HADOOP_HOME is NOT set"; else echo "HADOOP_HOME defined as '$HADOOP_HOME'"; fi

HOSTNAME="$(/bin/hostname -f)"

sbt clean compile package assembly

if [ "$1" = "csv" ]
then
  echo "Starting CSV Application at $SPARK_HOME"
  hadoop fs -rm hdfs://localhost:9000/users/lresende/data.csv
  hadoop fs -put /Users/lresende/dev/stc/source/spark-stream/src/main/resources/csv/data.csv hdfs://localhost:9000/users/lresende/data.csv
  nohup $SPARK_HOME/bin/spark-submit --master spark://$HOSTNAME:7077 --packages com.databricks:spark-csv_2.11:1.3.0 --class com.luck.csv.CsvApplication ./target/scala-2.11/spark-sandbox_2.11-1.0.jar >> ./target/application.out; tail -100f ./target/application.out &
fi
