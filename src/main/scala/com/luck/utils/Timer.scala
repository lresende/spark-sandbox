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
package com.luck.utils


object Timer {
  var begin: Double = 0;
  var total: Double = 0;

  /**
    * Start timer
    */
  def start(): Unit = {
    begin = System.currentTimeMillis
  }

  /**
    * Calculate a sample avarage and reset the begin counter
    * This is good to use particularly when measuring multiple samples
    */
  def addSample(): Unit = {
    val now: Long = System.currentTimeMillis
    total = (now - begin)
    begin = now
  }

  /**
    * Calculate a sample avarage based on the previous call to start or addSample
    */
  def finish(): Unit = {
    total = (System.currentTimeMillis - begin)
  }

  def getAverage(): Double = {
    return total;
  }

  def printAverage(): Unit = {
    val formatter = java.text.NumberFormat.getIntegerInstance
    println("Elapsed time: " + formatter.format(total) + " ms") // scalastyle:ignore
  }
}
