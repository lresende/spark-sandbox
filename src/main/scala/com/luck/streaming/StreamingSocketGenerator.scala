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
package com.luck.streaming


import java.io._
import java.net.{InetAddress, ServerSocket, Socket, SocketException}
import java.util.Random

object StreamingSocketGenerator {
  val ServerPort = 9999;

  def main(args: Array[String]): Unit = {
    try {
      println("Starting streaming application...") // scalastyle:ignore
      val serverSocket = new ServerSocket(ServerPort)
      val clientSocket = serverSocket.accept()
      // while (true) {
        new ServerThread(clientSocket).start()
      // }
      serverSocket.close()
    }
    catch {
      case e: IOException =>
        System.err.println("Could not listen on port: 9999."); //scalastyle:ignore
        System.exit(-1)
    }
  }
}

case class ServerThread(socket: Socket) extends Thread("ServerThread") {

  def generateJson(symbol: String): String = {
    val rand = new Random(System.currentTimeMillis());
    val price = rand.nextFloat();
    val date = java.time.LocalDateTime.now().toString()

    val json: String = "{\"symbol\":\"%s\", \"price\":%f, \"date\":\"%s\"}"
                       .format(symbol, price, date)

    return json
  }

  override def run(): Unit = {
    println("Starting streaming thread...") // scalastyle:ignore
    var symbols = Array("LNKD", "IBM", "APPL")
    try {
      val out = new PrintWriter(socket.getOutputStream(), true)
      val in = new BufferedReader( new InputStreamReader(socket.getInputStream))

      var count = 0
      while (count < 1000) {
        count = count + 1
        for(s <- symbols) {
          val json: String = generateJson(s)
          println(f"Submitting ==> $json") // scalastyle:ignore
          out.println(json) // scalastyle:ignore

          Thread.sleep(10)
        }
      }

      out.close()
      in.close()
      socket.close()
    }
    catch {
      case e: SocketException =>
        () // avoid stack trace when stopping a client with Ctrl-C
      case e: IOException =>
        e.printStackTrace();
    }
  }

}