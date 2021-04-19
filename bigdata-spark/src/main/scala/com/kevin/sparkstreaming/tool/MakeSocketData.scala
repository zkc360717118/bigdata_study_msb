package com.kevin.sparkstreaming.tool

import java.io.{OutputStream, PrintStream}
import java.net.{ServerSocket, Socket}

object MakeSocketData {
  /**
   * create a socket client and connect to 8889 port, send message to endless in every secs
   * eg:
    hello 1
    hi 1
    hi1


    hello 2
    hi 2
    hi 2
   * @param args
   */

  def main(args: Array[String]): Unit = {


    val listen = new ServerSocket(8889)
    println("server start")
    while(true){
      val client: Socket = listen.accept()

      new Thread(){
        override def run(): Unit = {
          var num = 0
          if(client.isConnected){
            val out: OutputStream = client.getOutputStream
            val printer = new PrintStream(out)
            while(client.isConnected){
              num+=1

              printer.println(s"hello ${num}")
              printer.println(s"hi ${num}")
              printer.println(s"hi ${num}")

              Thread.sleep(1000)
            }
          }
        }
      }.start()
    }
  }


}
