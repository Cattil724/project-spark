package com.atguigu.bigdata.spark.core.test

import java.io.{InputStream, ObjectInputStream, ObjectOutputStream}
import java.net.{ServerSocket, Socket}

object Executor {
  def main(args: Array[String]): Unit = {
    //    启动服务器，接受数据
    val server = new ServerSocket(9999)
    println("等待接收数据")
//    等待连接
    val client: Socket = server.accept()
    val in: InputStream = client.getInputStream
    val objIn = new ObjectInputStream(in)
    val tesk: SuTesk = objIn.readObject().asInstanceOf[SuTesk]
    val ints: List[Int] = tesk.compute()
    println("计算点[9999]计算结果为："+ints)
    objIn.close()
    client.close()
    server.close()
  }

}
