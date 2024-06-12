package com.atguigu.bigdata.spark.core.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket


object Driver {
  def main(args: Array[String]): Unit = {
//    连接服务器
      val client1=new Socket("localhost",9999)
      val client2=new Socket("localhost",8888)
    val tesk = new Tesk()

    val out1: OutputStream = client1.getOutputStream
    val objout1 = new ObjectOutputStream(out1)
    val suTesk = new SuTesk

    suTesk.logic=tesk.logic
    suTesk.datas=tesk.datas.take(2)

    objout1.writeObject(suTesk)
    objout1.flush()
    objout1.close()
    client1.close()



    val out2: OutputStream = client2.getOutputStream
    val objout2 = new ObjectOutputStream(out2)

    val suTesk1 = new SuTesk

    suTesk1.logic=tesk.logic
    suTesk1.datas=tesk.datas.takeRight(2)

    objout2.writeObject(suTesk1)
    objout2.flush()
    objout2.close()
    client1.close()
    println("客户端发送完毕")

  }

}
