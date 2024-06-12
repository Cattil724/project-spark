package com.atguigu.bigdata.spark.Test2

object iterator {
  def main(args: Array[String]): Unit = {

      val it = Iterator("Baidu", "Google", "Runoob", "Taobao")

      while (it.hasNext){
        println(it.next())
      }

  }

}
