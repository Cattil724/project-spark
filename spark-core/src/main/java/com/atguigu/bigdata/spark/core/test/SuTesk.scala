package com.atguigu.bigdata.spark.core.test

class SuTesk extends Serializable {
      var datas:List[Int]=_
      var logic:(Int)=>Int=_

  def compute()={
    datas.map(logic)
  }


}
