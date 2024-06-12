package com.atguigu.bigdata.spark.core.framework.service


import com.atguigu.bigdata.spark.core.framework.common.TService
import com.atguigu.bigdata.spark.core.framework.dao.WordConutDao
/*
* 控制层
* */
class WordConutService extends TService{

  private val WordConutDao=new WordConutDao()
//数据分析
  def dataAnalysis()={

    val lines=WordConutDao.readFile("data/word.txt")

    val words = lines.flatMap(_.split(" "))

    val wordGrouo = words.groupBy(word => word)

    val wordToCount = wordGrouo.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    val array = wordToCount.collect()
    array
  }

}
