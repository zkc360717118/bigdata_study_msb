package com.kevin.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("word cont")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    val fileRdd:RDD[String] = sc.textFile("bigdata-spark/data/testdata.txt")

//    统计每个单词出现的次数
    fileRdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
//     统计单词出现2次以上的有哪些
    val words = fileRdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    words.filter(_._2>=2).foreach(println)

  Thread.sleep(Long.MaxValue)
  }

}
