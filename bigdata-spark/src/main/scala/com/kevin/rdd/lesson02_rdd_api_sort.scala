package com.kevin.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object lesson02_rdd_api_sort {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sort")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //PV,UV
    //需求：根据数据计算各网站的PV,UV，同时，只显示top3
    //解题：要按PV值，或者UV值排序，取前3名
    val fileRDD = sc.textFile("bigdata-spark/data/pvuvdata",5)

    //pv：
    //  187.144.73.116	浙江	2018-11-12	1542011090255	3079709729743411785	www.jd.com	Comment
//    (18771,www.taobao.com)
//    (18728,www.mi.com)
//    (18636,www.baidu.com)
    println("----------PV:-----------")
    fileRDD.map(_.split("\t")).
      map(x=>(x.apply(5),1))
      .reduceByKey(_+_)
      .map(x=>(x._2,x._1))
      .sortByKey(false)
      .take(3)  //
      .map(_.swap)   //等同于上面的 map(x=>(x._2,x._1))
      .foreach(println)

    println("----------UV:-----------")
    val keys: RDD[(String, String)] = fileRDD.map(
      line => {
        val strs: Array[String] = line.split("\t")
        (strs(5), strs(0))
      }
    ).distinct() //先把所有数据去重一次


  }
}
