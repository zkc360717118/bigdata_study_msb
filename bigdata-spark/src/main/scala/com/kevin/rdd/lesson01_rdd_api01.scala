package com.kevin.rdd

import org.apache.spark.{SparkConf, SparkContext}

object lesson01_rdd_api01 {
  //面向数据集操作：
    //1. 带函数，非聚合：  map,flatmap
   //2. 单元素： union, cartesion 没有函数
   //3. kv 元素： cogroup,join 没有函数计算
   //4. 排序
   //5. 聚合计算： reduceByKey 有函数 combinerByKey

   def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setMaster("local[*]").setAppName("test01")
     val sc = new SparkContext(conf)

     sc.setLogLevel("ERROR")

     val dataRDD = sc.parallelize(List(1, 2, 3, 4, 5, 4, 3, 2, 1))

     val filterRDD = dataRDD.filter(_ > 3)
     val res01 = filterRDD.collect()
     res01.foreach(println)

     println("----------------")


  }
}
