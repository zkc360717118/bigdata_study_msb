package com.kevin.rdd

import org.apache.spark.rdd.RDD
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

//    val dataRDD = sc.parallelize(List(1, 2, 3, 4, 5, 4, 3, 2, 1))
//
//    val filterRDD = dataRDD.filter(_ > 3)
//    val res01 = filterRDD.collect()
//    res01.foreach(println)

    println("----------------")
    //RDD  （HadoopRDD,MappartitionsRDD,ShuffledRDD...）
    //reduceByKey:  复合  ->  combineByKey（）

    //spark很人性，面向数据集提供了不同的方法的封装，且，方法已经经过经验，常识，推算出自己的实现方式
    //人不需要干预（会有一个算子）
    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))
    val rdd2: RDD[Int] = sc.parallelize(List(3, 4, 5, 6, 7))
//    val subtracedRDD = rdd1.subtract(rdd2)
//    subtracedRDD.foreach(println)  // 1 2
//
//    println("----------------")
//    val intersectedRDD = rdd1.intersection(rdd2)
//    intersectedRDD.foreach(println)
//
//    println("----------------")

    // 如果数据，不需要区分每一条记录归属于那个分区。。。间接的，这样的数据不需要partitioner。。不需要shuffle ,比如cartesian union
    //因为shuffle的语义：洗牌  ---》面向每一条记录计算他的分区号
    //如果有行为，不需要区分记录，本地IO拉去数据，那么这种直接IO一定比先Parti。。计算，shuffle落文件，最后在IO拉去速度快！！！ x
//    val cartesian: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
//    cartesian.collect().foreach(println)


//    println(rdd1.partitions.size) //4
//    println(rdd2.partitions.size) //4
//    val unionedRDD = rdd1.union(rdd2)
//    println(unionedRDD.partitions.size) // 8 说明只是把rdd中
//    println("-------union test---------")
//    unionedRDD.collect().foreach(println)

    println("-------kv  value---------")
    val kv1: RDD[(String, Int)] = sc.parallelize(List(
      ("zhangsan", 11),
      ("zhangsan", 12),
      ("lisi", 13),
      ("wangwu", 14)
    ))
    val kv2: RDD[(String, Int)] = sc.parallelize(List(
      ("zhangsan", 21),
      ("zhangsan", 22),
      ("lisi", 23),
      ("zhaoliu", 28)
    ))

    println("-------cogroup---------")

    kv1.cogroup(kv2).foreach(println)

    println("-------join ---------")
    kv1.join(kv2).foreach(println)

    println("-------leftOuterJoin ---------")
    kv1.leftOuterJoin(kv2).foreach(println)

    println("-------rightOuterJoin ---------")
    kv1.rightOuterJoin(kv2).foreach(println)

    println("-------fullOuterJoin ---------")
    kv1.fullOuterJoin(kv2).foreach(println)

    while(true){

    }
  }
}
