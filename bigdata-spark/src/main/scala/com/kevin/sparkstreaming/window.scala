package com.kevin.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object window {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("testAPI")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Duration(1000)) //最小粒度  约等于：  win：  1000   slide：1000


    //-------------------------------------获取数据-----------------------------------------------
    //----
    val resource: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8889)
    val format = resource.map(_.split(" ")).map(x => (x(0), 1))

    //-------------------------------------window  api-----------------------------------------------
    /**
     * 总结一下，其实一直有窗口的概念，默认，val ssc = new StreamingContext(sc,Duration(1000))  //最小粒度  约等于：  win：  1000   slide：1000
     */

    //: 每秒中看到历史5秒的统计
    //方案1
//    val res: DStream[(String, Int)] = format.reduceByKey(_ + _)  //  窗口量是  1000  slide  1000
    //    res.print()
//    val res: DStream[(String, Int)] = format.window(Duration(5000)).reduceByKey(_ + _)
//        res.print()

    //方案2：
    //    val res: DStream[(String, Int)] = format.reduceByKeyAndWindow(_+_,Duration(5000))

    //下面是什么鬼
    //    val resource: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",8889)
    //    val format: DStream[(String, Int)] = resource.map(_.split(" ")).map(x=>(x(0),x(1).toInt))
    //    //调优：
    //    //reduceByKey 对  combinationBykey的封装  放入函数，聚合函数，combina函数
    //    val res: DStream[(String, Int)] = format.reduceByKeyAndWindow(
    //      //计算新进入的batch的数据
    //      (ov:Int,nv:Int)=>{
    //        println("first fun......")
    //        println(s"ov:$ov  nv:$nv")
    //
    //        ov+nv
    //    }
    //
    //      ,
    //      //挤出去的batch的数据
    //      (ov:Int,oov:Int)=>{
    ////        println("di 2 ge fun.....")
    ////        println(s"ov:$ov   oov:$oov")
    //        ov-oov
    //      }
    //      ,
    //      Duration(6000),Duration(2000)
    //    )
    //
    //    res.print()
    //
    //
    //
    //    res.print()


    //    format.print()


    //    val res1s1batch: DStream[(String, Int)] = format.reduceByKey(_+_)
    ////    res1s1batch.mapPartitions(iter=>{println("1s");iter}).print()//打印的频率：1秒打印1次
    //
    //
    //    val newDS: DStream[(String, Int)] = format.window(Duration(5000),Duration(5000))
    //
    //    val res5s5batch: DStream[(String, Int)] = newDS.reduceByKey(_+_)
    //    res5s5batch.mapPartitions(iter=>{println("5s");iter}).print() //打印频率：1秒打印1次

    ssc.start()
    ssc.awaitTermination()
  }
}
