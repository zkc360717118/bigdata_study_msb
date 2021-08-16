package com.kevin.common

import org.apache.spark.sql.SparkSession

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

object SparkSessionHelper {
  def main(args: Array[String]): Unit = {
//    val spark = initSparkSessionFromWindow(false)
//    val spark = initSparkSessionFromCDHWindow(false)
//    spark.sql("create table test(id string, name string)")
//    spark.sql("insert into test values(%s,%s)".format("1","a"))
//
//    spark.sql("show databases").show()

  }
  def initSparkSessionInCluster={
    SparkSession.builder().appName(s"${this.getClass.getSimpleName}").enableHiveSupport().getOrCreate()
  }

//  def initSparkSessionFromCDHWindow(debug:Boolean)={
//    //    System.setProperty("HADOOP_USER_NAME","root")
//    if (debug) System.setProperty("sun.security.krb5.debug", "true")
//    System.setProperty("java.security.krb5.conf","G:\\projects\\bigdata_study_msb\\bigdata-spark\\src\\main\\resources\\kerberos\\krb5.conf")
//    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
//
//    val  configuration = new Configuration();
//    configuration.set("hadoop.security.authentication", "kerberos");
//    configuration.setBoolean("hadoop.security.authorization", true);
//    configuration.set("kerberos.principal", "hive/cdh01@HADOOP.COM");
//    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
//    try{
//      UserGroupInformation.setConfiguration(configuration);
//      UserGroupInformation.loginUserFromKeytab("hive/cdh01@HADOOP.COM", "G:\\projects\\bigdata_study_msb\\bigdata-spark\\src\\main\\resources\\kerberos\\hive.keytab");
//      println(UserGroupInformation.getCurrentUser)
//    }catch{
//      case e:Exception=> println(e.getMessage)
//    }
//
//        SparkSession.builder()
//          .appName("hiveAppFromWindow")
//          .master("local[*]")
//          .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
//          .enableHiveSupport()
//          .getOrCreate()
//  }

  /**
   * 初始化sparksession来自ｗｉｎｄｏｗｓ， 集群非cdh
   * @param debug
   */
  def initSparkSessionFromWindow(debug:Boolean)={
    System.setProperty("HADOOP_USER_NAME","kevin")
    SparkSession.builder()
      .appName("APP_FROM_KEVIN_NON_CDH")
      .master("local[*]")
//      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.exec.dynamic.partition",true)
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .enableHiveSupport()
      .getOrCreate()

  }

}
