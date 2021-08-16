package sparksql

import com.kevin.common.SparkSessionHelper
import org.apache.spark.sql.SparkSession

object TestSpark {
  def main(args: Array[String]): Unit = {


    val spark = SparkSessionHelper.initSparkSessionFromWindow(false)
    import spark.implicits._
    val df = Seq(("kevin", "1", "20210201"), ("lara", "2", "20210203")).toDF("name", "age", "DATE" +
      "")
//    df.select($"name",$"age"+1).show()
//    spark.sql("drop table test_partition")
//    spark.sql("create table test_partition (`name` String,`age` String) partitioned by(`DATE` String)")
//    spark.sql("show partitions test_partition").show
    df.write.mode("overwrite").insertInto("test_partition")
    spark.sql("select * from test_partition").show
  }
}
