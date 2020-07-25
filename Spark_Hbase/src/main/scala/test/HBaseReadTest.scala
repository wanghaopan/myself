package test


import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by 2019-12-16
 * read data from Hyperbase
 */

object HBaseReadTest {
  def main(args: Array[String]): Unit = {

    val appName="HBaseReadTest"
    val tableName = "testTable"
    val hdfsAddr = "hdfs:///sophon/home/opdn1zx/00000-test0"

    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)

    // 配置相关信息
    val conf = HBaseUtils.getHBaseConfiguration()

    //设置表名
    conf.set(TableInputFormat.INPUT_TABLE,tableName)

    // HBase数据转成RDD
    println(" HBase数据转成RDD")
    val RDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    println("result saveAsTestFile to hdfs")

    //将结果保存到hdfs
    RDD.cache().map(x => {
      val result = x._2
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("cf".getBytes,"name".getBytes))
      val age = Bytes.toString(result.getValue("cf".getBytes,"age".getBytes))
      (key,name,age)
    }).saveAsTextFile(hdfsAddr)


    sc.stop()
    System.exit(0)
  }
}
