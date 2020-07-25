package test

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by 2019-12-16
 * 通过HTable中的Put向HBase写数据
 */

object HBaseWriteTest {
  def main(args: Array[String]): Unit = {
    val appName="HBaseWriteTest"
    val tableName = "testTable"

    //配置hyperBase
    val conf = HBaseUtils.getHBaseConfiguration()
    //设置表名
    conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    val htable = HBaseUtils.getTable(conf,tableName)

    val sparkConf = new SparkConf().setAppName(appName)


    val sc = new SparkContext(sparkConf)

    val indataRDD = sc.makeRDD(Array("1,ZhangSan,10","2,LiSi,10","3,ZhaoWu,50"))

    println("写入数据")

    indataRDD.foreachPartition(x=> {
      x.foreach(y => {
        val arr = y.split(",")
        val key = arr(0)
        val name = arr(1)
        val age = arr(2)

        val put = new Put(Bytes.toBytes(key))
        put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(name))
        put.add(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(age))
        htable.put(put)
      })
    })

    sc.stop()
    System.exit(0)

  }
}