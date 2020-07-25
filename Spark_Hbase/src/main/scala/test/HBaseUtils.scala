package test

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable}


/**
 * Created by 2019-12-16
 * read data from Hyperbase
 * hbase utils class
 * */

object HBaseUtils {

  /**
   * 设置HBaseConfiguration
   */
  def getHBaseConfiguration() = {
    val conf = HBaseConfiguration.create()
    conf.addResource(classOf[HbaseDemo].getClassLoader.getResource("hbase-site.xml"))
    conf.addResource(classOf[HbaseDemo].getClassLoader.getResource("hbase-site.xml"))
    conf.addResource(classOf[HbaseDemo].getClassLoader.getResource("hbase-site.xml"))

    conf
  }
  /**
   * 返回HBaseAdmin
   * @param conf
   * @param tableName
   * @return
   */
  def getHBaseAdmin(conf:Configuration,tableName:String) = {
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    }

    admin
  }

  /**
   * 返回HTable
   * @param conf
   * @param tableName
   * @return
   */
  def getTable(conf:Configuration,tableName:String) = {
    new HTable(conf,tableName)
  }





}
