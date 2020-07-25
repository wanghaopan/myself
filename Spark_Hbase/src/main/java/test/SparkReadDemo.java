package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


import java.io.IOException;

/**
 * Created by 2019-12-16
 * spark-submit
 * */

public class SparkReadDemo {
    public static void main(String[] args) throws IOException {


        String tableName = "testTable";
        String appName = "sparkReadDemo";

        Configuration HBASE_CONFIG = new Configuration();
        Configuration hconf = HBaseConfiguration.create(HBASE_CONFIG);
        hconf.addResource(HbaseDemo.class.getClassLoader().getResource("hbase-site.xml"));
        hconf.addResource(HbaseDemo.class.getClassLoader().getResource("hbase-site.xml"));
        hconf.addResource(HbaseDemo.class.getClassLoader().getResource("hbase-site.xml"));

        UserGroupInformation.setConfiguration(hconf);
        //		安全认证
        System.setProperty("java.security.krb5.conf",
                "/home/opdn1zx/krb5.conf");

        UserGroupInformation.loginUserFromKeytab("opdn1zx@OPDN1ZX.TDH",
                "/home/opdn1zx/opdn1zx.keytab");

        HBaseAdmin hBaseAdmin = new HBaseAdmin(hconf);

        SparkConf sparkConf = new SparkConf().setAppName(appName);
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //遍历表
        System.out.println("遍历表");
        TableName[] tables = hBaseAdmin.listTableNames();
        for (int i = 0; i < tables.length; i++) {

            System.out.println(tables[i].toString());
        }
        //put数据
        System.out.println("put 单条数据");
        HbaseDemo.hyperbasePut1(hconf,tableName);

        //获取单条数据
        System.out.println("获取单条数据");
        HbaseDemo.hyperbaseGet(hconf,tableName,"1");

        //扫描全表
        System.out.println("扫描全表");
        HbaseDemo.hyperbaseScan(hconf,tableName);

        hBaseAdmin.close();
        System.exit(0);



    }
}
