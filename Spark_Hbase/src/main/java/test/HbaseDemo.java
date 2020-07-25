package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 2019-12-16
 * java -cp
 * */
public class HbaseDemo {

	public static void main(String[] args) throws IOException {

		String tableName = "testTable";

		Configuration HBASE_CONFIG = new Configuration();
		Configuration conf = HBaseConfiguration.create(HBASE_CONFIG);
		conf.addResource(HbaseDemo.class.getClassLoader().getResource("hbase-site.xml"));
		conf.addResource(HbaseDemo.class.getClassLoader().getResource("hbase-site.xml"));
		conf.addResource(HbaseDemo.class.getClassLoader().getResource("hbase-site.xml"));
		//		安全认证
		UserGroupInformation.setConfiguration(conf);
		System.setProperty("java.security.krb5.conf",
				"/home/opdn1zx/krb5.conf");
		UserGroupInformation.loginUserFromKeytab("opdn1zx@OPDN1ZX.TDH",
				"/home/opdn1zx/opdn1zx.keytab");

		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);

		//遍历表
		System.out.println("遍历表");
		TableName[] tables = hBaseAdmin.listTableNames();
		for (int i = 0; i < tables.length; i++) {

			System.out.println(tables[i].toString());
		}

		//put数据
		System.out.println("put 单条数据");
		hyperbasePut1(conf,tableName);

		//获取单条数据
		System.out.println("获取单条数据");
		hyperbaseGet(conf,tableName,"1");

		//扫描全表
		System.out.println("扫描全表");
		hyperbaseScan(conf,tableName);

		hBaseAdmin.close();
		System.exit(0);

	}


	/**
	 * 向hyperbase中put数据
	 * @param conf hbase Configuration
	 * @param tableName hyperbase name
	 * */

	public static void hyperbasePut1(Configuration conf,String tableName){
		//设置表名
		conf.set(TableInputFormat.INPUT_TABLE,tableName);
		Connection conn = null;
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}

		List<Put> puts = new ArrayList<Put>();
		Put put1 = new Put(Bytes.toBytes("1"));

		put1.add(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("wd"));

		Put put2 = new Put(Bytes.toBytes("1"));
		put2.add(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes("25"));

		Put put3 = new Put(Bytes.toBytes("1"));
		put3.add(Bytes.toBytes("cf"), Bytes.toBytes("sex"), Bytes.toBytes("man"));

		puts.add(put1);
		puts.add(put2);
		puts.add(put3);

		try {
			table.put(puts);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 根据rowkey获取hyperbase数据
	 * @param conf hbase Configuration
	 * @param tableName hyperbase name
	 * @param rowkey hyperbase rowkey
	 * */

	public static void hyperbaseGet(Configuration conf,String tableName,String rowkey) {

		//设置表名
		conf.set(TableInputFormat.INPUT_TABLE,tableName);
		Connection conn = null;
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		//获得一行
		Get get = new Get(Bytes.toBytes(rowkey));
		Result set = null;
		try {
			set = table.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Cell[] cells = set.rawCells();
		for (Cell cell: cells){

			System.out.println(Bytes.toString(cell.getQualifierArray(),
					cell.getQualifierOffset(),
					cell.getQualifierLength()) + "::" +
					Bytes.toString(cell.getValueArray(),
							cell.getValueOffset(),
							cell.getValueLength()));
		}
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}


	/**
	 * 全表扫描
	 * @param conf hbase Configuration
	 * @param tableName hyperbase name
	 * */

	public static void hyperbaseScan(Configuration conf,String tableName) {

		//设置表名
		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		Connection conn = null;
		ResultScanner rscan = null;
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		//全表扫描
		Scan scan1 = new Scan();
		try {
			rscan = table.getScanner(scan1);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (Result rs : rscan) {
			String rowKey = Bytes.toString(rs.getRow());
			System.out.println("row key :" + rowKey);
			Cell[] cells = rs.rawCells();
			for (Cell cell : cells) {
				System.out.println(Bytes.toString(cell.getFamilyArray(),
						cell.getFamilyOffset(),
						cell.getFamilyLength()) + "::"
						+ Bytes.toString(cell.getQualifierArray(),
						cell.getQualifierOffset(),
						cell.getQualifierLength()) + "::"
						+ Bytes.toString(cell.getValueArray(),
						cell.getValueOffset(),
						cell.getValueLength()));
			}
		}

		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


}
