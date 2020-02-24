package com.chaiwei.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Before;
import org.junit.Test;

public class HBaseClient {

	static Configuration config = null;
	private Connection connection = null;
	private Table table = null;
	
	@Before
	//初始化hbase
	public void init() throws IOException{
		//1.配置hbase
		config = HBaseConfiguration.create();
		//2.指定zk的地址  注意windows上的host映射
		config.set("hbase.zookeeper.quorum", "chai");
		config.set("hbase.zookeeper.proterty.clientPort", "2181");
		//3.通过工厂模式创建hbase的链接
		connection = ConnectionFactory.createConnection(config);
		
		//4.连接一个表
		table = connection.getTable(TableName.valueOf("chai"));
	}
	
	
	//创建一个表
	@Test
	public void createTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		//1.创建表的管理类
		HBaseAdmin admin = new HBaseAdmin(config);
		//2.创建表的描述类
		TableName tableName = TableName.valueOf("chai");
		HTableDescriptor descriptor = new HTableDescriptor(tableName);
		
		//3.创建列族的描述类
		HColumnDescriptor family1 = new HColumnDescriptor("info");
		descriptor.addFamily(family1);
		
		HColumnDescriptor family2 = new HColumnDescriptor("info2");
		descriptor.addFamily(family2);
		
		//创建表
		admin.createTable(descriptor);
		
		
	}
	
}