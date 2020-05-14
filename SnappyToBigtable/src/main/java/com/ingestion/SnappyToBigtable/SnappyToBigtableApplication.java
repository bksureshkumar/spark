package com.ingestion.SnappyToBigtable;


import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.lang.System.currentTimeMillis;

//Spark Application POC to ingest the Snappy compressed data to Bigtable
public class SnappyToBigtableApplication {
	final static byte[] COLUMN_FAMILY = "cf".getBytes();
	final static byte[] COLUMN_QUALIFIER = "c1".getBytes();

	//Use the projectId and instanceId from the hbase-site.xml
//	final static String projectId = "broadcom-gcp-migration-pocs";
//	final static String instanceId = "broadcom-new-poc";

	final static String tableId = "poc-table";

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Snappy to Bigtable")
				.set("spark.executor.memory","2g");

		sparkConf.setJars(JavaSparkContext.jarOfClass(SnappyToBigtableApplication.class));

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> distFile = sc.textFile("gs://colossus-poc-snappy-data/*.snappy");

		//Make it to multiple partitions to load them in parallel to Bigtable
		distFile = distFile.repartition(100);
		System.out.println("THE TOTAL PARTITIONS: " + distFile.getNumPartitions());

		//Ingest the data for each partitions
		distFile.foreachPartition(new VoidFunction<Iterator<String>>() {
			@Override
			public void call(Iterator<String> partData) throws Exception {
				Configuration conf = HBaseConfiguration.create();
				TableName tableName = TableName.valueOf(tableId);
				try {
					createTable(tableName, conf,
							Collections.singletonList(Bytes.toString(COLUMN_FAMILY)));
				} catch (Exception e) {
					throw e;
				}
			//WRITE DATA: The whole row is written as Row key and as column  value... FOR POC purpose
				for (Iterator<String> it = partData; it.hasNext(); ) {
					String data = it.next();
					Connection connection = ConnectionFactory.createConnection(conf);
//				TableName tableName = TableName.valueOf(tblName);
					try (BufferedMutator mutator = connection.getBufferedMutator(tableName)) {
						mutator.mutate(new Put(data.getBytes(), currentTimeMillis()).addColumn(COLUMN_FAMILY,
								COLUMN_QUALIFIER, data.getBytes()));
						mutator.flush();
					} catch (Exception e) {
						throw e;
					}
				}


			}
		});

		sc.stop();
	}

	//Create the Table, if it is not exists
	public static void createTable(TableName tableName, Configuration conf,
								   List<String> columnFamilies) throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = null;
		try {
			admin = connection.getAdmin();
			if (tableExists(tableName, admin)) {
//				LOG.info("Table " + tableName + " already exists");
			} else {
				HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
				for (String columnFamily : columnFamilies) {
					tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
				}
				admin.createTable(tableDescriptor);
			}
		} catch (Exception e) {
//			LOG.error("Could not create table " + tableName, e);
		} finally {
			try {
				admin.close();
			} catch (Exception e) {
//				LOG.error("Could not close the admin", e);
			}
			connection.close();
		}
	}

	private static boolean tableExists(TableName tableName, Admin admin)  {
		try {
			return admin.tableExists(tableName);
		} catch (Exception e) {
//			LOG.error("Could not figure out if table " + tableName + " exists.", e);
			return false;
		} finally {
		}
	}

	public static double getRandomIntegerBetweenRange(double min, double max){
		double x = (int)(Math.random()*((max-min)+1))+min;
		return x;
	}

	public static String generateRowKey(){
		//row18#randomstring
		int bucketSlot = (int) getRandomIntegerBetweenRange(0,100);
		String rowKey = RandomStringUtils.randomAlphanumeric(60);
		return "row" + bucketSlot + "#" + rowKey;
	}
}
