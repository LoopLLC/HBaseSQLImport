/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.autoloop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author ericzbeard
 */
public class HBaseSQLImport {

	/**
	 * @param args the command line arguments
	 */
	public static void main(String[] args) throws Exception {
		
		if (args.length == 0) {
			usage();
			return;
		}
		
		Configuration config = HBaseConfiguration.create();
		//config.set("log4j.logger.org.apache.zookeeper", "WARN");
		//config.set("log4j.logger.org.apache.hadoop.hbase.zookeeper.ZKUtil", "WARN");
		//config.set("log4j.logger.org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher", "WARN");
		//config.set("log4j.logger.org.apache.hadoop.hbase", "WARN");
		//Logger log = Logger.getLogger("log4j.logger.org.apache.zookeeper");
		//log.setLevel(Level.WARN);
		//log = Logger.getLogger("log4j.logger.org.apache.hadoop.hbase.zookeeper.ZKUtil");
		//log.setLevel(Level.WARN);
		//log = Logger.getLogger("log4j.logger.org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher");
		//log.setLevel(Level.WARN);
		//log = Logger.getLogger("log4j.logger.org.apache.hadoop.hbase");
		//log.setLevel(Level.WARN);
		
		HTable table = new HTable(config, "schema");
		
		HBaseDescription d = new HBaseDescription();
		HBaseColumn c = new HBaseColumn();
		d.setHbaseColumn(c);
		
		boolean isSave = false;
		boolean isShow = false;
		boolean showQuery = false;
		boolean isDelete = false;
		boolean isFormatted = false;
		
		for (int i = 0; i < args.length; i++) {
			String arg = args[i].toLowerCase();
			switch (arg) {
				case "-qn": // Query Name
					d.setQueryName(args[++i]);
					break;
				case "-q": // Query File
					String queryFile = args[++i];
					byte[] encoded = Files.readAllBytes(Paths.get(queryFile));
					d.setQuery(StandardCharsets.UTF_8
							.decode(ByteBuffer.wrap(encoded)).toString());
					break;
				case "-ty":
					d.setType(args[++i]);
					break;
				case "-k":
					d.setSqlKey(args[++i]);
					break;
				case "-hbt":
					d.setTableName(args[++i]);
					break;
				case "-hbcf":
					c.setColumnFamily(args[++i]);
					break;
				case "-hbq":
					c.setQualifier(args[++i]);
					break;
				case "-c":
					c.setSqlName(args[++i]);
					break;
				case "-hbl":
					c.setLogicalName(args[++i]);
					break;
				case "-hbd":
					c.setDescription(args[++i]);
					break;
				case "-save":
					isSave = true;
					break;
				case "-show":
					isShow = true;
					break;
				case "-query":
					showQuery = true;
					break;
				case "-delete":
					isDelete = true;
					break;
				case "-format":
					isFormatted = true;
					break;
				default: break;
			}
		}
		
		if (isSave) {
			save(table, d);
		} else if (isShow) {
			show(table, d, showQuery, isFormatted);
		} else if (isDelete) {
			delete(table, d);
		} else {
			usage();
		}
		
	}
	
	static void delete(HTable table, HBaseDescription d) throws Exception {
		List<Delete> list = new ArrayList<>();
		String rowKey = d.getRowKey();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
        table.delete(list);
	}
	
	static void show(HTable table, HBaseDescription d, boolean showQuery, 
			boolean isFormatted) throws Exception {
		if (d.getQueryName() == null) {
			System.out.println("Missing -qn QueryName");
			usage();
			return;
		}
		
		// Show all info regarding that Query
		Scan s = new Scan();
		byte[] cf = Bytes.toBytes("d");
		byte[] a = Bytes.toBytes("qn");
		
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		
		// Get the row for the table/query/keys
		SingleColumnValueFilter tableFilter = new SingleColumnValueFilter(
			cf,
			a,
			CompareFilter.CompareOp.EQUAL, 
			Bytes.toBytes(d.getQueryName())
		);
		list.addFilter(tableFilter);
		
		// Get the column maps
		RegexStringComparator comp = new RegexStringComparator("^" + d.getQueryName() + "_*");   
		SingleColumnValueFilter columnFilter = new SingleColumnValueFilter(
			cf,
			a,
			CompareFilter.CompareOp.EQUAL,
			comp
			);
		list.addFilter(columnFilter);
		
		s.setFilter(list);
		
		ResultScanner ss = table.getScanner(s);
		if (isFormatted) {
			showFormatted(ss);
		} else {
			showRaw(ss, showQuery);
		}
		
	}
	
	static void showRaw(ResultScanner ss, boolean showQuery) {
		
		for(Result r:ss){
			for(KeyValue kv : r.raw()){
				String qualifier = new String(kv.getQualifier());
				
				if (showQuery) {
					if (qualifier.equals("q")) {
						System.out.println(new String(kv.getValue()));
					}
				} else {
					System.out.print(new String(kv.getRow()) + " ");
					//System.out.print(new String(kv.getFamily()) + ":");

					System.out.print(qualifier + " ");
					//System.out.print(kv.getTimestamp() + " ");
					if (!qualifier.equals("q")) {
						 System.out.println(new String(kv.getValue()));
					} else {
						System.out.println("[SQL Query not shown, use -query to see it]");
					}
				}
			}
		}
	}
	
	static void showFormatted(ResultScanner ss) {
		/*
		 * Query Name: Companies
		 * SQL Key: CompanyId
		 * HBase Row Key: CompanyId
		 * 
		 * Columns:
		 * 
		 * Company Name: CompanyName = d.cn
		 *     The name of the company
		 * 
		 * Column 2: ....
		 */
		
		HBaseDescription tableDescription = null;
		List<HBaseDescription> columns = new ArrayList<>();
		for(Result r:ss) {
			HBaseDescription d = HBaseDescription.fromResult(r);
			if (d.getType() ==  null) {
				System.out.println("Missing ty Type");
				continue;
			}
			if (d.getType().equals("Table")) {
				tableDescription = d;
			} else {
				columns.add(d);
			}
		}
		
		if (tableDescription == null) {
			System.out.println("No Table Description Found");
		} else {
			System.out.format("Query Name: %s%n", tableDescription.getQueryName());
			System.out.format("HBase Table: %s%n", tableDescription.getTableName());
			System.out.format("HBase Row Key: %s%n", tableDescription.getSqlKey());
		}
		System.out.println();
		System.out.println("Columns:");
		
		for (HBaseDescription d:columns) {
			HBaseColumn c = d.getHbaseColumn();
			System.out.format("%s: %s = %s.%s%n\t%s%n", 
					c.getLogicalName(), 
					c.getSqlName(), 
					c.getColumnFamily(), 
					c.getQualifier(), 
					c.getDescription());
		}
		
	}
	
	static void save(HTable table, HBaseDescription d) throws Exception {
		
		d.validate();
		
		putDescription(table, d);
		
		System.out.println("Wrote the description to HBase");
	}
	
	/**
	 * Write a SQL->HBase map description to HBase.
	 * 
	 * @param table
	 * @param d
	 * @throws IOException 
	 */
	private static void putDescription(HTable table, HBaseDescription d) throws IOException {
		HBaseColumn c = d.getHbaseColumn();
		HashMap<String, String> values = new HashMap<>();
		 
		values.put("qn", d.getQueryName());
		values.put("hbt", d.getTableName());
		values.put("ty", d.getType());
		
		switch (d.getType()) {
			case "Table":
				values.put("q", d.getQuery());
				values.put("k", d.getSqlKey());
				values.put("hbk", c.getSqlName());
				break;
			case "Column":
				values.put("c", c.getSqlName());
				values.put("hbcf", c.getColumnFamily());
				values.put("hbq", c.getQualifier());
				values.put("hbl", c.getLogicalName());
				values.put("hbd", c.getDescription());
				break;
			default: break;
		}
		
		put(table, d.getRowKey(), "d", values);
	}
	
	private static void put(HTable table, 
			String rowKey, 
			String columnFamily, 
			String attribute, 
			String value) throws IOException {
		
		Put p = new Put(Bytes.toBytes(rowKey));
		
		p.add(Bytes.toBytes(columnFamily), Bytes.toBytes(attribute), 
				Bytes.toBytes(value));
		
		table.put(p);
		
	}
	
	private static void put(HTable table, 
			String rowKey, 
			String columnFamily, 
			HashMap<String, String> values) throws IOException {
		
		Put p = new Put(Bytes.toBytes(rowKey));
		for (Map.Entry<String, String> kvp : values.entrySet()) {
      
			String attribute = kvp.getKey();
			String v = kvp.getValue();
			if (v == null) continue;
			p.add(Bytes.toBytes(columnFamily), Bytes.toBytes(attribute), 
				Bytes.toBytes(v));
			
		}
		
		table.put(p);
		
	}
	
	private static void usage() {
		System.out.println("java HBaseSQLImport.jar");
		System.out.println("\t-qn\tQueryName");
		System.out.println("\t-q\tQueryFile");
		System.out.println("\t-ty\tType (Table or Column)");
		System.out.println("\t-c\tSQL Column Name");
		System.out.println("\t-k\tSQL Key");
		System.out.println("\t-hbt\tHBase Table");
		System.out.println("\t-hbcf\tHBase Column Family");
		System.out.println("\t-hbq\tHBase Attribute");
		System.out.println("\t-hbl\tHBase Logical Column Name");
		System.out.println("\t-hbd\tHBase Column Description");
		System.out.println("");
	}
}
