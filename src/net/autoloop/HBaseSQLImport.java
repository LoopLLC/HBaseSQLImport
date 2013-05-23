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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 *
 * @author ericzbeard
 */
public class HBaseSQLImport {

	/**
	 * @param args the command line arguments
	 */
	public static void main(String[] args) throws Exception {
		//usage();
		
		Configuration config = HBaseConfiguration.create();
		
		HTable table = new HTable(config, "schema");
		
		if (args.length == 0) {
			usage();
			return;
		}
		
		HBaseDescription d = new HBaseDescription();
		HBaseColumn c = new HBaseColumn();
		d.setHbaseColumn(c);
		
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
				case "-c":
					d.setColumn(args[++i]);
					break;
				case "-k":
					d.setSqlKey(args[++i]);
					break;
				case "-hbt":
					c.setTableName(args[++i]);
					break;
				case "-hbcf":
					c.setColumnFamily(args[++i]);
					break;
				case "-hba":
					c.setAttribute(args[++i]);
					break;
				case "-hbk":
					c.setKey(args[++i]);
					break;
				case "-hbl":
					c.setLogicalName(args[++i]);
					break;
				case "-hbd":
					c.setDescription(args[++i]);
					break;
				default: break;
			}
		}
		
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
		values.put("hbt", c.getTableName());
		values.put("hbl", c.getLogicalName());
		values.put("hbd", c.getDescription());
		
		switch (d.getType()) {
			case "Table":
				values.put("q", d.getQuery());
				values.put("k", d.getSqlKey());
				values.put("hbk", c.getKey());
				break;
			case "Column":
				values.put("c", d.getColumn());
				values.put("hbcf", c.getColumnFamily());
				values.put("hba", c.getAttribute());
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
		System.out.println("java hbsqlimp.jar -stuff");
	}
}
