package net.autoloop;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Types;

public class HBaseHelper {

	/**
	 * Convert a row from the schema table to a description object.
	 */
	public static HBaseDescription getDescriptionFromResult(Result r) {

		HBaseDescription d = new HBaseDescription();
		HBaseColumn c = new HBaseColumn();
		d.setHbaseColumn(c);
		
		/*
		 *  
			Companies hbt company
			Companies k CompanyId
			Companies q [SQL Query not shown, use -query to see it]
			Companies qn Companies
			* 
			* or
			* 
			Companies_CompanyName c CompanyName
			Companies_CompanyName t 12
			Companies_CompanyName hbcf d
			Companies_CompanyName hbd The name of the company
			Companies_CompanyName hbl CompanyName
			Companies_CompanyName hbq cn
			Companies_CompanyName hbt company
			Companies_CompanyName qn Companies
		 */
		
		String rowKey = new String(r.getRow());
		
		for (KeyValue kv:r.raw()) {
			String qualifier = new String(kv.getQualifier());
			String value = new String(kv.getValue());
			switch (qualifier) {
				case "qn": // Query Name
					d.setQueryName(value);
					break;
				case "q": // The SQL query
					d.setQuery(value);
					break;
				case "k":
					d.setSqlKey(value);
					break;
				case "hbt":
					d.setTableName(value);
					break;
				case "hbcf":
					c.setColumnFamily(value);
					break;
				case "hbq":
					c.setQualifier(value);
					break;
				case "c":
					c.setSqlColumnName(value);
					break;
				case "t":
					c.setDataType(value);
					break;
				case "hbl":
					c.setLogicalName(value);
					break;
				case "hbd":
					c.setDescription(value);
					break;
				case "ty":
					d.setType(value);
					break;
				case "hbn":
					if ("true".equals(value)) {
						c.setIsNested(true);
					} else {
						c.setIsNested(false);
					}
					break;
				default: break;
			}
		}
		
		return d;
	}

	/**
	 * Convert a JSON facade object to a description object.
	 */
	public static HBaseDescription getDescriptionFromJsonSchema(HBaseJsonSchema j) {

		HBaseDescription d = new HBaseDescription();
		HBaseColumn c = new HBaseColumn();
		d.setHbaseColumn(c);

		d.setQueryName(j.qn);
		d.setQuery(j.q);
		d.setSqlKey(j.k);
		d.setTableName(j.hbt);
		d.setType(j.ty);

		c.setColumnFamily(j.hbcf);
		c.setQualifier(j.hbq);
		c.setSqlColumnName(j.c);
		c.setDataType(j.t);
		c.setLogicalName(j.hbl);
		c.setDescription(j.hbd);
		if ("true".equals(j.hbn)) {
			c.setIsNested(true);
		} else {
			c.setIsNested(false);
		}
		
		return d;	
	}

	/**
	 * Get the java sql type that corresponds to the HBase column type name.
	 */
	public static int getJavaSqlDataType(String dataType) {
		switch (dataType) {
			case "int": return java.sql.Types.INTEGER;
			case "long": return java.sql.Types.BIGINT;
			case "String": return java.sql.Types.VARCHAR;
			case "double": return java.sql.Types.DOUBLE;
			case "float": return java.sql.Types.FLOAT;
			case "boolean": return java.sql.Types.BIT;
			case "byte": return java.sql.Types.TINYINT;
			default: return 0;
		}
		// TODO - Convert this to a dictionary
	}
}

