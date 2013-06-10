package net.autoloop;

import java.util.*;
import java.io.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.io.*;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.sql.*;

import com.google.gson.*;
import com.google.gson.reflect.*;

import java.lang.reflect.*;

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
	public static HBaseDescription 
		getDescriptionFromJsonSchema(HBaseJsonSchema j) {

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
			case "int": return java.sql.Types.INTEGER; // int
			case "long": return java.sql.Types.BIGINT; // long
			case "string": return java.sql.Types.VARCHAR; // String
			case "nstring": return java.sql.Types.NVARCHAR; // String
			case "double": return java.sql.Types.DOUBLE; // double
			case "float": return java.sql.Types.FLOAT; // float
			case "boolean": return java.sql.Types.BIT; // boolean
			case "byte": return java.sql.Types.TINYINT; // byte
			case "datetime": return java.sql.Types.TIMESTAMP; // Date (long)
			case "guid": return java.sql.Types.CHAR; // String
			case "short": return java.sql.Types.SMALLINT; // short
			default: return 0;
		}
		// TODO - Convert this to a dictionary

		// TODO - Document "HBSQLI Type" - "Java Type" - "SQL Type"

		// A Java type or SQL type could appear multiple times, 
		// depending on how we want to interpret the value.

		// e.g. "int" = "Java int" = "sql INTEGER"
		//		"int2" = "Java int" = "sql SMALLINT"
	}

	/**
	 * Convert a scan result to a dictionary object.
	 */
	public static HBaseDictionary getDictionaryFromResult(Result r) 
		throws Exception {

		HBaseDictionary d = new HBaseDictionary();

		for (KeyValue kv:r.raw()) {
			String qualifier = new String(kv.getQualifier());
			String value = new String(kv.getValue());
			switch (qualifier) {
				case "table": 
					d.setTable(value);
					break;
				case "family": 
					d.setFamily(value);
					break;
				case "qualifier": 
					d.setQualifier(value);
					break;
				case "name": 
					d.setName(value);
					break;
				case "description": 
					d.setDescription(value);
					break;
				case "nested": 
					d.setNested(value);
					break;
				case "type": 
					d.setType(value);
					break;
				default: break;
			}
		}

		return d;
	}

	public static String makeSortableString(int val) {
		int maxWidth = ("" + Integer.MAX_VALUE).length();
		return padLeft(("" + val), maxWidth, '0');
	}

	public static String makeSortableString(byte val) {
		int maxWidth = ("" + Byte.MAX_VALUE).length();
		return padLeft(("" + val), maxWidth, '0');
	}

	public static String makeSortableString(short val) {
		int maxWidth = ("" + Short.MAX_VALUE).length();
		return padLeft(("" + val), maxWidth, '0');
	}

	public static String makeSortableString(long val) {
		int maxWidth = ("" + Long.MAX_VALUE).length();
		return padLeft(("" + val), maxWidth, '0');
	}

	public static String padLeft(String str, 
			int totalLength, char padWith) {
		return pad(str, totalLength, padWith, true);
	}

	public static String padRight(String str, 
			int totalLength, char padWith) {
		return pad(str, totalLength, padWith, false);
	}

	/**
	 * Pad a string.
	 */
	public static String pad(String str, 
			int totalLength, char padWith, boolean isLeft) {

		if (str == null) return str;

		if (str.length() >= totalLength) return str;

		int remainingLength = totalLength - str.length();

		StringBuilder sb = new StringBuilder();

		if (!isLeft) sb.append(str);

		for (int i = 0; i < remainingLength; i++) {
			sb.append(padWith);
		}

		if (isLeft) sb.append(str);

		return sb.toString();
	}

	public static void test() throws Exception {
		
		String abc = pad("abc", 5, '0', true);
		if (!abc.equals("00abc")) {
			throw new Exception(abc + " should be 00abc");
		}

		// 127
		// 000

		// 32767
		// 00000

		// 2147483647
		// 0000000000

		// 9223372036854775807
		// 0000000000000000000

		byte oneHundredByte = (byte)100;
		String bb = makeSortableString(oneHundredByte);
		System.out.println(bb);
		if (!bb.equals("100")) {
			throw new Exception(
					"makeSortableString(oneHundredByte) should be 100");
		}

		short tenShort = (short)10;
		String ss = makeSortableString(tenShort);
		System.out.println(ss);
		if (!ss.equals("00010")) {
			throw new Exception(
					"makeSortableString(tenShort) should be 00010");
		}

		int fiveInt = (int)5;
		String ii = makeSortableString(fiveInt);
		System.out.println(ii);
		if (!ii.equals("0000000005")) {
			throw new Exception(
					"makeSortableString(fiveInt) should be 0000000005");
		}

		long fiveNinesLong = (long)99999;
		String ll = makeSortableString(fiveNinesLong);
		System.out.println(ll);
		if (!ll.equals("0000000000000099999")) {
			throw new Exception(
					"makeSortableString(fiveNinesLong) should be " + 
					"0000000000000099999");
		}

		System.out.println("Padding tests passed");

		// TODO - Test template tokens

		// static String[] getTokensFromTemplate(String template) {

		String template = "cpn_{SlotNumber}_Name";

		template = "{CompanyId}_{NotificationRunId}_{NotificationId}";

		template = "{First_Name}_{Last_Name}";

		template = "CompanyId";

		template = "First_Name";

	}

	/**
	 * Convert a value from the SQL result set to a byte array
	 * for the HBase put.
	 *
	 * If the SQL value was NULL, an empty byte array is returned.
	 */
	public static byte[] convertSqlValueToHBase(
			int columnType, 
			ResultSet rs, 
			String columnName) throws Exception {

		byte[] nullArray = new byte[0];

		// TODO - Seems like there should be a less verbose way to do this.

		switch (columnType) {
			case Types.INTEGER:
				int i = rs.getInt(columnName);
				if (rs.wasNull()) {
					return nullArray;
				}
				return Bytes.toBytes(i);
			case Types.SMALLINT:
				int sh = rs.getShort(columnName);
				if (rs.wasNull()) {
					return nullArray;
				}
				return Bytes.toBytes(sh);
			case Types.BIGINT:
				long l = rs.getLong(columnName);
				if (rs.wasNull()) {
					return nullArray;
				}
				return Bytes.toBytes(l);
			case Types.VARCHAR:
			case Types.NVARCHAR:
			case Types.CHAR:
				String s = rs.getString(columnName);
				if (rs.wasNull()) {
					return nullArray;
				}
				return Bytes.toBytes(s);
			case Types.BIT:
			case Types.BOOLEAN:
				boolean b = rs.getBoolean(columnName);
				if (rs.wasNull()) {
					return nullArray;
				}
				return Bytes.toBytes(b);
			case Types.DOUBLE:
				double d = rs.getDouble(columnName);
				if (rs.wasNull()) {
					return nullArray;
				}
				return Bytes.toBytes(d);
			case Types.FLOAT:
				float f = rs.getFloat(columnName);
				if (rs.wasNull()) {
					return nullArray;
				}
				return Bytes.toBytes(f);
			case Types.TINYINT:
				byte by = rs.getByte(columnName);
				if (rs.wasNull()) {
					return nullArray;
				}
				byte[] ba = new byte[1];
				ba[0] = by;
				return ba;
			case Types.TIMESTAMP:
				java.util.Date date = rs.getDate(columnName);
				if (rs.wasNull()) {
					return nullArray;
				}
				return Bytes.toBytes(date.getTime()); // long
			default: throw new Exception(
						"Unexpected SQL type: " + columnType);
		}	
	}

	/**
	 * Get the column qualifer or row key based on a template.
	 *
	 * Makes variable substitutions for anything in {braces}.
	 *
	 * Tokens must be separated by underscores.
	 *
	 * e.g. cpn_{SlotNumber}_Name
	 * e.g. {CompanyId}_{NotificationRunId}_{NotificationId}
	 *
	 * If subBracesOnly is true, values not in braces will 
	 * be left as-is and not substituted for ResultSet values.
	 *
	 * Setting subBracesOnly to false is for row keys that 
	 * are simply specified as the column name.
	 *
	 */
	public static String getCompositeValue(
			HashMap<String, Integer> columnNames, 
			ResultSet rs, 
			String template, 
			boolean subBracesOnly) throws Exception {

		StringBuffer sb = new StringBuffer();

		// Break the template into tokens.
		String[] tokens = getTokensFromTemplate(template);
		
		boolean first = true;
		for (String token:tokens) {
		
			// Put the underscore back in between tokens	
			if (first) first = false;
			else sb.append("_");

			String colName = token;

			if (token.startsWith("{")) {

				// First make sure it's valid.
				if (!token.endsWith("}")) {
					throw new Exception(
						"Unexpected token: " + token);
				}
				
				// Strip the braces
				colName = token.replace("{", "").replace("}", "");

			} else {
			
				if (subBracesOnly) {
					// This is a non-variable token, part of a qualifier
					// e.g. cpn_{SlotNumber}_Name
					// cpn and Name are not variables, so the end result
					// will be something like cpn_001_Name
					sb.append(token);
					continue;
				}
			
			}

			// Get the java.sql.Type for this column
			int columnType = columnNames.get(colName);

			// Get the value from the result set
			String val = getPaddedValueFromResultSet(
					columnType, rs, colName);

			// Make sure it wasn't NULL
			if (val == null) {
				// This shouldn't happen.  If it does, the 
				// query needs to be re-written to make sure 
				// composite keys will always have values.
				throw new Exception(
					"Result Set value for " + 
					colName + 
					" was null.  It is part of a composite key.");
			}
			
			// Add it to the qualifier
			sb.append(val);

		}

		return sb.toString();
	}

	/**
	 * Break a row key or qualifier template into tokens.
	 *
	 * Tokens must be separated by underscores.
	 *
	 * Underscores inside braces do not count as token dividers.
	 *
	 * Braces are retained in the returned values.
	 *
	 * cpn_{SlotNumber}_Name
	 *	[0] cpn
	 *	[1] {SlotNumber}
	 *  [2] Name
	 *
	 * {First_Name}_{Last_Name}
	 * 	[0] {First_Name}
	 *	[1] {Last_Name}
	 */
	static String[] getTokensFromTemplate(String template) {
		String[] tokens = template.split("_");

		List<String> retval = new ArrayList<>();

		// TODO

		return retval.toArray(new String[retval.size()]);
	}
	
	/**
	 * Pads numbers, leaves strings alone, and throws Exceptions
	 * for any other type.
	 *
	 * Guids are CHAR, so they get treated like strings and left alone.
	 */
	static String getPaddedValueFromResultSet(int columnType, 
			ResultSet rs, String columnName) throws Exception {

		switch (columnType) {
			case Types.INTEGER:
				int i = rs.getInt(columnName);
				if (rs.wasNull()) {
					return null;
				}
				return makeSortableString(i);
			case Types.SMALLINT:
				int sh = rs.getShort(columnName);
				if (rs.wasNull()) {
					return null;
				}
				return makeSortableString(sh);
			case Types.BIGINT:
				long l = rs.getLong(columnName);
				if (rs.wasNull()) {
					return null;
				}
				return makeSortableString(l);
			case Types.TINYINT:
				byte b = rs.getByte(columnName);
				if (rs.wasNull()) {
					return null;
				}
				return makeSortableString(b);
			case Types.VARCHAR:
			case Types.NVARCHAR:
			case Types.CHAR:
				String s = rs.getString(columnName);
				if (rs.wasNull()) {
					return null;
				}
				return s;
			default: throw new Exception(
						"Unexpected SQL type: " + columnType);
		}

	}

	/**
	 * Get the HBase row key value from the ResultSet, 
	 * using the sqlKey template.
	 *
	 * The sqlKey might be something simple like "CompanyId", 
	 * in which case we just get CompanyId from the ResultSet.
	 *
	 * But it might be composite, like {CompanyId}_{NotificationRunId}
	 *
	 * Deprecated.
	 */
	String getRowKeyFromResultSet(String sqlKey, ResultSet rs) 
		throws Exception {

		String[] tokens = getRowKeyTokens(sqlKey);
		StringBuffer sb = new StringBuffer();
		boolean first = true;
		for (String token:tokens) {
		 	if (first) {
				first = false;
			} else {
				sb.append("_");
			}

			// TODO - Need to pad numbers so that sorting works!

			sb.append(rs.getString(token));	
			
			if (rs.wasNull()) {
				throw new Exception("The field for sqlKey " + 
						sqlKey + 
						" token " + 
						token + 
						" was null");
			}
		}
		return sb.toString();

	}

	/**
	 * Split the sqlKey into row key tokens.
	 *
	 * Deprecated.
	 */
	String[] getRowKeyTokens(String sqlKey) {

		String[] tokens = sqlKey.split("\\}_\\{");
		for (int i = 0; i < tokens.length; i++) {
			tokens[i] = tokens[i].replace("{", "").replace("}", "");
		}
		return tokens;

	}

}

