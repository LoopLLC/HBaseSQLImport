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

import java.sql.*;

/**
 *
 * @author ericzbeard
 */
public class HBaseSQLImport {

	/**
	 * @param args the command line arguments
	 */
	public static void main(String[] args) throws Exception {

		HBaseSQLImport hbsqli = new HBaseSQLImport();
		hbsqli.run(args);
	
	}

	public HBaseSQLImport() {}

	private HTable htable;
 	private HBaseDescription d;

	void run(String[] args) throws Exception {
		
		if (args.length == 0) {
			usage();
			return;
		}

		boolean isSave = false;
		boolean isShow = false;
		boolean showQuery = false;
		boolean isDelete = false;
		boolean isFormatted = false;
		boolean isSqlDescribe = false;
		boolean isImport = false;
		String sqlTable = null;
		String sqlSchema = null;

		this.d = new HBaseDescription();
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
				case "-sqld":
					isSqlDescribe = true;
					sqlSchema = args[++i];
					sqlTable = args[++i];
					break;
				case "-hbn":
					c.setIsNested(true);
					break;
				case "-import":
					isImport = true;
					break;
				default: break;
			}
		}
		
		if (isSqlDescribe) {
			sqlDescribe(sqlSchema, sqlTable); // TODO - Connection string
			return;
		}
	
		initHbase();
		
		if (isSave) {
			saveMapping();
		} else if (isShow) {
			show(showQuery, isFormatted);
		} else if (isDelete) {
			deleteMapping();
		} else if (isImport) {
			importSQL();	
		} else {
			usage();
		}
		
	}

	void initHbase() throws Exception {

		Configuration config = HBaseConfiguration.create();
		
		Logger.getRootLogger().setLevel(Level.WARN);
		
		this.htable = new HTable(config, "schema");
		
	}
	
	void deleteMapping() throws Exception {
		List<Delete> list = new ArrayList< >();
		String rowKey = d.getRowKey();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
        this.htable.delete(list);
	}
	
	/**
	  * Show information about the schema mapping.
	  */
	void show(boolean showQuery, boolean isFormatted) throws Exception {
		
		if (d.getQueryName() == null) {
			System.out.println("Missing -qn QueryName");
			usage();
			return;
		}

		ResultScanner ss = getSchemaScanner();
		
		if (isFormatted) {
			showFormatted(ss);
		} else {
			showRaw(ss, showQuery);
		}
		
	}

	/**
	  * Get a scanner that returns all rows matching QueryName.
	  */
	ResultScanner getSchemaScanner() throws Exception {

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
		RegexStringComparator comp = 
			new RegexStringComparator("^" + d.getQueryName() + "_*");   
		SingleColumnValueFilter columnFilter = new SingleColumnValueFilter(
			cf,
			a,
			CompareFilter.CompareOp.EQUAL,
			comp
			);
		list.addFilter(columnFilter);
		
		s.setFilter(list);
		
		ResultScanner ss = this.htable.getScanner(s);
		return ss;
	}
	
	/**
	  * Show raw information for the rows matching QueryName.
	  */
	void showRaw(ResultScanner ss, boolean showQuery) {
		
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

	/**
	  * Get all schema columns matching the QueryName.
	  */
	List<HBaseDescription> getSchemaColumns(ResultScanner ss) {

		List<HBaseDescription> columns = new ArrayList< >();
		for(Result r:ss) {
			HBaseDescription d = HBaseDescription.fromResult(r);
			if (d.getType() ==  null) {
				System.out.println("Missing ty Type");
				continue;
			}
			columns.add(d);
		}
		return columns;

	}
	
	/**
	  * Show formatted information for the rows matching QueryName.
	  */
	void showFormatted(ResultScanner ss) {
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
		List<HBaseDescription> list = getSchemaColumns(ss);
		List<HBaseDescription> columns = new ArrayList< >();
		for (HBaseDescription d:list) {
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
			System.out.format("%s: %s = %s.%s %s%n\t%s%n", 
					c.getLogicalName(), 
					c.getSqlName(), 
					c.getColumnFamily(), 
					c.getQualifier(), 
					c.getIsNested() ? " (nested) " : "",
					c.getDescription());
		}
		
	}
	
	/**
	  * Save a schema mapping to HBase.
	  */
	void saveMapping() throws Exception {
		
		d.validate();
		
		putDescription();
		
		System.out.println("Wrote the description to HBase");
	}
	
	/**
	 * Write a SQL to HBase map description to HBase.
	 * 
	 * @param d
	 * @throws IOException 
	 */
	private void putDescription() throws IOException {
		
		HBaseColumn c = d.getHbaseColumn();
		HashMap<String, String> values = new HashMap< >();
		 
		values.put("qn", d.getQueryName());
		values.put("hbt", d.getTableName());
		values.put("ty", d.getType());
		
		switch (d.getType()) {
			case "Table":
				values.put("q", d.getQuery());
				values.put("k", d.getSqlKey());
				break;
			case "Column":
				values.put("c", c.getSqlName());
				values.put("hbcf", c.getColumnFamily());
				values.put("hbq", c.getQualifier());
				values.put("hbl", c.getLogicalName());
				values.put("hbd", c.getDescription());
				values.put("hbn", c.getIsNested() ? "true" : "false");
				break;
			default: break;
		}
		
		put(d.getRowKey(), "d", values);
	}
	
	private void put( 
			String rowKey, 
			String columnFamily, 
			String qualifier, 
			String value) throws IOException {
		
		Put p = new Put(Bytes.toBytes(rowKey));
		
		p.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), 
				Bytes.toBytes(value));
		
		this.htable.put(p);
		
	}
	
	private void put( 
			String rowKey, 
			String columnFamily, 
			HashMap<String, String> values) throws IOException {
		
		Put p = new Put(Bytes.toBytes(rowKey));
		for (Map.Entry<String, String> kvp : values.entrySet()) {
      
			String qualifier = kvp.getKey();
			String v = kvp.getValue();
			if (v == null) continue;
			p.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), 
				Bytes.toBytes(v));
			
		}
		
		this.htable.put(p);
		
	}

	/**
     * Describe a table in SQL Server.
     */
	private void sqlDescribe(String schema, String table) {

		String connectionUrl = "jdbc:sqlserver://engineering25:1433;"
				+ "databaseName=LoopEzb_A_01;user=hadoop;password=password";

		Connection con = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			con = DriverManager.getConnection(connectionUrl);

			byte[] encoded = Files.readAllBytes(Paths.get("sql/describe.sql"));
			String SQL = StandardCharsets.UTF_8
							.decode(ByteBuffer.wrap(encoded)).toString();
					
			stmt = con.prepareStatement(SQL);
			stmt.setString(1, schema);
			stmt.setString(2, table);
			rs = stmt.executeQuery();

			ResultSetMetaData rsmd = rs.getMetaData();
			int numColumns = rsmd.getColumnCount();
			List<String> columnNames = new ArrayList< >();
			for (int i = 1 ; i <= numColumns; i++) {
				columnNames.add(rsmd.getColumnName(i));
			}
			
			System.out.format("%s.%s%n", schema, table);

			while (rs.next()) {
				String lenstr = "";
                String length = rs.getString("Length");
                if (!rs.wasNull())
                {
                  lenstr = "(" + length + ")";
                }
                boolean isPk = rs.getBoolean("IsPrimaryKey");
                System.out.format("\t%s %s %s %s %s%n", 
                        rs.getString("Name"), 
                        rs.getString("Type"), 
                        lenstr, 
                        rs.getString("Nullable"), 
                        isPk ? "PK" : "");
			}

		} 
		catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception e) {
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
				}
			}
		}

	}

	/**
	 * Run the import SQL and write the data to HBase.
	 */
	private void importSQL() throws Exception {

		// Get the table description and the list of columns
		HBaseDescription tableDescription = null;
		List<HBaseDescription> list = getSchemaColumns(getSchemaScanner());
		HashMap<String, HBaseColumn> columns = new HashMap< >();
		for (HBaseDescription d:list) {
			if (d.getType().equals("Table")) {
				tableDescription = d;
			} else {
				HBaseColumn c = d.getHbaseColumn();
				columns.put(c.getSqlName(), c);
			}
		}

		// TODO - Specify cx on command line
		String connectionUrl = "jdbc:sqlserver://engineering25:1433;"
				+ "databaseName=LoopEzb_A_01;user=hadoop;password=password";

		System.out.format("Using connectionUrl: %s%n", 
				connectionUrl);

		Connection con = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		// Execute the SQL stored in schema.d.q
		try {

			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			con = DriverManager.getConnection(connectionUrl);

			String sql = tableDescription.getQuery(); 

			System.out.format("Running the following query:%n%s%n", sql);
					
			stmt = con.prepareStatement(sql);
			rs = stmt.executeQuery();

			// Ask the result set for column meta-data, and cross-check 
			// this against the mappings that are stored in HBase.
			ResultSetMetaData rsmd = rs.getMetaData();
			int numColumns = rsmd.getColumnCount();
			HashMap<String, Integer> columnNames = new HashMap< >();
			for (int i = 1 ; i <= numColumns; i++) {
				String columnName = rsmd.getColumnName(i);
				HBaseColumn hbColumn = columns.get(columnName);
				if (hbColumn == null) {
					System.out.println("Could not find " + columnName + 
							"in HBase schema mapping");
				} else {
					System.out.println("Adding " + columnName + 
							" to column name list");
					columnNames.put(columnName, rsmd.getColumnType(i));
				}
			}

			while (rs.next()) {

				// The row key for the row in HBase is the value of the 
				// field with the column name "sqlKey" from the HBase
				// schema mapping table description.  e.g. in the Companies
				// query, CompanyId is designated as the SQL Key for each row.
				// TODO - Handle composite keys.  If sqlKey is something like:
				// {CompanyId}_{NotificationRunId}_{NotificationId}, the 
				// row key will be something like 1_Guid_Guid.
				String sqlKey = tableDescription.getSqlKey();
				String rowKey = rs.getString(sqlKey);
				
				if (rs.wasNull()) {
					throw new Exception("The field for sqlKey " + 
							sqlKey + 
							" was null");
				}

				Put p = new Put(Bytes.toBytes(rowKey));

				// Use mapping information to decide where to put each field value
				for (Map.Entry<String, Integer> kvp : columnNames.entrySet()) {
			  
					String columnName = kvp.getKey();
					int columnType = kvp.getValue();
					HBaseColumn hbColumn = columns.get(columnName);
					String columnFamily = hbColumn.getColumnFamily();
					String qualifier = hbColumn.getQualifier();

					p.add(  Bytes.toBytes(columnFamily), 
							Bytes.toBytes(qualifier), 
							convertSqlValueToHBase(
								columnType, 
								rs, 
								columnName) );

					// TODO - If the value is null, what should we do?
					// Do a delete instead to remove the value?
				}
				this.htable.put(p);
			}
			

		} 
		catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception e) {
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
				}
			}
		}


	}

	/**
	 * Convert a value from the SQL result set to a byte array
	 * for the HBase put.
	 *
	 * If the SQL value was NULL, an empty byte array is returned.
	 */
	byte[] convertSqlValueToHBase(
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
			case Types.BIGINT:
				long l = rs.getLong(columnName);
				if (rs.wasNull()) {
					return nullArray;
				}
				return Bytes.toBytes(l);
			case Types.VARCHAR:
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
			default: throw new Exception("Unexpected SQL type: " + columnType);
		}	
	}
	
	private void usage() {
		System.out.println("java HBaseSQLImport.jar");
		System.out.println("\t-qn\tQueryName");
		System.out.println("\t-q\tQueryFile");
		System.out.println("\t-ty\tType (Table or Column)");
		System.out.println("\t-c\tSQL Column Name");
		System.out.println("\t-k\tSQL Key");
		System.out.println("\t-hbt\tHBase Table");

		System.out.println("\t-save\tSave a mapping description");
		
		System.out.println("\t\t-hbcf\tHBase Column Family");
		System.out.println("\t\t-hbq\tHBase Attribute");
		System.out.println("\t\t-hbl\tHBase Logical Column Name");
		System.out.println("\t\t-hbd\tHBase Column Description");
		System.out.println("\t\t-hbn\tA nested column");

		System.out.println("\t-show\tShow a mapping description");
		System.out.println("\t-format\tFormat the output from -show");
		System.out.println("\t-sqld\tDescribe a table in SQL Server");
		System.out.println("");
	}
}
