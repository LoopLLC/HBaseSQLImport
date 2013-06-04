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

	private Configuration config;
	private HTable htable;
	private String connectionString;
	private String sqlHost;
	private String sqlDatabase;
	private String sqlUser;
	private String sqlPassword;

	/**
	 * Run the application.
	 */
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
		boolean isSqlGenerate = false;
		boolean isImport = false;
		boolean isSchema = false;
		String schemaPath = null;
		String sqlTable = null;
		String sqlSchema = null;

		// The HBase "schema" table can hold the table 
		// description or the column description.
		// Both have "qn" and "ty", so HBaseDescription + HBaseColumn 
		// are used to represent a row.
		HBaseDescription description = new HBaseDescription();
		HBaseColumn c = new HBaseColumn();
		description.setHbaseColumn(c);

		// Parse command line args
		for (int i = 0; i < args.length; i++) {
			String arg = args[i].toLowerCase();
			switch (arg) {
				case "-qn": // Query Name
					description.setQueryName(args[++i]);
					break;
				case "-q": // Query File
					String queryFile = args[++i];
					byte[] encoded = 
						Files.readAllBytes(Paths.get(queryFile));
					description.setQuery(StandardCharsets.UTF_8
							.decode(ByteBuffer.wrap(encoded)).toString());
					break;
				case "-ty": // Schema row type "Table" or "Column"
					description.setType(args[++i]);
					break;
				case "-k":
					description.setSqlKey(args[++i]);
					break;
				case "-hbt":
					description.setTableName(args[++i]);
					break;
				case "-hbcf":
					c.setColumnFamily(args[++i]);
					break;
				case "-hbq":
					c.setQualifier(args[++i]);
					break;
				case "-c":
					c.setSqlColumnName(args[++i]);
					break;
				case "-t":
					c.setDataType(args[++i]);
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
				case "-sqlg":
					isSqlGenerate = true;
					break;
				case "-sqlh":
					this.sqlHost = args[++i];
					break;
				case "-sqlu":
					this.sqlUser = args[++i];
					break;
				case "-sqldb":
					this.sqlDatabase = args[++i];
					break;
				case "-sqlp":
					this.sqlPassword = args[++i];
					break;
				case "-hbn":
					c.setIsNested(true);
					break;
				case "-import":
					isImport = true;
					break;
				case "-schema":
					isSchema = true;
					schemaPath = args[++i];
					break;
				default: break;
			}
		}

		if (isSqlDescribe || isImport) {
				
			// Get the SQL password if we need to connect to SQL Server.
			// Only do this if the password wasn't already provided.
			if (sqlPassword == null) {
				sqlPassword = readPassword();
			}

			if (this.sqlHost == null || this.sqlUser == null || 
					this.sqlPassword == null || this.sqlDatabase == null) {
				System.out.println("Missing SQL arguments");
				usage();
				return;
			}

			createConnectionString();

		}
		
		if (isSqlDescribe) {
			sqlDescribe(sqlSchema, sqlTable, isSqlGenerate);
			return;
		}
	
		// If we get to here we will need to connect to HBase

		initHbase();
		
		if (isSave) {
			saveMapping(description);
		} else if (isShow) {
			show(description, showQuery, isFormatted);
		} else if (isDelete) {
			deleteMapping(description);
		} else if (isImport) {
			importSQL(description);	
		} else if (isSchema) {
			parseSchemaFile(schemaPath);	
		} else {
			usage();
		}
		
	}

	/**
	 * Initialize HBase configuration.
	 */
	void initHbase() throws Exception {

		this.config = HBaseConfiguration.create();
		
		Logger.getRootLogger().setLevel(Level.WARN);
		
		this.htable = new HTable(config, "schema");
		
	}

	/**
	 * Set the current HBase table against which all operations
	 * will happen.
	 */
	void setHbaseTable(String tableName) throws Exception {
		this.htable = new HTable(config, tableName);
	}

	/**
	 * Parse the JSON schema file and make entries in 
	 * the HBase schema table.
	 */
	void parseSchemaFile(String path) throws Exception {

		// Read the schema file
		byte[] encoded = Files.readAllBytes(Paths.get(path));

		// Convert it to a String
		String json = StandardCharsets.UTF_8
						.decode(ByteBuffer.wrap(encoded)).toString();

		// Set up Gson
		Type collectionType = 
			new TypeToken<Collection<HBaseJsonSchema>>(){}.getType();
		Gson gson = new Gson();

		// Deserialize the file
		Collection<HBaseJsonSchema> list = 
			gson.fromJson(json, collectionType);

		// Save each description to the HBase schema table
		for (HBaseJsonSchema s:list) {
			HBaseDescription d = 
				HBaseHelper.getDescriptionFromJsonSchema(s);

			try {
				d.validate();
			} catch (Exception ex) {
				System.out.println("Invalid description: " + 
						ex.getMessage());
				continue;
			}
			
			System.out.println("Saving " + d.getType() + 
					" " + (d.getType().equals("Table") 
					? d.getTableName() 
					: d.getHbaseColumn().getLogicalName()));

			// Load the SQL query
			if (d.getType().equals("Table")) {
				String queryFile = d.getQuery();

				byte[] encodedSql = 
					Files.readAllBytes(Paths.get(queryFile));
				d.setQuery(StandardCharsets.UTF_8
						.decode(ByteBuffer.wrap(encodedSql)).toString());
			}

			putDescription(d);
		}
	}
	
	/**
	 * Delete an entry from the HBase schema table.
	 */
	void deleteMapping(HBaseDescription description) throws Exception {
		List<Delete> list = new ArrayList< >();
		String rowKey = description.getRowKey();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
        this.htable.delete(list);
	}
	
	/**
	 * Show information about the schema mapping.
	 */
	void show(HBaseDescription description, 
			boolean showQuery, boolean isFormatted) throws Exception {
		
		if (description.getQueryName() == null) {
			System.out.println("Missing -qn QueryName");
			usage();
			return;
		}

		ResultScanner ss = getSchemaScanner(description);
		
		if (isFormatted) {
			showFormatted(ss);
		} else {
			showRaw(ss, showQuery);
		}
		
	}

	/**
	 * Get a scanner that returns all rows matching QueryName.
	 */
	ResultScanner getSchemaScanner(HBaseDescription description) 
		throws Exception {

		Scan s = new Scan();
		byte[] cf = Bytes.toBytes("d");
		byte[] a = Bytes.toBytes("qn");
		
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		
		// Get the row for the table/query/keys
		SingleColumnValueFilter tableFilter = new SingleColumnValueFilter(
			cf,
			a,
			CompareFilter.CompareOp.EQUAL, 
			Bytes.toBytes(description.getQueryName())
		);
		list.addFilter(tableFilter);
		
		// Get the column maps
		RegexStringComparator comp = 
			new RegexStringComparator("^" + 
					description.getQueryName() + "_*");   
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
						System.out.println(
							"[SQL Query not shown, use -query to see it]");
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
			HBaseDescription d = HBaseHelper.getDescriptionFromResult(r);
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
		 * Company Name: CompanyName = d.cn (String)
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
			System.out.format("Query Name: %s%n", 
					tableDescription.getQueryName());
			System.out.format("HBase Table: %s%n", 
					tableDescription.getTableName());
			System.out.format("HBase Row Key: %s%n", 
					tableDescription.getSqlKey());
		}
		System.out.println();
		System.out.println("Columns:");
		
		for (HBaseDescription d:columns) {
			HBaseColumn c = d.getHbaseColumn();
			System.out.format("%s: %s = %s.%s %s (%s) %n\t%s%n", 
					c.getLogicalName(), 
					c.getSqlColumnName(), 
					c.getColumnFamily(), 
					c.getQualifier(), 
					c.getIsNested() ? " (nested) " : "",
					c.getDataType(),
					c.getDescription());
		}
		
	}
	
	/**
	  * Save a schema mapping to HBase.
	  */
	void saveMapping(HBaseDescription description) throws Exception {
		
		description.validate();
		
		putDescription(description);
		
		System.out.println("Wrote the description to HBase");
	}
	
	/**
	 * Write a SQL to HBase map description to HBase.
	 * 
	 * @param d
	 * @throws IOException 
	 */
	private void putDescription(HBaseDescription d) throws IOException {
		
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
				values.put("c", c.getSqlColumnName());
				values.put("t", c.getDataType());
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
	
	/**
	 * Write a single value to HBase.
	 *
	 * Uses this.htable
	 */
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
	
	/**
	 * Write a collection of values to an HBase row.
	 *
	 * Uses this.htable
	 */
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
	 * Get a password from the user, suppressing command line echo.
	 */
	String readPassword() {

		Console cons;
 		char[] passwd;
		String retval = null;
  		if ((cons = System.console()) != null &&
		    (passwd = cons.readPassword("[%s]", 
						"SQL Server Password:")) != null) {
				retval = new String(passwd);	
				java.util.Arrays.fill(passwd, ' ');
		}
		return retval;

	}

	/**
	 * Create the connection string used to connect to SQL Server.
	 */
	void createConnectionString() {

		this.connectionString = "jdbc:sqlserver://" + 
			this.sqlHost + ":1433;databaseName=" + 
			this.sqlDatabase + 
			";user=" + 
			this.sqlUser + 
			";password=" + 
			this.sqlPassword;

	}

	/**
     * Describe a table in SQL Server.
	 * 
	 * Optionally generate SQL queries for the table.
     */
	private void sqlDescribe(String schema, 
			String table, boolean generate) {

		//System.out.println(connectionUrl);

		Connection con = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			con = DriverManager.getConnection(this.connectionString);

			String resourcesPath = "resources/describe.sql";
			InputStream stream = this.getClass()
				.getResourceAsStream(resourcesPath);
			String sql = IOUtils.toString(stream, "UTF-8");

			stmt = con.prepareStatement(sql);
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

			StringBuffer selectSql = new StringBuffer();
			selectSql.append(String.format("select %n")); 

			boolean first = true;
			while (rs.next()) {
				String lenstr = "";
                String length = rs.getString("Length");
                if (!rs.wasNull() && !"".equals(length))
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

				if (generate) {
					selectSql.append("\t");
					if (!first) selectSql.append(",");
					selectSql.append(String.format("%s%n", 
								rs.getString("Name")));
				}
				first = false;
			}

			System.out.println();

			if (generate) {
				selectSql.append(String.format("from %s.%s%n;%n", 
						schema, table));
				System.out.println(selectSql.toString());
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
	private void importSQL(HBaseDescription description) throws Exception {

		// Get the table description and the list of columns
		HBaseDescription tableDescription = null;
		List<HBaseDescription> list = 
			getSchemaColumns(getSchemaScanner(description));
		HashMap<String, HBaseColumn> columns = new HashMap< >();
		for (HBaseDescription d:list) {
			if (d.getType().equals("Table")) {
				tableDescription = d;
			} else {
				HBaseColumn c = d.getHbaseColumn();
				columns.put(c.getSqlColumnName(), c);
			}
		}

		// Switch to the target table
		setHbaseTable(tableDescription.getTableName());

		Connection con = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		// Execute the SQL stored in schema.d.q
		try {

			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			con = DriverManager.getConnection(this.connectionString);

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

				// Make sure we have this column in the HBase schema table
				if (hbColumn == null) {
					System.out.println("Could not find " + columnName + 
							"in HBase schema mapping");
					continue;
				}

				int rsType = rsmd.getColumnType(i);

				// Make sure the java.sql.Types value matches
				if (rsType != HBaseHelper
						.getJavaSqlDataType(hbColumn.getDataType())) {

					System.out.println("Result Set Type " + 
							rsType + 
							" does not match HB schema type " + 
							hbColumn.getDataType() + 
							" for column " + 
						   columnName);

					continue;	

				}

				System.out.println("Adding " + columnName + 
						" to column name list");

				columnNames.put(columnName, rsType);
			}

			int totalRowsSaved = 0;

			while (rs.next()) {

				// The row key for the row in HBase is the value of the 
				// field with the column name "sqlKey" from the HBase
				// schema mapping table description.  e.g. in the Companies
				// query, CompanyId is designated as the SQL Key for 
				// each row.
				// TODO - Handle composite keys.  
				// If sqlKey is something like:
				// {CompanyId}_{NotificationRunId}_{NotificationId}, the 
				// row key will be something like 1_Guid_Guid.

				String sqlKey = tableDescription.getSqlKey();
				String rowKey = rs.getString(sqlKey);
				
				if (rs.wasNull()) {
					throw new Exception("The field for sqlKey " + 
							sqlKey + 
							" was null");
				}

				byte[] rowKeyBytes = Bytes.toBytes(rowKey);
				Put p = new Put(rowKeyBytes);

				// Use mapping information to decide 
				// where to put each field value
				for (Map.Entry<String, Integer> kvp : 
						columnNames.entrySet()) {
			  
					String columnName = kvp.getKey();
					int columnType = kvp.getValue();
					HBaseColumn hbColumn = columns.get(columnName);
					String columnFamily = hbColumn.getColumnFamily();
					String qualifier = hbColumn.getQualifier();

					byte[] hbaseValue = convertSqlValueToHBase(
								columnType, 
								rs, 
								columnName);

					if (hbaseValue == null || hbaseValue.length == 0) {

						// Delete the value to represent NULL
						Delete del = new Delete(rowKeyBytes);
						del.deleteColumns(Bytes.toBytes("d"), 
								Bytes.toBytes(qualifier));

					} else {

						// Add the value to the list of values to 
						// be saved to the HBase row.
						p.add(  Bytes.toBytes(columnFamily), 
								Bytes.toBytes(qualifier), 
								hbaseValue);
					}

				}

				this.htable.put(p);
				totalRowsSaved++;
			}
			
			System.out.println("Done.  Saved " + totalRowsSaved + " rows");

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
	 * Output usage to the console.
	 */
	private void usage() {
		System.out.println("java -jar dist/HBaseSQLImport.jar");
		System.out.println("\t-qn\tQueryName");
		System.out.println("\t-q\tQueryFile");
		System.out.println("\t-ty\tType (Table or Column)");
		System.out.println("\t-c\tSQL Column Name");
		System.out.println("\t-t\tData Type");
		System.out.println("\t\tstring, int, boolean, byte, long");
		System.out.println("\t\tdouble, float, datetime");
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
		System.out.println();

		System.out.println("\t-sqld\tDescribe a table in SQL Server");
		System.out.println("\t-sqlh\tSQL Host");
		System.out.println("\t-sqldb\tSQL Database Name");
		System.out.println("\t-sqlu\tSQL Username");
		System.out.println();
		System.out.println("\t-schema JSON Schema File");
		System.out.println("");
	}
}
