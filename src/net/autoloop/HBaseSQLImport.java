package net.autoloop;

import java.util.*;
import java.io.*;

import java.io.IOException;
import java.nio.ByteBuffer;
 
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
public class HBaseSQLImport implements Runnable {

	/**
	 * @param args the command line arguments
	 */
	public static void main(String[] args) throws Exception {

		// Check to see if this is a "multi" invocation, where
		// we read a file with multiple imports and then run
		// them at the same time on separate threads.
		String multiFile = null;
		String sqlu = null;
		String sqlp = null;
		for (int i = 0; i < args.length; i++) {
			String arg = args[i].toLowerCase();
			if (arg.equals("-multi")) {
				multiFile = args[++i];
				continue;
			}
			if (arg.equals("-sqlu")) {
				sqlu = args[++i];
				continue;
			}
			if (arg.equals("-sqlp")) {
				sqlp = args[++i];
				continue;
			}
		}

		if (multiFile != null) {
			
			List<HBaseSQLThread> threads = new ArrayList<HBaseSQLThread>();
		
			// Open the JSON file and read the arguments for each 
			// import, then run them all concurrently.

			// Read the schema file
			String json = HBaseHelper.readFile(multiFile);

			// The schema file looks like this and is 
			// represented by net.autoloop.HBaseMulti
			//
			// [
			// {
			// 		"qn": "ShortURL", 
			// 		"sqlh": "windows1", 
			// 		"sqldb": "SurlEzb_00"
			// }
			// ...
			// ]
			//
			
			// All databases need to be using the same username/password
			// so we can read them once from the command line 
			// securely instead of embedding it in the JSON.

			// Read the password securely now if it wasn't provided
			if (sqlp == null) {
				sqlp = readPassword();
			}

			if (sqlu == null) {
				throw new Exception("-sqlu required for -multi");
			}

			// Set up Gson
			Type collectionType = 
				new TypeToken<Collection<HBaseMulti>>(){}.getType();
			Gson gson = new Gson();

			// Deserialize the file
			Collection<HBaseMulti> list = 
				gson.fromJson(json, collectionType);

			for (HBaseMulti multi : list) {

				HBaseSQLImport hbsqli = new HBaseSQLImport();
				
				List<String> argList = new ArrayList<String>();
				for (int i = 0; i < args.length; i++) {
					String a = args[i].toLowerCase();
					if (a.equals("-qn") || 
						a.equals("-sqlh") || 
						a.equals("-sqldb")) {
						throw new Exception(
								"Invalid arg with -multi: " + 
								args[i]);
						}
					argList.add(args[i]);
				}
				
				argList.add("-qn");
				argList.add(multi.qn);

				argList.add("-sqlh");
				argList.add(multi.sqlh);

				argList.add("-sqldb");
				argList.add(multi.sqldb);

				argList.add("-sqlu");
				argList.add(sqlu);

				argList.add("-sqlp");
				argList.add(sqlp);

				// Set the altered args so that this instance 
				// can run as if it were launched in the same 
				// way from the command line.
				hbsqli.args = argList.toArray(
						new String[argList.size()]);

				hbsqli.prefix = multi.qn + "." + 
					multi.sqlh + "." + multi.sqldb + " ";

				// Add the thread to a list so we can join them all later
				HBaseSQLThread t = new HBaseSQLThread();
				t.hbsqli = hbsqli;
				threads.add(t);
	
				// Start the thread
				t.start();
			}

			// Wait for all threads to complete
			for (HBaseSQLThread t : threads) {
				t.join();
			}

		} else {

			// This is a regular invocation that does one import
			HBaseSQLImport hbsqli = new HBaseSQLImport();
			hbsqli.prefix = "";
			hbsqli.args = args;
			hbsqli.run();

		}

	}

	/**
	 * This class is used to run several hbsqli instances at once.
	 */
	public static class HBaseSQLThread extends Thread {
		
		public HBaseSQLImport hbsqli;

		public void run() {
			hbsqli.run();
		}
	}

	public HBaseSQLImport() {}

	public HBaseSQLImport(String[] args) {
		this.args = args;
	}

	/**
	 * Command line arguments.
	 */
	public String[] args;

	/**
	 * Console output needs to be prefixed so we know which
	 * one is reporting when it's -multi.  qn.sqlh.sqldb 
	 */
	protected String prefix = "";

	public static boolean s_isVerbose = false;

	private Configuration config;
	private HTable schemaTable;
	private HTable dictionaryTable;
	private String connectionString;
	private String sqlHost;
	private String sqlDatabase;
	private String sqlUser;
	private String sqlPassword;

	/**
	 * Run the application (Runnable implementation).
	 *
	 * Requires command line args to be copied to this.args.
	 */
	public void run() {
		
		try {
			runInstance();
		} catch (Exception ex) {
			System.err.println(this.prefix);
			ex.printStackTrace();
		}
	}

	protected void runInstance() throws Exception {

		if (args.length == 0) {
			usage();
			return;
		}

        if (this.prefix != null && !this.prefix.equals(""))
    		System.out.println(this.prefix + "starting...");

		boolean isSave = false;
		boolean isShow = false;
		boolean showQuery = false;
		boolean isDelete = false;
		boolean isFormatted = false;
		boolean isSqlDescribe = false;
		boolean isSqlGenerate = false;
		boolean isImport = false;
		boolean isSchema = false;
		boolean isTest = false;
		boolean isShowDictionary = false;
		boolean isScan = false;
		boolean isGet = false;
		boolean isExamples = false;
		boolean isDaily = false;
		String scanFilter = null;
		String columnFilter = "*";
		String getRowKey = null;
		
		String sqlTable = null;
		String sqlSchema = null;
		String tableName = null;
                String zookeeperQuorum = "localhost";
                String schemaFile = null;

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

			if (arg.equals("-qn")) { // Query Name
				description.setQueryName(args[++i]);
				continue;
			}
			if (arg.equals("-q")) { // Query File
				String queryFile = args[++i];
				description.setQuery(HBaseHelper.readFile(queryFile));
				continue;
			}
			if (arg.equals("-ty")) { // Schema row type "Table" or "Column"
				description.setType(args[++i]);
				continue;
			}
			if (arg.equals("-k")) {
				description.setSqlKey(args[++i]);
				continue;
			}
			if (arg.equals("-hbt")) {
				description.setTableName(args[++i]);
				continue;
			}
			if (arg.equals("-hbcf")) {
				c.setColumnFamily(args[++i]);
				continue;
			}
			if (arg.equals("-hbq")) {
				c.setQualifier(args[++i]);
				continue;
			}
			if (arg.equals("-c")) {
				c.setSqlColumnName(args[++i]);
				continue;
			}
			if (arg.equals("-t")) {
				c.setDataType(args[++i]);
				continue;
			}
			if (arg.equals("-hbl")) {
				c.setLogicalName(args[++i]);
				continue;
			}
			if (arg.equals("-hbd")) {
				c.setDescription(args[++i]);
				continue;
			}
			if (arg.equals("-save")) {
				isSave = true;
				continue;
			}
			if (arg.equals("-show")) {
				isShow = true;
				continue;
			}
			if (arg.equals("-query")) {
				showQuery = true;
				continue;
			}
			if (arg.equals("-delete")) {
				isDelete = true;
				continue;
			}
			if (arg.equals("-format")) {
				isFormatted = true;
				continue;
			}
			if (arg.equals("-sqld")) {
				isSqlDescribe = true;
				sqlSchema = args[++i];
				sqlTable = args[++i];
				continue;
			}
			if (arg.equals("-sqlg")) {
				isSqlGenerate = true;
				continue;
			}
			if (arg.equals("-sqlh")) {
				this.sqlHost = args[++i];
				continue;
			}
			if (arg.equals("-sqlu")) {
				this.sqlUser = args[++i];
				continue;
			}
			if (arg.equals("-sqldb")) {
				this.sqlDatabase = args[++i];
				continue;
			}
			if (arg.equals("-sqlp")) {
				this.sqlPassword = args[++i];
				continue;
			}
			if (arg.equals("-hbn")) {
				c.setIsNested(true);
				continue;
			}
			if (arg.equals("-import")) {
				isImport = true;
				continue;
			}
			if (arg.equals("-schema")) {
				isSchema = true;
				schemaFile = args[++i];
				continue;
			}
			if (arg.equals("-test")) {
				isTest = true;
				continue;
			}
			if (arg.equals("-dictionary")) {
				isShowDictionary = true;
				tableName = args[++i];
				continue;
			}
			if (arg.equals("-scan")) {
				isScan = true;
				scanFilter = args[++i];
				continue;
			}
			if (arg.equals("-columns")) {
				columnFilter = args[++i];
				continue;
			}
			if (arg.equals("-get")) {
				isGet = true;
				getRowKey = args[++i];
				continue;	
			}
			if (arg.equals("-examples")) {
				examples();
				return;
			}
			if (arg.equals("-daily")) {
				isDaily = true;
				continue;
			}
			if (arg.equals("-verbose")) {
				s_isVerbose = true;
				continue;
			}
            if (arg.equals("-zkq")) {
                zookeeperQuorum = args[++i];
                continue;
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

		initHbase(zookeeperQuorum);
		
		if (isSave) {
			saveMapping(description);
		} else if (isShow) {
			show(description, showQuery, isFormatted);
		} else if (isDelete) {
			deleteMapping(description);
		} else if (isImport) {
			if (description.getQueryName() == null) {
				System.out.println("Missing -qn QueryName");
				usage();
				return;
			}
			importSQL(description, isDaily);	
		} else if (isSchema) {
			parseSchemaFile(schemaFile);	
		} else if (isTest) {
			test();
		} else if (isShowDictionary) {
			showDictionaryFormatted(tableName);
		} else if (isScan) {
			if (description.getTableName() == null) {
				System.out.println("Missing -hbt Table Name");
				usage();
				return;
			}
			scan(scanFilter, description.getTableName(), columnFilter);
		} else if (isGet) {
			if (description.getTableName() == null) {
				System.out.println("Missing -hbt Table Name");
				usage();
				return;
			}
			get(getRowKey, description.getTableName(), columnFilter);
		} else {
			usage();
		}
		
		// Clean up hbase resources
		try { this.schemaTable.close(); } catch (Exception e) {}
		try { this.dictionaryTable.close(); } catch (Exception e) {}
	}

	/**
	 * Initialize HBase configuration.
	 */
	void initHbase(String zookeeperQuorum) throws Exception {

		this.config = HBaseConfiguration.create();

        String zkq = "hbase.zookeeper.quorum";

        System.out.println("Setting " + zkq + " to " + zookeeperQuorum);

        this.config.set(zkq, zookeeperQuorum);

        System.out.println(zkq + ": " + this.config.get(zkq));
		
		Logger.getRootLogger().setLevel(Level.WARN);
		
		this.schemaTable = new HTable(config, "schema");
		this.dictionaryTable = new HTable(config, "dictionary");
		
	}

	/**
	 * Parse the JSON schema file and make entries in 
	 * the HBase schema table.
	 */
	void parseSchemaFile(String schemaFilePath) throws Exception {

		// Read the schema file
		String json = HBaseHelper.readFile(schemaFilePath);

		//System.out.println(json);

		// Set up Gson
		Type collectionType = 
			new TypeToken<Collection<HBaseJsonSchema>>(){}.getType();
		Gson gson = new Gson();

		// Deserialize the file
		Collection<HBaseJsonSchema> list = 
			gson.fromJson(json, collectionType);

		List<String> qualifiers = new ArrayList<String>();

		// Save each description to the HBase schema table
		for (HBaseJsonSchema j:list) {
			HBaseDescription d = 
				HBaseHelper.getDescriptionFromJsonSchema(j);

			// Validate the description
			try {
				d.validate();
			} catch (Exception ex) {
				System.out.println("Invalid description: " + 
						ex.getMessage());
				continue;
			}

			// Make sure the file doesn't have duplicate hbq
			if (d.getType().equals("Column")) {
				String qualifier = d.getHbaseColumn().getQualifier();
				if (qualifiers.contains(qualifier)) {
					System.out.println("Non-unique hbq: " + 
							qualifier + 
							", exiting");
					return;
				} else {
					qualifiers.add(qualifier);
				}
			}
			
			System.out.println("Saving " + d.getType() + 
					" " + (d.getType().equals("Table") 
					? d.getTableName() 
					: d.getHbaseColumn().getLogicalName()));

			// Load the SQL query
			if (d.getType().equals("Table")) {
                            //Get the parent directory for the file and then use the relative file path
                            String queryFilePath = (new File(schemaFilePath)).getParent();
                            
                            String queryFile = new File( queryFilePath, d.getQuery()).getPath();

                            String sql = HBaseHelper.readFile(queryFile);

                            System.out.println(sql);

                            d.setQuery(sql);
			}

			putDescription(d);
		}
	}
	
	/**
	 * Delete an entry from the HBase schema table.
	 */
	void deleteMapping(HBaseDescription description) throws Exception {
		List<Delete> list = new ArrayList<Delete>();
		String rowKey = description.getRowKey();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
        this.schemaTable.delete(list);
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
		byte[] q = Bytes.toBytes("qn");
		
		FilterList list = 
			new FilterList(FilterList.Operator.MUST_PASS_ONE);
		
		// Get the row for the table/query/keys
		SingleColumnValueFilter tableFilter = new SingleColumnValueFilter(
			cf,
			q,
			CompareFilter.CompareOp.EQUAL, 
			Bytes.toBytes(description.getQueryName())
		);
		list.addFilter(tableFilter);
		
		// Get the column maps
		/*
		RegexStringComparator comp = 
			new RegexStringComparator("^" + 
					description.getQueryName() + "_*");   
		SingleColumnValueFilter columnFilter = 
			new SingleColumnValueFilter(
				cf,
				q,
				CompareFilter.CompareOp.EQUAL,
				comp
				);
		list.addFilter(columnFilter);
		*/
		
		s.setFilter(list);
		
		ResultScanner ss = this.schemaTable.getScanner(s);
		return ss;
	}

	/**
	 * Get a scanner that returns all rows in dictionary
	 * matching the table name.
	 */
	ResultScanner getDictionaryScanner(String tableName) 
		throws Exception {

		Scan s = new Scan();
		byte[] cf = Bytes.toBytes("d");
		byte[] q = Bytes.toBytes("table");
		
		FilterList list = 
			new FilterList(FilterList.Operator.MUST_PASS_ONE);
		
		// Get the row for the table/query/keys
		SingleColumnValueFilter tableFilter = new SingleColumnValueFilter(
			cf,
			q,
			CompareFilter.CompareOp.EQUAL, 
			Bytes.toBytes(tableName)
		);
		list.addFilter(tableFilter);
		
		s.setFilter(list);
		
		ResultScanner ss = this.dictionaryTable.getScanner(s);
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

		List<HBaseDescription> columns = new ArrayList<HBaseDescription>();
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
	 * Get a list of dictionary objects from the scanner.
	 */
	List<HBaseDictionary> getDictionaryColumns(ResultScanner ss) 
		throws Exception {
		List<HBaseDictionary> columns = new ArrayList<HBaseDictionary>();
		for (Result r:ss) {
			HBaseDictionary d = HBaseHelper.getDictionaryFromResult(r);
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
		List<HBaseDescription> columns = new ArrayList<HBaseDescription>();
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

		// Sort by Logical Name
		Collections.sort(columns);
		
		for (HBaseDescription d:columns) {
			HBaseColumn c = d.getHbaseColumn();
			System.out.format("%s: %s = %s.%s %s (%s) %n\t%s%n%n", 
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
	 * Show the data dictionary for the HBase table.
	 */
	void showDictionaryFormatted(String tableName) throws Exception {

		ResultScanner ss = getDictionaryScanner(tableName);
		List<HBaseDictionary> list = getDictionaryColumns(ss);
		Collections.sort(list);

		for (HBaseDictionary d:list) {
			String formatted = String.format(
				"%s  %s (%s)  %s%n", 
				d.getFQ(),
				//HBaseHelper.padRight(d.getFQ(), 30, ' '), 
				d.getName(), 
				d.getType(),
				d.getDescription());
			System.out.println(formatted);
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
	 * Writes query map to schema and table docs to dictionary.
	 * 
	 * @param d
	 * @throws IOException 
	 */
	private void putDescription(HBaseDescription d) throws Exception {
		
		HBaseColumn c = d.getHbaseColumn();
		HashMap<String, String> values = new HashMap<String, String>();
		 
		values.put("qn", d.getQueryName());
		values.put("hbt", d.getTableName());
		values.put("ty", d.getType());
		
		String t = d.getType();
		if (t.equals("Table")) {
				values.put("q", d.getQuery());
				values.put("k", d.getSqlKey());
		} else if (t.equals("Column")) {
				values.put("c", c.getSqlColumnName());
				values.put("t", c.getDataType());
				values.put("hbcf", c.getColumnFamily());
				values.put("hbq", c.getQualifier());
				values.put("hbl", c.getLogicalName());
				values.put("hbd", c.getDescription());
				values.put("hbn", c.getIsNested() ? "true" : "false");
		}
		
		put(this.schemaTable, d.getRowKey(), "d", values);

		System.out.println("Wrote " + d.getRowKey() + " to schema");

		putDictionary(d);
	}

	/**
	 * Save the column description to the data dictionary.
	 */
	void putDictionary(HBaseDescription d) throws Exception {
		if (!d.getType().equals("Column")) {
			return;
		}

		HBaseColumn c = d.getHbaseColumn();

		// d.tableName is null for column descriptions, 
		// to we need to look it up in the HBase schema table.
		String tableName = getTableNameForQuery(d.getQueryName());
		
		HashMap<String, String> values = new HashMap<String, String>();
		values.put("table", tableName);
		values.put("type", c.getDataType());
		values.put("family", c.getColumnFamily());
		values.put("qualifier", c.getQualifier());
		values.put("name", c.getLogicalName());
		values.put("description", c.getDescription());
		values.put("nested", c.getIsNested() ? "true" : "false");

		String rowKey = tableName + ":" + 
		   c.getColumnFamily() + ":" + c.getQualifier();	

		put(this.dictionaryTable, rowKey, "d", values);

		System.out.println("Wrote " + rowKey + " to dictionary");
	}
	
	/**
	 * Get the table name (hbt) from the schema table for queryName.
	 */
	String getTableNameForQuery(String queryName) throws Exception {
		Get get = new Get(Bytes.toBytes(queryName));
		Result r = this.schemaTable.get(get);
		return Bytes.toString(r.getValue(Bytes.toBytes("d"), 
					Bytes.toBytes("hbt")));
	}

	/**
	 * Write a single value to HBase.
	 */
	private void put( 
			HTable htable,
			String rowKey, 
			String columnFamily, 
			String qualifier, 
			String value) throws IOException {
		
		Put p = new Put(Bytes.toBytes(rowKey));
		
		p.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), 
				Bytes.toBytes(value));
		
		htable.put(p);
		
	}
	
	/**
	 * Write a collection of values to an HBase row.
	 */
	private void put( 
			HTable htable,
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
		
		htable.put(p);
		
	}

	/**
	 * Get a password from the user, suppressing command line echo.
	 */
	public static String readPassword() {

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
			List<String> columnNames = new ArrayList<String>();
			for (int i = 1 ; i <= numColumns; i++) {
				columnNames.add(rsmd.getColumnName(i));
			}
			
			System.out.format("%s.%s%n", schema, table);

			StringBuffer selectSql = new StringBuffer();
			selectSql.append(String.format(
				"set transaction isolation level read uncommitted;%n"));
			selectSql.append(String.format(
				"set deadlock_priority low;%n"));
			selectSql.append(String.format(
				"set xact_abort on;%n"));
			selectSql.append(String.format(
				"set nocount on;%n%n"));

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
			try { rs.close(); } catch (Exception e) { }
			try { stmt.close(); } catch (Exception e) { }
			try { con.close(); } catch (Exception e) { }
		}
	}

	/**
	 * Run the import SQL and write the data to HBase.
	 */
	private void importSQL(HBaseDescription description, 
			boolean isDaily) throws Exception {

		// Get the table description and the list of columns
		HBaseDescription tableDescription = null;
		List<HBaseDescription> list = 
			getSchemaColumns(getSchemaScanner(description));
		HashMap<String, HBaseColumn> columns = 
			new HashMap<String, HBaseColumn>();
		for (HBaseDescription d:list) {
			if (d.getType().equals("Table")) {
				tableDescription = d;
			} else {
				HBaseColumn c = d.getHbaseColumn();
				columns.put(c.getSqlColumnName(), c);
			}
		}

		if (tableDescription == null) {
			throw new Exception("No Table for " + 
				description.getQueryName());
		}

		// Create a reference to the target table
		HTable htable = 
			new HTable(this.config, tableDescription.getTableName());

		htable.setAutoFlush(false);

		Connection con = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		// Execute the SQL stored in schema.d.q
		try {

			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			con = DriverManager.getConnection(this.connectionString);

			String sql = tableDescription.getQuery(); 

			// If this is a daily incremental import, parse 
			// out the addition to the SQL.
			// 
			// -- DAILY
			// -- where x=y
			// -- /DAILY

			StringBuffer extra = new StringBuffer();
			if (isDaily) {
				StringReader sr = new StringReader(sql);
				BufferedReader br = new BufferedReader(sr);
				String line=null;
				boolean isExtraSql = false;
				while((line = br.readLine()) != null) {
					if (line.indexOf("-- DAILY") == 0) {
						isExtraSql = true;
						continue;
					}
					if (line.indexOf("-- /DAILY") == 0) {
						isExtraSql = false;
						continue;
					}
					if (isExtraSql) {
						extra.append(String.format("%s%n", 
							line.replace("--", "")));	
					}
				}
				sql += String.format("%n%s", extra.toString());
			}

			if (s_isVerbose) {
				System.out.format("%sRunning the following query:%n%s%n", 
					this.prefix, sql);
			}

			stmt = con.prepareStatement(sql);
			rs = stmt.executeQuery();

			// Ask the result set for column meta-data, and cross-check 
			// this against the mappings that are stored in HBase.
			ResultSetMetaData rsmd = rs.getMetaData();
			int numColumns = rsmd.getColumnCount();
			HashMap<String, Integer> columnNames = 
				new HashMap<String, Integer>();
			for (int i = 1 ; i <= numColumns; i++) {
				String columnName = rsmd.getColumnName(i);
				HBaseColumn hbColumn = columns.get(columnName);

				// Make sure we have this column in the HBase schema table
				if (hbColumn == null) {
					System.out.format("%sCould not find %s" +
							" in HBase schema mapping%n", 
							this.prefix, columnName);
					continue;
				}

				int rsType = rsmd.getColumnType(i);

				// Make sure the java.sql.Types value matches
				if (rsType != HBaseHelper
						.getJavaSqlDataType(hbColumn.getDataType())) {

					System.out.format(
						"%sResult Set Type %s does not match HB " + 
						"schema type %s for column %s%n",
							this.prefix,
							rsType,
							hbColumn.getDataType(),
						   	columnName 
						  );

					continue;	

				}

				if (s_isVerbose) {
					System.out.format("%sAdding " + columnName + 
						" to column name list%n", this.prefix);
				}

				columnNames.put(columnName, rsType);
			}

			int totalRowsSaved = 0;
			int numWriteErrors = 0;
			int maxWriteErrors = 10;

			while (rs.next()) {

				// Get the template for the row key for the row in HBase
				// e.g. "CompanyId" 
				// e.g. "{CompanyId}_{NotificationRunId}_{NotificationId}
				//
				// Braces aren't really required for row keys but
				// they are a good convention to indicate a composite.
				String sqlKey = tableDescription.getSqlKey();

				// Get the key value based on the sqlKey template
				String rowKey = HBaseHelper.getCompositeValue(
						columnNames, rs, sqlKey, false);
				byte[] rowKeyBytes = Bytes.toBytes(rowKey);

				// Create a Put to hold the column values for this row
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

					// Convert the raw qualifer to the actual one
					// e.g. cpn_{SlotNumber}_Name = cpn_001_Name
					qualifier = HBaseHelper.getCompositeValue(
							columnNames, 
							rs, 
							qualifier, 
							true);

					byte[] hbaseValue = 
						HBaseHelper.convertSqlValueToHBase(
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

				try {

					// Write the data to HBase
					htable.put(p);

				} catch (Exception putex1) {
					
					System.out.format("%shtable.put(p) failed on first try, retrying", this.prefix);
					putex1.printStackTrace();

					// Try again
					try {
						htable.put(p);
					} catch (Exception putex2) {

						// If we've exceeded max acceptable errors, throw the exception
						if (++numWriteErrors > maxWriteErrors) {
							throw putex2;
						} else {

							// Skip this row
							System.out.format("%shtable.put(p) failed on second try, skipping", 
									this.prefix);
							putex2.printStackTrace();
							continue;
						}
					}
				}

				totalRowsSaved++;

				if (totalRowsSaved % 1000 == 0) {
					System.out.format(
							"%sSaved %s rows...%n", 
							this.prefix, 
						   	totalRowsSaved);
				}
					
			}
			
			System.out.format("%sDone.  Saved %s rows.%n", 
				   this.prefix, totalRowsSaved);

		} 
		catch (Exception e) {
			System.out.format("%sFAILED!!!", this.prefix);
			e.printStackTrace();
		} finally {

			// Flush HTable writes
			try {
				htable.flushCommits();
			} catch (Exception hfex) {
				hfex.printStackTrace();
			}

			// Close HTable
			try { htable.close(); } catch(Exception e) {}

			// Close SQL resources
			try { rs.close(); } catch (Exception e) { }
			try { stmt.close(); } catch (Exception e) { }
			try { con.close(); } catch (Exception e) { }
		}

	}

	/**
	 * Scan the HBase table, apply the filter, and show the results, 
	 * formatted according to the schema in the dictionary table.
	 */
	void scan(String scanFilter, String tableName, 
			String columnFilter) throws Exception {

		// For now we'll just do simple comparisons.
		// "d:cid = 4"

		System.out.println("Scan Filter: " + scanFilter);

		String family = "*";
		String qualifier = "*";
		String columnValue = "*";
		if (!scanFilter.equals("all")) {
			String[] filterTokens = scanFilter.split("=");
			String[] fqTokens = filterTokens[0].split(":");
			family = fqTokens[0].trim();
			qualifier = fqTokens[1].trim();
			columnValue = filterTokens[1].trim();
		}

		System.out.println(String.format(
			"Scanning %s for family %s, qualifier %s, value %s", 
			tableName, family, qualifier, columnValue));
		
		HTable htable = 
			new HTable(this.config, tableName);

		// Put the table dictionary into a map keyed by
		// nested column signature (cpn_n)
		ResultScanner dictionaryScanner = getDictionaryScanner(tableName);
		List<HBaseDictionary> list = 
			getDictionaryColumns(dictionaryScanner);
		Collections.sort(list);

		Scan s = new Scan();
		byte[] f = Bytes.toBytes(family);
		byte[] q = Bytes.toBytes(qualifier);
		
		if (!scanFilter.equals("all")) {
			FilterList filterList = 
				new FilterList(FilterList.Operator.MUST_PASS_ONE);
			
			SingleColumnValueFilter tableFilter = 
				new SingleColumnValueFilter(
					f,
					q,
					CompareFilter.CompareOp.EQUAL, 
					Bytes.toBytes(columnValue)
			);
			tableFilter.setFilterIfMissing(true);
			filterList.addFilter(tableFilter);

			// TODO - This only works for Strings.  
			// Need to look up the dictionary
			// type and convert to int, etc.
			
			/*
			RegexStringComparator comp = 
				new RegexStringComparator("^" + 
						description.getQueryName() + "_*");   
			SingleColumnValueFilter columnFilter = 
				new SingleColumnValueFilter(
					cf,
					q,
					CompareFilter.CompareOp.EQUAL,
					comp
					);
			list.addFilter(columnFilter);
			*/

			System.out.println("Adding filter");
			s.setFilter(filterList);
		} else {
			System.out.println("No filter");
		}
		
		ResultScanner ss = htable.getScanner(s);

		for (Result r : ss) {

			printResult(r, list, columnFilter);
			System.out.println("-------------------");
			// TODO - Confirm after each row?
		}
	}

	/**
	 * Compares the column filter list to the column to see 
	 * if we should include the column in output.
	 */
	boolean includeColumn(String columnFilter, HBaseDictionary d) {
		
		if (columnFilter == null || columnFilter.equals("*")) {
			return true;
		}

		String[] tokens = columnFilter.split(",");
		for (String token : tokens) {
			token = token.trim();

			String[] fq = token.split(":");
			String family = "d";
			String qualifier = null;
			if (fq.length == 1) {
				qualifier = fq[0];
			} else {
				family = fq[0];
				qualifier = fq[1];
			}
			if (d.getFamily().equals(family) && 
				(d.getQualifier().equals(qualifier) || 
				 qualifier.equals("*"))) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Print out the value, formatting according to the type.
	 */
	void printValue(HBaseDictionary d, byte[] value, String prefix, 
			String actualQualifier, String columnFilter) {

		// First make sure we actually want to see this column.
		// TODO - Better to filter pre-scan??
		if (!includeColumn(columnFilter, d)) {
			return;
		}

		System.out.print(String.format(
			"%s%s (%s:%s): ", 
			prefix,
			d.getName(), 
			d.getFamily(), 
			actualQualifier));

		if (value == null) {
			System.out.println("[NULL]");
		} else {
			String t = d.getType();
			
			if (t.equals("boolean")) {
				System.out.println(Bytes.toBoolean(value));
			} else 
			if (t.equals("byte")) {
				System.out.println(value[0]);
			} else 
			if (t.equals("short")) {
				System.out.println(Bytes.toShort(value));
			} else  
			if (t.equals("int")) {
				System.out.println(Bytes.toInt(value));
			} else 
			if (t.equals("long")) {
				System.out.println(Bytes.toLong(value));
			} else 
			if (t.equals("float")) {
				System.out.println(Bytes.toFloat(value));
			} else 
			if (t.equals("decimal") || t.equals("numeric")) {
				System.out.println(Bytes.toBigDecimal(value));
			} else 
			if (t.equals("double")) {
				System.out.println(Bytes.toDouble(value));
			} else 
			if (t.equals("datetime")) {
				System.out.println(
					new java.util.Date(Bytes.toLong(value)));
			} else 
			if (t.equals("guid") || t.equals("string") || t.equals("nstring")) {
				String str = Bytes.toString(value);
				str = str.replace("\r", "\\r");
				str = str.replace("\n", "\\n");
				System.out.println(str);
			} else {
				System.out.println("Unexpected type: " + t);
			}
			
		}
	}

	/**
	 * Get a single row from the HBase table and show the results, 
	 * formatted according to the schema in the dictionary table.
	 */
	void get(String getRowKey, String tableName, String columnFilter) 
		throws Exception {

		// Trigger
		// 0000000004_901D4ECF-D879-41CE-B293-5EDFA9EA6BF2_9B1EF894-9807-4F1A-81D1-002F95212BEE
		
		// Trigger with coupon
		// 0000000004_E4C1D7BA-9F61-42DF-A489-7630A68E8F2D_9C7B8599-144F-4E16-B9B2-BFB3BC7F4485

		// Campaign
		// 0000000004_00064BCD-C059-4A1A-996A-0AAA8E3A8554_54C9F106-1FA3-4FED-96EC-615BFF36D219  

		// Click
		// 0000000004_6D89B3CE-E4AC-460E-94D8-60D5533A86F2_9CC982A6-D010-4ED2-B840-0F9C54E1A6A1  

		// ROI
		// 0000000004_7FC7637A-E52A-4168-A7E7-E0470DD1E65F_885827C6-C50B-46FD-8730-61A34A5AF391  

		// Put the table dictionary into a map keyed by
		// nested column signature (cpn_n)
		ResultScanner ss = getDictionaryScanner(tableName);
		List<HBaseDictionary> list = getDictionaryColumns(ss);
		Collections.sort(list);

		// Get the row from HBase
		Get get = new Get(Bytes.toBytes(getRowKey));
		HTable htable = 
			new HTable(this.config, tableName);

		Result r = htable.get(get);

		printResult(r, list, columnFilter);
	}

	/**
	 * Print out the contents of a row in Hbase.
	 */
	void printResult(Result r, List<HBaseDictionary> columnList, 
			String columnFilter) 
		throws Exception {

		// Map of all columns
		HashMap<String, HBaseDictionary> map = new HashMap<String, HBaseDictionary>();

		for (HBaseDictionary d:columnList) {
			map.put(HBaseHelper.getNestedSignature(d.getQualifier()), d);
		}

		for (HBaseDictionary d : columnList) {
			
			if (d.getNested()) {

				// Nested columns are printed out later
				continue;

			} else {

				// This is a regular non-nested column
				byte[] value = r.getValue(
					Bytes.toBytes(d.getFamily()), 
					Bytes.toBytes(d.getQualifier()));
				
				printValue(d, value, "", d.getQualifier(), columnFilter);
			}
		}

		// Now print nested columns

		boolean hasNested = false;
		for (HBaseDictionary d : columnList) {
			
			//System.out.println(String.format(
			//	"%s: %b", d.getQualifier(), d.getNested()));

			if (d.getNested()) {
				hasNested = true;
				break;
			}
		}

		// No need to continue if there are no nested columns
		if (!hasNested) {
			//System.out.println("(No Nested Columns)");
			return;
		}

		System.out.println();

		// Create a collection for nested groups, which are keyed
		// by the first token in the qualifier.

		// (This seems like it would have been a great use for 
		// column families, but apparently HBase performs poorly
		// if you use them in the way that makes sense)

		// Map of nested column token[0] to list of matching columns
		// e.g. "cpn" = map(cpn_n -> cpn_{SlotNumber}_n, etc)
		HashMap<String, HashMap<String, HBaseDictionary>> nestedTables = 
			HBaseHelper.getNestedTables(columnList);

		KeyValue[] keyValues = r.raw();

		// Map of tableName_Value -> list of KeyValue
		// e.g. all KeyValues for cpn_001
		HashMap<String, List<KeyValue>> rows = new HashMap<String, List<KeyValue>>();

		List<String> nestedKeys = new ArrayList<String>();

		// Iterate through all KeyValues, and add the nested ones
		// to the row that matches the nested row key.
		for (int i = 0; i < keyValues.length; i++) {

			KeyValue kv = keyValues[i];

			String family = Bytes.toString(kv.getFamily());
			String qualifier = Bytes.toString(kv.getQualifier());

			String signature = HBaseHelper.getNestedSignature(qualifier);
			HBaseDictionary d = map.get(signature);

			if (d == null) {
				throw new Exception("map missing " + signature);
			}

			if (!d.getNested()) continue;

			String nestedKey = HBaseHelper.getNestedKey(qualifier);

			List<KeyValue> row = null;
			if (rows.containsKey(nestedKey)) {
				row = rows.get(nestedKey);
			} else {
				row = new ArrayList<KeyValue>();
				rows.put(nestedKey, row);
				nestedKeys.add(nestedKey);
			}
			row.add(kv);
		}

		Collections.sort(nestedKeys);

		for (String nestedKey : nestedKeys) {

			List<KeyValue> row = rows.get(nestedKey);

			// Get the dictionary descriptions for the columns
			HashMap<String, HBaseDictionary> nestedTable = 
				nestedTables.get(nestedKey.split("_")[0]);

			boolean first = true;
			for (KeyValue kv : row) {

				if (first) {

					// Don't print the nested row header if we're 
					// using a column filter.
					if (columnFilter == null || 
						columnFilter.equals("*")) {
						
						System.out.println("Nested Row: " + nestedKey);
					
					}

					first = false;
				}

				String family = Bytes.toString(kv.getFamily());
				String qualifier = Bytes.toString(kv.getQualifier());

				HBaseDictionary d = nestedTable.get(
						HBaseHelper.getNestedSignature(qualifier));

				printValue(d, kv.getValue(), "\t", qualifier, 
						columnFilter);
			}
		}
	}
	
	/**
	 * Output usage to the console.
	 */
	private void usage() {
		System.out.println("HBaseSQLImport v0.1");
		System.out.println("\t-qn\tQueryName");
		System.out.println("\t-q\tQueryFile");
		System.out.println("\t-ty\tType (Table or Column)");
		System.out.println("\t-c\tSQL Column Name");
		System.out.println("\t-t\tData Type");
		System.out.println("\t\tstring, int, boolean, byte, long");
		System.out.println("\t\tdouble, float, datetime, decimal, ");
		System.out.println("\t\tnumeric ");
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
		System.out.println("\t-dictionary Describe columns for a table");
		System.out.println();

		System.out.println("\t-sqld\tDescribe a table in SQL Server");
		System.out.println("\t-sqlh\tSQL Host");
		System.out.println("\t-sqldb\tSQL Database Name");
		System.out.println("\t-sqlu\tSQL Username");
		System.out.println();
		System.out.println("\t-import -qn QueryName " + 
				"-sqlh Host -sqlu User -sqldb Database");
		System.out.println("\t\tRun the SQL and import data into HBase");
		System.out.println(
			"\t-daily\tImport with the addition of optional ");
		System.out.println(
			"\t\tSQL at the end of the file to limit results");
		System.out.println("\t-import -multi multi.json -sqlu user");
		System.out.println("\t\tImport from multiple sources at once.");
		System.out.println("\t-schema SchemaFile.json");
		System.out.println("");
		System.out.println("\t-get RowKey -hbt TableName");
		System.out.println("\t\tGet a single row");
		System.out.println("\t-scan \"d:x=y\" -hbt TableName");
		System.out.println("\t\tScan all rows that match the filter");
		System.out.println("\t-columns \"d:x,d:y\"");
		System.out.println("\t\tColumn filter for -get or -scan");
		System.out.println("");
		System.out.println("\t-examples Output some example commands");
	}

	void examples() {
		System.out.println(
			"java -jar dist/HBaseSQLImport.jar" + 
		   	" -scan \"d:asn=Scheduled Maintenance\" " + 
			" -hbt notification -columns \"d:cid,d:nid\"");
		
		System.out.println(
			"java -jar dist/HBaseSQLImport.jar" + 
		   	" -get 0000000004_E4C1D7BA-9F61-42DF-A489" + 
			"-7630A68E8F2D_9C7B8599-144F-4E16-B9B2-BFB3BC7F4485" + 
		   	" -hbt notification");
		
		System.out.println(
			"java -jar dist/HBaseSQLImport.jar" + 
		   	" -import -qn Lookups -sqlh engineering19b" + 
			" -sqlu hadoop -sqldb LoopEzb_A_01");
		
		System.out.println(
			"java -jar dist/HBaseSQLImport.jar" + 
		   	" -schema ~/Hadoop/LoopHadoop/hbsqli/lookup.json");
		
		System.out.println(
			"java -jar dist/HBaseSQLImport.jar" + 
		   	" -qn Companies -ty Table -hbt company " + 
			" -k CompanyId -save -q sql/companies.sql ");
		
		System.out.println(
			"java -jar dist/HBaseSQLImport.jar" + 
		   	" -qn Companies -show -format");
	}

	void test() throws Exception {

		System.out.println("Testing HBaseHelper");
		HBaseHelper.test();

		System.out.println("Done.");
	}
}
