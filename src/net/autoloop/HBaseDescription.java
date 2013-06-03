package net.autoloop;

/**
 * 
 * Describes a mapping from a SQL column or table to the HBase schema.
 * 
 * @author ericzbeard
 */
public class HBaseDescription {
	
	protected String queryName;
	protected String query;
	protected String tableName;
	protected String sqlKey;
	protected String type;
	protected HBaseColumn hbaseColumn;
	
	public void validate() throws Exception {
		
		if (this.tableName == null) {
			throw new Exception("-hbt HBaseTable must be given");
		}
		
		if (this.queryName == null) {
			throw new Exception("-qn QueryName is required");
		}
		
		if (this.type == null) {
			throw new Exception("-ty Type is required (Table or Column)");
		}
		
		switch (this.type) {
			case "Table":
				if (this.query == null) {
					throw new Exception("-q QueryFile must be given for -ty Table");
				}
				if (this.sqlKey == null) {
					throw new Exception("-k SQLKey must be given for -ty Table");
				}
				break;
			case "Column":
				break;
			default: throw new Exception("Unexpected type, should be Table or Column");
		}
		
		if (this.hbaseColumn == null) {
			throw new Exception("HBaseColumn is null");
		}
		
		this.hbaseColumn.validate(this.type);
	}
	
	/**
	 * Get the key for this row in the HBase schema table.
	 * 
	 * @return 
	 */
	public String getRowKey() {
		if (this.type != null && this.type.equals("Table")) {
			return this.queryName;
		} else {
			return this.queryName + "_" + this.hbaseColumn.getSqlColumnName();
		}
	}
	
	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @param tableName the tableName to set
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * @return the queryName
	 */
	public String getQueryName() {
		return queryName;
	}

	/**
	 * @param queryName the queryName to set
	 */
	public void setQueryName(String queryName) {
		this.queryName = queryName;
	}

	/**
	 * @return the query
	 */
	public String getQuery() {
		return query;
	}

	/**
	 * @param query the query to set
	 */
	public void setQuery(String query) {
		this.query = query;
	}

	/**
	 * @return the key
	 */
	public String getSqlKey() {
		return sqlKey;
	}

	/**
	 * @param sqlKey the key to set
	 */
	public void setSqlKey(String sqlKey) {
		this.sqlKey = sqlKey;
	}

	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * @return the hbaseColumn
	 */
	public HBaseColumn getHbaseColumn() {
		return hbaseColumn;
	}

	/**
	 * @param hbaseColumn the hbaseColumn to set
	 */
	public void setHbaseColumn(HBaseColumn hbaseColumn) {
		this.hbaseColumn = hbaseColumn;
	}
}
