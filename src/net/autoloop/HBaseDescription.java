package net.autoloop;

import org.apache.commons.lang.*;

/**
 * 
 * Describes a mapping from a SQL column or table to the HBase schema.
 * 
 * @author ericzbeard
 */
public class HBaseDescription implements Comparable<HBaseDescription> {
	
	protected String queryName;
	protected String query;
	protected String tableName;
	protected String sqlKey;
	protected String type;
	protected HBaseColumn hbaseColumn;
	
	public void validate() throws Exception {
		
		if (StringUtils.isBlank(this.queryName)) {
			throw new Exception("-qn QueryName is required");
		}
		
		if (StringUtils.isBlank(this.type)) {
			throw new Exception("-ty Type is required (Table or Column)");
		}
		
		if (this.type == null) {
			throw new Exception("type is null");
		}
		
		if (!(this.type.equals("Column") || this.type.equals("Table"))) {
			throw new Exception("Unexpected type: " + this.type);
		}
		
		if (this.type.equals("Table")) {
			if (StringUtils.isBlank(this.query)) {
				throw new Exception(
						"-q QueryFile must be given for -ty Table");
			}
			if (StringUtils.isBlank(this.sqlKey)) {
				throw new Exception(
						"-k SQLKey must be given for -ty Table");
			}
			if (StringUtils.isBlank(this.tableName)) {
				throw new Exception(
						"-hbt HBaseTable must be given for -ty Table");
			}
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
			return this.queryName + "_" + 
				this.hbaseColumn.getSqlColumnName();
		}
	}

	/**
	 * Compare the description.  For sorting.
	 */
	public int compareTo(HBaseDescription that) {
		if (that == null) return 1;

		if (that.getType() == null && this.getType() == null) return 0;

		if (that.getType() == null) return 1;

		if (this.getType() == null) return -1;

		// Sort Table before anything else
		if (this.getType().equals("Table") && 
			!that.getType().equals("Table")) {
			return 1;
		}

		if (this.getType().equals("Table")) {
			if (this.getTableName() == null && 
				that.getTableName() == null) return 0;
			if (this.getTableName() == null) return -1;
			if (that.getTableName() == null) return 1;
			return this.getTableName().compareTo(that.getTableName());
		} else {
			if (this.getHbaseColumn() == null && 
				that.getHbaseColumn() == null) {
				return 0;
			}
			if (that.getHbaseColumn() == null) return 1;
			if (this.getHbaseColumn() == null) return -1;
			return this.getHbaseColumn().compareTo(that.getHbaseColumn());
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
