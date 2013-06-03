package net.autoloop;

/**
 * Describes a column in HBase.
 * 
 * @author ericzbeard
 */
public class HBaseColumn {
	protected String columnFamily;
	protected String qualifier;
	protected String logicalName;
	protected String description;
	protected String sqlColumnName;
	protected int sqlType;
	protected boolean isNested;
	
	public void validate(String type) throws Exception {
		
		switch (type) {
			case "Table":
				break;
			case "Column":
				if (this.columnFamily == null) {
					throw new Exception(
							"-hbc HBaseCF must be given for -ty Column");
				}
				if (this.qualifier == null) {
					throw new Exception(
							"-hbq HBaseQualifier must be given for -ty Column");
				}
				if (this.sqlType == 0) {
					throw new Exception(
							"-t SQL Type must be set for -ty Column");
				}
				if (this.sqlColumnName == null) {
					throw new Exception(
							"-c SQL Column Name must be set for -ty Column");
				}
				break;
			default: throw new Exception("Unexpected type");
		}
	}

	public int getSqlType() {
		return this.sqlType;
	}

	public void setSqlType(int t) {
		this.sqlType = t;
	}

	public boolean getIsNested() { 
		return this.isNested;
	}

	public void setIsNested(boolean isNested) {
		this.isNested = isNested;
	}

	/**
	 * @return the columnFamily
	 */
	public String getColumnFamily() {
		return columnFamily;
	}

	/**
	 * @param columnFamily the columnFamily to set
	 */
	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	/**
	 * @return the qualifier
	 */
	public String getQualifier() {
		return qualifier;
	}

	/**
	 * @param qualifier the qualifier to set
	 */
	public void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}

	/**
	 * @return the logicalName
	 */
	public String getLogicalName() {
		return logicalName;
	}

	/**
	 * @param logicalName the logicalName to set
	 */
	public void setLogicalName(String logicalName) {
		this.logicalName = logicalName;
	}

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the sqlColumnName
	 */
	public String getSqlColumnName() {
		return sqlColumnName;
	}

	/**
	 * @param sqlName the sqlColumnName to set
	 */
	public void setSqlColumnName(String sqlColumnName) {
		this.sqlColumnName = sqlColumnName;
	}
}
