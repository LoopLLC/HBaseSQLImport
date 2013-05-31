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
	protected String sqlName;
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
				break;
			default: throw new Exception("Unexpected type");
		}
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
	 * @return the sqlName
	 */
	public String getSqlName() {
		return sqlName;
	}

	/**
	 * @param sqlName the sqlName to set
	 */
	public void setSqlName(String sqlName) {
		this.sqlName = sqlName;
	}
}
