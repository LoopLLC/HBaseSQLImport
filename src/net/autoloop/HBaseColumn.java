package net.autoloop;

/**
 * Describes a column in HBase.
 * 
 * @author ericzbeard
 */
public class HBaseColumn {
	protected String tableName;
	protected String columnFamily;
	protected String attribute;
	protected String logicalName;
	protected String description;
	protected String key;
	
	public void validate(String type) throws Exception {
		if (this.tableName == null) {
			throw new Exception("-hbt HBaseTable must be given");
		}
		
		switch (type) {
			case "Table":
				if (this.key == null) {
					throw new Exception("-hbk HBaseKey must be given for -ty Table");
				}
				break;
			case "Column":
				if (this.columnFamily == null) {
					throw new Exception("-hbc HBaseCF must be given for -ty Column");
				}
				if (this.attribute == null) {
					throw new Exception("-hba HBaseAttribute must be given for -ty Column");
				}
				break;
			default: throw new Exception("Unexpected type");
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
	 * @return the attribute
	 */
	public String getAttribute() {
		return attribute;
	}

	/**
	 * @param attribute the attribute to set
	 */
	public void setAttribute(String attribute) {
		this.attribute = attribute;
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
	 * @return the key
	 */
	public String getKey() {
		return key;
	}

	/**
	 * @param key the key to set
	 */
	public void setKey(String key) {
		this.key = key;
	}
}
