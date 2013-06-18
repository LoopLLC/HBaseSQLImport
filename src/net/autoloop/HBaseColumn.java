package net.autoloop;

import org.apache.commons.lang.*;
import java.util.*;

/**
 * Describes a column in HBase.
 * 
 * @author ericzbeard
 */
public class HBaseColumn implements Comparable<HBaseColumn> {
	protected String columnFamily;
	protected String qualifier;
	protected String logicalName;
	protected String description;
	protected String sqlColumnName;
	protected String dataType;
	protected boolean isNested;
	
	public void validate(String type) throws Exception {
		
		switch (type) {
			case "Table":
				break;
			case "Column":
				if (StringUtils.isBlank(this.columnFamily)) {
					throw new Exception(
						"-hbc HBaseCF must be given for -ty Column");
				}
				if (StringUtils.isBlank(this.qualifier)) {
					throw new Exception(
					"-hbq HBaseQualifier must be given for -ty Column");
				}
				if (StringUtils.isBlank(this.dataType)) {
					throw new Exception(
						"-t SQL Type must be set for -ty Column");
				}
				List<String> validTypes = new ArrayList<>();
				validTypes.add("int");
				validTypes.add("string");
				validTypes.add("nstring");
				validTypes.add("boolean");
				validTypes.add("long");
				validTypes.add("float");
				validTypes.add("double");
				validTypes.add("datetime");
				validTypes.add("byte");
				validTypes.add("guid");
				validTypes.add("short");
				validTypes.add("decimal");
				validTypes.add("numeric");
				if (!validTypes.contains(this.dataType)) {
					throw new Exception("Invalid -t SQL Type");
				}
				if (StringUtils.isBlank(this.sqlColumnName)) {
					throw new Exception(
						"-c SQL Column Name must be set for -ty Column");
				}
				break;
			default: throw new Exception("Unexpected type");
		}
	}

	/**
	 * Compare to another column.  For sorting.
	 *
	 * Sorts by Logical Name
	 */
	public int compareTo(HBaseColumn that) {
		if (that == null) return 1;
		if (this.getLogicalName() == null && 
			that.getLogicalName() == null) return 0;		
		if (that.getLogicalName() == null) return 1;
		if (this.getLogicalName() == null) return -1;
		return this.getLogicalName().compareTo(that.getLogicalName());
	}

	public String getDataType() {
		return this.dataType;
	}

	public void setDataType(String t) {
		this.dataType = t;
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
