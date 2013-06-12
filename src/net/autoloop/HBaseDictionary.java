package net.autoloop;

/**
 * Represents a row in the dictionary table, which 
 * documents columns in tables imported from SQL Server.
 */
public class HBaseDictionary implements Comparable<HBaseDictionary> {
	
	String table;
	String family;
	String qualifier;
	String name;
	String description;
	String type;
	boolean nested;

	public HBaseDictionary() {}

	public int compareTo(HBaseDictionary that) {
		if (that == null) return 1;
		if (this.getRowKey() == null && that.getRowKey() == null) {
			return 0;
		}
		if (that.getRowKey() == null) {
			return 1;
		}
		if (this.getRowKey() == null) {
			return -1;
		}
		return this.getRowKey().compareTo(that.getRowKey());
	}

	public String getRowKey() {
		String t = this.table;
		if (t == null) t = "";
		String f = this.family;
		if (f == null) f = "";
		String q = this.qualifier;
		if (q == null) q = "";
		return t + ":" + f + ":" + q;
	}

	public String getTable() { return this.table; }
	public void setTable(String s) { this.table = s; }
	
	public String getFamily() { return this.family; }
	public void setFamily(String s) { this.family = s; }

	public String getQualifier() { return this.qualifier; }
	public void setQualifier(String s) { this.qualifier = s; }

	public String getName() { return this.name; }
	public void setName(String s) { this.name = s; }

	public String getDescription() { return this.description; }
	public void setDescription(String s) { this.description = s; }

	public String getType() { return this.type; }
	public void setType(String s) { this.type = s; }

	public boolean getNested() { return this.nested; }
	public void setNested(boolean b) { this.nested = b; }
	public void setNested(String s) throws Exception {
		if (s == null) this.nested = false;
		s = s.toLowerCase();

		if (s.equals("true")) this.nested = true;
		else if (s.equals("false")) this.nested = false;
		else throw new Exception("Invalid String: " + s);
	}	

}
