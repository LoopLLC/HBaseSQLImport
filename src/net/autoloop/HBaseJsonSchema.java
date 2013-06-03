package net.autoloop;

/**
 * This class is used to deserialize JSON schema files.
 *
 * Gson seems to require an actual class for deserialization, or 
 * I'd be happy to use a HashMap of String:Object.
 */
public class HBaseJsonSchema {

	public String qn;
	public String q;
	public String k;
	public String hbt;
	public String hbcf;
	public String hbq;
	public String c;
	public String t;
	public String hbl;
	public String hbd;
	public String ty;
	public String hbn;

	public HBaseJsonSchema() {}

}
