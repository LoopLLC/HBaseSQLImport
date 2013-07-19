package net.autoloop;

/**
 * This class is used to deserialize JSON schema files.
 *
 * Gson seems to require an actual class for deserialization, or 
 * I'd be happy to use a HashMap of String:Object.
 */
public class HBaseMulti {

	public String qn;
   	public String sqlh;
	public String sqldb;	

	public HBaseMulti() {}

}
