##HBaseSQLImport

**Eric Z. Beard**

<eric@loopfx.com>

Imports data from SQL Server into HBase.

Mappings are defined between the columns in custom queries and columns in HBase.  

This is a command line application written in Java that is similar to Sqoop.

It also offers some convenient functions for getting and scanning rows in HBase, displaying the values according to the schema established when importing data. 

*DISCLAIMER*

*This code is very early in its life cycle and has not undergone any serious testing!  I don't recommend using it for any mission critical applications yet.  The design is still evolving, and it may undergo serious changes before it's ready for production.  It's been years since I've done any serious Java programming.  In other words, use at your own risk!*

Development Configuration: Mac OS X, Java 1.7, Hadoop 1.1.2, HBase 0.94.7 in pseudo-distributed mode, SQL Server 2008/12, and Vim of course.  :w!

Build:

Just type "ant".

The build system probably needs some work.  I created it with NetBeans, and I also created an IntelliJ IDEA project.  Seems like all the cool kids are using Maven these days, so I should get that figured out at some point.

Usage:

java HBaseSQLImport.jar
*	-qn	QueryName
	
	This is given for all commands that need to interact with HBase.  It names the query to be used for importing data.

*	-save

	Save a mapping to the "schema" table.  This table must be created manually before using hbsqli.

*	-q	QueryFile

	Used when saving a table definition.  The SQL import script is saved to HBase.

*	-ty	Type (Table or Column)

	The HBase schema table holds the basic information for the table as well as the columns.
	
*	-c	SQL Column Name

	Maps a SQL column name from the import script.

*	-t	Column Data Type

	int, string, nstring, boolean, byte, short, double, float, datetime, guid, decimal
	
	These type names represent a relationship between a Java SQL Type and a regular Java type.  For example, "guid" represents a SQL Server UNIQEUIDENTIFIER, which is a java.sql.CHAR, which gets converted to a Java String.
	
*	-k	SQL Key

	The key column in the SQL query. This is used as the row key for each row saved to this HBase table.  May be composite.
	
*	-hbt	HBase Table

	The name of the HBase table.
	
*	-hbcf	HBase Column Family

	The name of the HBase column family.  HBase doesn't actually support many families well per table, so often you just have a single column family called "d".
	
*	-hbq	HBase Column Qualifier

	The HBase column qualifer.  Can be composite for nested values.
		
*	-hbl	HBase Logical Column Name

	For documentation purposes.  The logical name for the column.  A data dictionary is created when defining mappings, so you can see what fields are where.
	
*	-hbd	HBase Column Description

	For documentation purposes.  A long description of the field.
	
*	-hbn	HBase Nested Column (denormalized rows)

	You can store nested rows in the HBase table by supplying a composite qualifier.  For example if a customer has multiple email addresses, you could have a qualifier of "Email\_{EmailAddress}" or "Email\_{EmailID}".  The data from the field matching the part of the qualifer in braces would get written to the column name (yes, in HBase you store data in column names sometimes, it's Ok).
	
*	-show [-format]

	Show the schema mapping information for -qn QueryName
	
*	-dictionary

	Describe the columns in an HBase table, based on the schema mapping descriptions.  These descriptions are independent of a any single import, so if two imports describe the same column, the last description processed wins (ideally they would have the same description).
	
*	-delete

	Delete a table or column schema mapping
	
*	-import

	Import the data for -qn QueryName
	
*	-schema FileName.json

	Define the schema using a JSON file instead of typing each one out manually as a separate command.  This is how you will define schemas most of the time.  There should be a one to one relationship between JSON schema files and SQL import files.  If the same column is defined in multiple schema files, the most recent version wins.  The schema and dictionary entries will be overwritten each time the schema is saved.
	
    [
    { 
	    "qn":"Notifications", 
	    "ty":"Table", 
	    "hbt":"notification", 
	    "k":"{CompanyId}\_{NotificationRunId}\_{NotificationId}", 
	    "q":"/Users/ericzbeard/Hadoop/LoopHadoop/hbsqli/sql/notifications.sql"
    }, 
    {
	    "qn":"Notifications", 
	    "ty":"Column", 
	    "c":"NotificationId", 
	    "hbcf":"d", 
	    "hbq":"nid", 
	    "hbl":"Notification ID", 
	    "hbd":"The primary key for the Notification, a Guid String", 
	    "t":"guid"
    }
    ]

*	-get RowKey -hbt TableName
		
	Get a single row

*	-scan "d:x=y" -hbt TableName
		
	Scan all rows that match the filter

*	-columns "d:x,d:y"
	
	Column filter for -get or -scan

*	-examples Output some example commands

Tables and Columns are linked by the Query Name.

The idea is that you write import queries and name them so that as the application is processing rows, it knows where to put the data.

The import query can be any valid SQL Server TSQL query, it just needs to produce a single result set with values that match the expected data types.

Examples:

java -jar dist/HBaseSQLImport.jar -qn Companies -ty Table -hbt company -k CompanyId -save -q sql/companies.sql 

java -jar dist/HBaseSQLImport.jar -qn Companies -ty Column -c CompanyName -hbcf d -hbq cn -hbl CompanyName -hbd "The name of the company" -save

java -jar dist/HBaseSQLImport.jar -qn Companies -show

java -jar dist/HBaseSQLImport.jar -qn Companies -show -format

java -jar dist/HBaseSQLImport.jar -qn Companies -delete -ty Table

java -jar dist/HBaseSQLImport.jar -scan "d:asn=Scheduled Maintenance" -hbt notification -columns "d:cid,d:nid"

java -jar dist/HBaseSQLImport.jar -get 123 -hbt company

java -jar dist/HBaseSQLImport.jar -import -qn Companies -sqlh mymachine -sqlu hadoop -sqldb dbname

java -jar dist/HBaseSQLImport.jar -schema ~/company.json

java -jar dist/HBaseSQLImport.jar -scan \* -hbt lookup




