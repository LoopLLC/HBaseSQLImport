##HBaseSQLImport

Imports data from SQL Server into HBase.

Mappings are defined between the columns in custom queries and columns in HBase.

This is a command line application written in Java that is similar to Sqoop. 

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

	int, string, nstring, boolean, byte, short, double, float, datetime, guid
	
	These type names are generic and not necessarily targeted to a single language.
	
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
	
*	-delete

	Delete a table or column schema mapping
	
*	-import

	Import the data for -qn QueryName
	
*	-schema FileName.json

	Define the schema using a JSON file instead of typing each one out manually.
	
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


Tables and Columns are linked by the Query Name.

The idea is that you write import queries and name them so that as the application is processing rows, it knows where to put the data.

The import query can be any valid SQL Server TSQL query, it just needs to produce a single result set.

Examples:

java -jar dist/HBaseSQLImport.jar -qn Companies -ty Table -hbt company -k CompanyId -save -q sql/companies.sql 

java -jar dist/HBaseSQLImport.jar -qn Companies -ty Column -c CompanyName -hbcf d -hbq cn -hbl CompanyName -hbd "The name of the company" -save

java -jar dist/HBaseSQLImport.jar -qn Companies -show

java -jar dist/HBaseSQLImport.jar -qn Companies -show -format

java -jar dist/HBaseSQLImport.jar -qn Companies -delete -ty Table



