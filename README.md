##HBaseSQLImport

Imports data from SQL Server into HBase.

Mappings are defined between the columns in custom queries and columns in HBase.

Usage:

java HBaseSQLImport.jar
*	-qn	QueryName
	
	This is given for all commands.  It names the query to be used for importing data.

*	-save

	Save a mapping to the "schema" table.
	

	Save a table: -qn -q -ty -k -hbt
	
	Save a column: -qn -c -hbcf -hbq -hbd -hbl
		
*	-q	QueryFile

	Used when saving a table definition.  The SQL import script is saved to HBase.

*	-ty	Type (Table or Column)

*	-c	SQL Column Name

	Maps a SQL column name from the import script.

*	-t	Column Data Type

	int, String, boolean, byte, double, float, DateTime
	
	These type names are generic and not necessarily targeted to a single language.
	
	SQL equivalents:
	
	INTEGER, VARCHAR, BIT, TINYINT, DOUBLE, FLOAT, DATETIME
		
*	-k	SQL Key
*	-hbt	HBase Table
*	-hbcf	HBase Column Family
*	-hbq	HBase Column Qualifier*	-hbl	HBase Logical Column Name
*	-hbd	HBase Column Description
*	-hbn	HBase Nested Column (denormalized rows)
*	-show [-format]
*	-delete
*	-import

This stores mapping data in an HBase table called "schema".  There are two types of maps, Table and Column.  For a table, the keys are defined, and for a column, the column family and attribute name are defined.

Tables and Columns are linked by the Query Name.

The idea is that you write import queries and name them so that as the application is processing rows, it knows where to put the data.

The import query can select from multiple tables.

Nested data can be defined also.

The SQL Key will be used to create the row key.

Examples:

java -jar dist/HBaseSQLImport.jar -qn Companies -ty Table -hbt company -k CompanyId -save -q sql/companies.sql 

java -jar dist/HBaseSQLImport.jar -qn Companies -ty Column -c CompanyName -hbt company -hbcf d -hbq cn -hbl CompanyName -hbd "The name of the company" -save

java -jar dist/HBaseSQLImport.jar -qn Companies -show

java -jar dist/HBaseSQLImport.jar -qn Companies -show -format

java -jar dist/HBaseSQLImport.jar -qn Companies -delete -ty Table



