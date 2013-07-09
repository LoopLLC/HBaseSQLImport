#!/bin/sh
if [ -z "$HBSQLI_HOME" ]; then
	echo "HBSQLI_HOME must be set"
	exit 1
fi
java -cp "$HBSQLI_HOME/dist/*" -jar $HBSQLI_HOME/dist/HBaseSQLImport.jar $@

