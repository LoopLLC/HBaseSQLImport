#!/bin/sh
if [ -z "$HBSQLI_HOME" ]; then
	echo "HBSQLI_HOME must be set"
	exit 1
fi

if [ -z "$ZOOKEEPER_QUORUM" ]; then
    echo "ZOOKEEPER_QUORUM not set, using localhost"
    $ZOOKEEPER_QUORUM=localhost
fi

java -cp "$HBSQLI_HOME/dist/*" -jar $HBSQLI_HOME/dist/hbsqli.jar -zkq $ZOOKEEPER_QUORUM $@

