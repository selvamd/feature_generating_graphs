echo off
export CLASSPATH=.:dsl.jar
export CLASSPATH=$CLASSPATH:libs\fgg-ext-libs.jar:libs\sqlite-jdbc-3.27.2.1.jar
java -Xms1028m -cp $CLASSPATH fgg.jclients.$1 $2 $3 $4
