echo off

rm dsl.jar
rm -rf class
mkdir class

export CLASSPATH=.:dsl.jar:class
export CLASSPATH=$CLASSPATH:libs/fgg-ext-libs.jar:libs/trove4j-3.0.3.jar

javac -d class fgg/data/*.java fgg/access/*.java fgg/utils/*.java
javac -d class fgg/grpc/*.java
javac -d class fgg/jclients/*.java

cd class
jar -cfm ../dsl.jar ../conf/manifest.mf fgg/*
cd ..
