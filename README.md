## How to build

`mvn install -DskipTests`

## How to use

1. Install Apache Hadoop
2. Execute the jar

`hadoop jar target/hadoop-tools-0.1-SNAPSHOT-2.7.0.jar <ClassName>`

## ClassName

INotifyUtil

* Poll events for HDFS using HDFS INotify feature and output the details.
* Ctrl + C to stop polling.
* Need to install Hadoop 2.7.0.