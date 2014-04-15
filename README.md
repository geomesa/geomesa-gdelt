# Analysis of GDELT using GeoMesa

This sample project demonstrates how to analyze GDELT using GeoMesa.

To ingest:
1) mvn install
2) hadoop jar target/geomesa-gdelt-1.0-SNAPSHOT.jar geomesa.gdelt.GDELTIngest -instanceId [instanceId] -zookeepers [zookeepers] -user [user] -password [password] -auths [auths] -tableName [tableName] -featureName [featureName] -ingestFile [ingestFile]

It will copy your jar to HDFS and requires that the ingestFile be a gdelt format TSV that is already on HDFS.
