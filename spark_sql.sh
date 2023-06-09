# build the jar file
mvn package
# copy our dataset inside the spark docker container
docker cp stream/dataset.csv spark:/dataset.csv
# copy the generated jar file inside the spark docker container
docker cp target/big-data-showcase-1.0-SNAPSHOT.jar spark:/app_sql.jar
# Run spark submit inside the docker container
docker exec -it spark spark-submit --master=local --conf spark.sql.shuffle.partitions=1 --class com.jowilf.SparkSQLAnalyze --packages "org.apache.hbase:hbase-client:2.4.17,org.apache.hbase:hbase-common:2.4.17" /app_sql.jar /dataset.csv