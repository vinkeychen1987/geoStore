export HADOOP_CLASSPATH=`/usr/bin/hbase classpath`:/n/taylor/bin/geohash-1.0.11-SNAPSHOT.jar
export HADOOP_CLIENT_OPTS="-Xmx8192m $HADOOP_CLIENT_OPTS"
export JAVA_HEAP_MAX="-Xmx8192m"
export LIBJARS=/n/taylor/bin/geohash-1.0.11-SNAPSHOT.jar

hadoop jar target/locstore-0.8.jar com.att.research.locstore.client.BatchLoad \
  -libjars ${LIBJARS} \
  -bmonth=2014/09 \
