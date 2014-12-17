export HADOOP_CLASSPATH=`/usr/bin/hbase classpath`:/n/taylor/bin/geohash-1.0.11-SNAPSHOT.jar
export HADOOP_CLIENT_OPTS="-Xmx8192m $HADOOP_CLIENT_OPTS"
export JAVA_HEAP_MAX="-Xmx8192m"
export LIBJARS=/n/taylor/bin/geohash-1.0.11-SNAPSHOT.jar
dt=$(perl -e 'use POSIX;print strftime "%Y/%m/%d",localtime time-86400;')

hadoop jar target/locstore-0.8.jar com.att.research.locstore.client.BatchLoad \
  -libjars ${LIBJARS} \
  -day=$dt \
