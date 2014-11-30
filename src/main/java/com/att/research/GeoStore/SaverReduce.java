package com.att.research.GeoStore;

import java.io.IOException;
import java.lang.StringBuilder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.KeyValue;

class SaverReduce extends
 Reducer<Text, Text, Text, Text> {

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    List<Text> sortedList = new ArrayList<Text>();
    for (Text t : values) {
        sortedList.add(new Text(t));
    }
    Collections.sort(sortedList);
    for (int i = 0; i < sortedList.size(); i++ )
      context.write(key, sortedList.get(i));
  }
}