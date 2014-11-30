package com.att.research.GeoStore;

import java.io.IOException;
import java.lang.NumberFormatException;
import java.lang.StringBuilder;
import java.text.ParseException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class SaverMap extends
 Mapper<LongWritable, Text, Text, Text> {

  Text keyout = new Text();
  Text valout = new Text();

  @Override
  protected void setup(Context context) throws IOException,
   InterruptedException {
    Configuration c = context.getConfiguration();
  }

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    LocationRecord lr = new LocationRecord(value.toString());

    keyout.set(lr.createFlatKey());
    valout.set(lr.createFlatValue());

    context.write(keyout, valout);

  }
}
