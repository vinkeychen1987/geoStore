package com.att.research.GeoStore;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.KeyValue;

class HBaseLoaderMap extends
 Mapper<LongWritable,Text,ImmutableBytesWritable,KeyValue> {

  String table_name;
  byte[] FAMILY_COLUMN = "d".getBytes();

  @Override
  protected void setup(Context context) throws IOException,
   InterruptedException {
    Configuration c = context.getConfiguration();
    table_name = c.get("hbase.table.name");
  }

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {


    LocationRecord lr = new LocationRecord(value.toString());

    String thisKey = "";
    String thisCol = "";
    String thisVal = "";
    if (table_name.equals("locstore.entity")) {
      thisKey = lr.createEntityHBaseKey();
      thisCol = lr.createEntityHBaseCol();
      thisVal = lr.createEntityHBaseValue();
    } else if (table_name.equals("locstore.geohash")) {
      thisKey = lr.createGeoHBaseKey();
      thisCol = lr.createGeoHBaseCol();
      thisVal = lr.createGeoHBaseValue();
    }

    ImmutableBytesWritable hKey = new ImmutableBytesWritable();
    hKey.set( thisKey.getBytes() );

    KeyValue kv = new KeyValue(hKey.get(),
                                FAMILY_COLUMN,
                                thisCol.getBytes(),
                                lr.timestamp,
                                thisVal.getBytes());
    context.write(hKey, kv);

  }

}
