package com.att.research.geoStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A class for running a map reduce job which pushes the raw
 * data parsed by RawLocDataParser, sorts it by entity and timestamp,
 * and saves a daily snapshot to the Hadoop Filesystem.
 *
 * @author Taylor Arnold
 * @see MapRedJob
 * @since 0.2
 */
public class Saver extends MapRedJob {

  /**
   * Default constructor for creating a Saver instance; technically
   * any number of input dates can be given and any time range can be provided,
   * though the intention is to process some 24-hour period, which may not
   * coorispond to UTC time (which is the baseline for all of the raw data).
   * Also, often data records arrive in the datalake several days after they
   * occur; the Saver class then provides a way to capture these late-arriving
   * records.
   *
   * @param dateToProcess   an dates string in yyyy/MM/dd format, for which to look
   *                        for data in the raw records of
   * @throws IOException
   */
  public Saver(String dateToProcess, Configuration conf_in) throws IOException {
    conf = conf_in;
    dt = dateToProcess;
    output_dir = "/projects/locstore/daily/" + dt;
    job_name = this.getClass().getName() + ":" + dt;

    createHadoopConfig(true);

    initHadoopJob(SaverMap.class, null, SaverReduce.class, TextInputFormat.class, 200);
    setMapOutClasses(Text.class, Text.class);
    setOutputClasses(Text.class, Text.class);

    attachInputPath("/projects/locstore/raw/" + dt);
  }

}

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
    if (!lr.parseErrorOkay()) return;

    keyout.set(lr.createImsiKey());
    valout.set(lr.createFlatRecord());

    context.write(keyout, valout);

  }
}

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
