package com.att.research.geoStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A class for running a map reduce job which pushes the raw
 * data parsed by RawLocDataParser, sorts it by entity and timestamp,
 * and saves a daily snapshot to the Hadoop Filesystem.
 *
 * @author Taylor Arnold
 * @see MapRedJob
 * @since 0.8
 */
public class Aggregator extends MapRedJob {

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
  public Aggregator(String inputPath, String outputPath,
                  int numReducers, Configuration conf_in) throws IOException {
    conf = conf_in;
    dt = "";
    output_dir = outputPath;
    job_name = this.getClass().getName() + ":" + dt;
    logInputFlag = false;

    createHadoopConfig(true);

    initHadoopJob(AggregatorMap.class, null, AggregatorReduce.class, TextInputFormat.class, numReducers);
    setMapOutClasses(Text.class, Text.class);
    setOutputClasses(Text.class, Text.class);
    job.setPartitionerClass(MyHashPartitioner.class);

    Path pAll = new Path(inputPath);
    FileInputFormat.addInputPath(job, pAll);
  }
}

class AggregatorMap extends
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

    String[] vals = value.toString().split("\\|", 2);
    if (vals.length == 2) {
      keyout.set(vals[0]);
      valout.set(vals[1]);
      context.write(keyout, valout);
    }
  }
}

class AggregatorReduce extends
 Reducer<Text, Text, Text, Text> {

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    List<Text> sortedList = new ArrayList<Text>();
    for (Text t : values) {
        sortedList.add(new Text(t));
    }
    Collections.sort(sortedList);
    Text thisElem = new Text("");
    Text prevElem = new Text("");
    for (int i = 0; i < sortedList.size(); i++ ) {
      thisElem = sortedList.get(i);
      if (!thisElem.equals(prevElem)) context.write(key, thisElem);
      prevElem = thisElem;
      //context.write(key, sortedList.get(i));
    }
  }
}

class MyHashPartitioner extends Partitioner<Text, Text> {

  @Override
  public int getPartition(Text key, Text value, int numReduceTasks) {
    // The 5843 is just a randomly choosen large integer to make sure
    // the resulting partitioner evenly distributes the load.
    return ((key.hashCode() & Integer.MAX_VALUE) / 5843) % numReduceTasks;
  }

}

