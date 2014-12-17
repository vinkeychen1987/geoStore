package com.att.research.geoStore;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


/**
 * A class for creating the data for several reports based on
 * a single day's worth of data records. Reports are created
 * for type crossed with time buckets (15 minutes), geohash
 * buckets (3 digits), and error codes.
 *
 * @author Taylor Arnold
 * @see MapRedJob
 * @since 0.4
 */
public class Reporter extends MapRedJob {

  /**
   * Default constructor for creating a LocReporter instance.
   *
   * @param dateToProcess  the day to process in yyyy/MM/dd format
   * @param conf_in        the hadoop configuration object
   * @throws IOException
   */
  public Reporter(String dateToProcess, Configuration conf_in) throws IOException {
    conf = conf_in;
    dt = dateToProcess;
    output_dir = "/tmp/" + UUID.randomUUID().toString();
    job_name = this.getClass().getName() + ":" + dateToProcess;

    createHadoopConfig(false);

    initHadoopJob(ReporterMap.class, ReporterReduce.class,
                  ReporterReduce.class, TextInputFormat.class, 50);
    setMapOutClasses(Text.class, Text.class);
    setOutputClasses(Text.class, Text.class);

    attachInputPath("/projects/locstore/raw/" + dateToProcess);
  }

  /**
   * Parses the reporter output into four files, one for each type of
   * report (time, geospatial, error code, and laccid/venue)
   *
   */
  public void save() throws IOException {
    List<String> eout = new ArrayList<String>();
    List<String> tout = new ArrayList<String>();
    List<String> gout = new ArrayList<String>();

    String tmpdir = "/" + UUID.randomUUID().toString() + "/";
    String mrOutput = "/tmp" + tmpdir + "tempMrOut/";
    String efile = "/tmp" + tmpdir + "errors.txt";
    String tfile = "/tmp" + tmpdir + "time.txt";
    String gfile = "/tmp" + tmpdir + "geohash.txt";

    fs.copyToLocalFile(false, new Path(output_dir), new Path(mrOutput));
    File inputDir = new File(mrOutput);
    File[] inputFiles = inputDir.listFiles();

    for (int i = 0; i < inputFiles.length; i++) {
      BufferedReader br = new BufferedReader(new FileReader(inputFiles[i]));
      String line;
      while ((line = br.readLine()) != null) {
        String[] ll = line.split("\\|", -1);
        if (ll.length == 4) {
          if (ll[2].equals("e")) {
            String out = new String();
            out = ll[0] + "|" + ll[1] + "|" + ll[3];
            eout.add(out);
          }
          if (ll[2].equals("t")) {
            String out = new String();
            out = ll[0] + "|" + ll[1] + "|" + ll[3];
            tout.add(out);
          }
          if (ll[2].equals("g")) {
            String out = new String();
            out = ll[0] + "|" + ll[1] + "|" + ll[3];
            gout.add(out);
          }
        }
      }
      br.close();
    }

    BufferedWriter writer;

    writer = new BufferedWriter(new FileWriter(efile));
    for(int i = 0; i < eout.size(); i++) {
      writer.write(eout.get(i));
      writer.newLine();
    }
    writer.close();

    writer = new BufferedWriter(new FileWriter(tfile));
    for(int i = 0; i < tout.size(); i++) {
      writer.write(tout.get(i));
      writer.newLine();
    }
    writer.close();

    writer = new BufferedWriter(new FileWriter(gfile));
    for(int i = 0; i < gout.size(); i++) {
      writer.write(gout.get(i));
      writer.newLine();
    }
    writer.close();

    String outputSaveDir = "/projects/locstore/meta/reporter/" + dt;
    fs.delete(new Path(outputSaveDir), true);
    fs.mkdirs(new Path(outputSaveDir));

    fs.copyFromLocalFile(true, true, new Path(efile),
                         new Path(outputSaveDir));
    fs.copyFromLocalFile(true, true, new Path(tfile),
                         new Path(outputSaveDir));
    fs.copyFromLocalFile(true, true, new Path(gfile),
                         new Path(outputSaveDir));

    FileUtils.deleteDirectory(new File("/tmp" + tmpdir));
  }
}

class ReporterMap extends
 Mapper<LongWritable, Text, Text, Text> {

  Text keyout = new Text();
  Text valout = new Text();

  @Override
  protected void setup(Context context) throws IOException,
   InterruptedException {
    Configuration c = context.getConfiguration();
  }

  protected void writeToContext(LocationRecord lr, Context context)
   throws IOException, InterruptedException {

    valout.set("1");

    // Error codes (e)
    if (lr.parseCode != null) {
      keyout.set(lr.type.toString() + "|" + lr.parseCode.toString() + "|e");
      context.write(keyout, valout);
    }

    // Time buckets (t)
    if (lr.ts != null) {
      keyout.set(lr.type.toString() + "|" + Integer.toString(lr.ts / (15 * 60)) + "|t");
      context.write(keyout, valout);
    }

    // Geographic buckets (g)
    if (lr.geohash != null) {
      keyout.set(lr.type.toString() + "|" + lr.geohash.substring(0,3) + "|g");
      context.write(keyout, valout);
    }

    // laccid/venue buckets (l)
    if (lr.location != null) {
      //keyout.set(lr.type.toString() + "|" + lr.location + "|l");
      //if (!lr.type.equals(LocationType.NELOS)) context.write(keyout, valout);
    }
  }

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    LocationRecord lr = new LocationRecord(value.toString());
    writeToContext(lr, context);

  }
}

class ReporterReduce extends
 Reducer<Text, Text, Text, Text> {

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    Text valout = new Text();
    long count = 0;
    for (Text val : values) {
      try {
        count += Long.parseLong(val.toString(), 10);
      } catch (NumberFormatException e) {
        count++;
      }
    }

    valout.set(Long.toString(count));
    context.write(key,valout);
  }
}
