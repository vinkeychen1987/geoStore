package com.att.research.geoStore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Parent class for running map reduce jobs within the locstore
 * package. Takes care of much of the overhead in running jobs
 * over hadoop. Subclasses should implement their own constructors.
 *
 * @author Taylor Arnold
 * @since 0.3
 */
public class MapRedJob {

  protected String dt = "";
  protected String output_dir = "";
  protected String job_name = "";
  protected boolean logInputFlag = true;
  protected List<String> inputPathGlob = new ArrayList<String>();

  protected Configuration conf;
  protected Job job;
  protected FileSystem fs;

  /**
   * Runs a properly constructed map reduce job. Will wait for job completion,
   * and throw an appropriate error if the job cannot be initalized or gets
   * interupted. Hadoop jobs which run normally but return as 'FAIL' will appear
   * to return from run normally, so this possiblity will need to be monitored.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  public void run() throws IOException, InterruptedException, ClassNotFoundException {
    removeHdfsDirectory(output_dir);
    job.waitForCompletion(true);
  }

  /**
   * Returns all of the input paths which are attached to the hadoop job;
   * write the results in '/projects/locstore/log/input/''
   *
   */
  public void currentInputPaths() throws FileNotFoundException, IOException {
    String[] inputPathGlobArray = new String[inputPathGlob.size()];
    inputPathGlob.toArray(inputPathGlobArray); // fill the array

    File tempFile = File.createTempFile("fname",".serialized");
    tempFile.deleteOnExit();
    String tempFilename = tempFile.getPath();

    BufferedWriter writer = new BufferedWriter(new FileWriter(tempFilename));
    for(int i = 0; i < inputPathGlobArray.length; i++) {
      writer.write(inputPathGlobArray[i]);
      writer.newLine();
    }
    writer.close();

    if (logInputFlag) {
      String outputLocation = "/projects/locstore/meta/log/input/" + dt + "/inputFiles.txt";
      fs.delete(new Path(outputLocation), true);
      fs.copyFromLocalFile(true, true, new Path(tempFilename),
                           new Path(outputLocation));
    }
  }

  protected void createHadoopConfig(boolean compress) throws IOException {
    conf.set("mapreduce.output.textoutputformat.separator", "|");
    conf.setLong("mapreduce.task.timeout", 60000000);
    conf.set("fs.permissions.umask-mode", "000");

    if (compress) {
      String compressCodecName = "org.apache.hadoop.io.compress.BZip2Codec";
      conf.set("mapreduce.output.fileoutputformat.compress.type", "RECORD");
      conf.set("mapreduce.output.fileoutputformat.compress","true");
      conf.set("mapreduce.output.fileoutputformat.compress.codec", compressCodecName);
      conf.set("mapreduce.map.output.compress", "true");
      conf.set("mapreduce.map.output.compress.codec", compressCodecName);
    }

    fs = FileSystem.get(conf);
  }

  protected void attachInputPath(String path) throws IOException {
    Path p = new Path(path);
    if(fs.exists(p)) {
      Path pAll = new Path(path + "/*");
      FileInputFormat.addInputPath(job, pAll);
      FileStatus[] files = fs.globStatus(pAll);
      for (int i = 0; i < files.length; i++) {
        if (files[i].isDirectory()) {
          Path newPath = files[i].getPath();
          String newPathName = newPath.getParent() + "/" + newPath.getName() + "/*";
          FileStatus[] filesSub = fs.globStatus(new Path(newPathName));
          for (int j = 0; j < filesSub.length; j++) {
            String fname = filesSub[j].getPath().getParent() + "/" +
                           (filesSub[j].getPath()).getName();
            String modTime = Long.toString(filesSub[j].getModificationTime() / 1000);
            String byteSize = Long.toString(filesSub[j].getLen());
            inputPathGlob.add(fname + "|" + modTime + "|" + byteSize);
          }
        } else {
          String fname = files[i].getPath().getParent() + "/" + (files[i].getPath()).getName() ;
          String modTime = Long.toString(files[i].getModificationTime() / 1000);
          String byteSize = Long.toString(files[i].getLen());
          inputPathGlob.add(fname + "|" + modTime + "|" + byteSize);
        }
      }
    }
  }

  protected void addCache(String path) throws IOException {
    try {
      job.addCacheFile(new URI(path));
    } catch (URISyntaxException e) {
      throw new IOException("Bad input path " + path + ".");
    }
  }

  protected void initHadoopJob(Class<? extends Mapper> mapper,
      Class<? extends Reducer> combiner,
      Class<? extends Reducer> reducer,
      Class<? extends InputFormat> inputFormat, int numReducers) throws IOException {

    job = Job.getInstance(conf, job_name);

    if (mapper != null) {
      job.setJarByClass(mapper);
      job.setMapperClass(mapper);
    }
    if (combiner != null) {
      job.setJarByClass(combiner);
      job.setCombinerClass(combiner);
    }
    if (reducer != null) {
      job.setJarByClass(reducer);
      job.setReducerClass(reducer);
    }
    if (inputFormat != null) job.setInputFormatClass(inputFormat);
    if (numReducers >= 0) job.setNumReduceTasks(numReducers);

    FileOutputFormat.setOutputPath(job, new Path(output_dir));
  }

  protected void setMapOutClasses(Class key, Class val) {
    job.setMapOutputKeyClass(key);
    job.setMapOutputValueClass(val);
  }

  protected void setOutputClasses(Class key, Class val) {
    job.setOutputKeyClass(key);
    job.setOutputValueClass(val);
  }

  protected void removeHdfsDirectory(String dirpath) throws IOException{
    Path dp = new Path(dirpath);
    if(fs.exists(dp)) fs.delete(dp, true);
  }

}
