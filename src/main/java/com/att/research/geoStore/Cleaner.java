package com.att.research.geoStore;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A class for removing the raw data records for loading data
 * when they are no longer needed.
 *
 * @author Taylor Arnold
 * @since 0.3
 */
public class Cleaner {

  String dt;
  Configuration conf;
  FileSystem fs;

  /**
   * Constructs a Cleaner instance, one of which should be created for every day
   * for which data is to be removed.
   *
   * @param dateToProcess  date in yyyy/MM/dd format, for which data will be loaded
   * @param conf_input     Configuration object, should have been parsed by GenericOptionsParser
   * @throws IOException
   */
  public Cleaner(String dateToProcess, Configuration conf_input) throws IOException {
    dt = dateToProcess;
    conf = conf_input;
    fs = FileSystem.get(conf);
  }

  public void RemoveRaw() throws IOException {
    Path dp = new Path("/projects/locstore/raw/" + dt);
    if(fs.exists(dp)) fs.delete(dp, true);
  }

}
