package com.att.research.GeoStore.client;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.Date;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.att.research.GeoStore.HBaseLoader;
import com.att.research.GeoStore.Saver;

/**
 * A client for loading a given day of data into the Hadoop Filesystem (HDFS)
 * and HBase tables. An instance should be constructed for every day of data
 * to be processed.
 *
 * Two methods are provided, the loadAllData method runs all but one of the steps
 * in the process of parsing the location data. The saveDayOfData method pushes
 * the raw data onto a permanent directory in the Hadoop Filesystem (HDFS). The
 * rational behind this is that the latter can only be called if the previous
 * two days have been loaded. See Saver for more details.
 *
 * @author Taylor Arnold
 * @since 0.2
 */
public class ProcessInput {

  private String inputPaths;
  private String outputDir;
  private int depth;
  private FileSystem fs;
  private Configuration conf;

  /**
   * Default constructor for creating a LoadDay instance.
   *
   * @param dateToProcess  date in yyyy/MM/dd format, for which data will be loaded
   * @param conf_input     Configuration object, should have been parsed by GenericOptionsParser
   * @throws Exception
   */
  public ProcessInput(String inputPaths_in, String outputDir_in,
                    int inputDepth, Configuration conf_input) throws Exception {
    conf = conf_input;
    inputPaths = inputPaths_in;
    for (int i = 0; i < inputDepth; i++) inputPaths = inputPaths + "/*";
    outputDir = outputDir_in;
    fs = FileSystem.get(conf);
  }

  /**
   * Runs all of loading proceedures for the input associated to a given instance
   * of LocstoreLoadClient.
   *
   * @throws Exception
   */
  public void loadAllData() throws Exception {
    // Saver sv = new Saver(dt_string, conf);
    // sv.run();

    // HBaseLoader hbl_geohash = new HBaseLoader(dt_string, "locstore.geohash", conf);
    // hbl_geohash.run();
    // hbl_geohash.load();

    // HBaseLoader hbl_entity = new HBaseLoader(dt_string, "locstore.entity", conf);
    // hbl_entity.run();
    // hbl_entity.load();
  }

}