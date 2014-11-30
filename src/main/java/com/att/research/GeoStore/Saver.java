package com.att.research.GeoStore;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configuration;

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

    initHadoopJob(SaverMap.class, SaverReduce.class, TextInputFormat.class, 50);
    setMapOutClasses(Text.class, Text.class);
    setOutputClasses(Text.class, Text.class);

    attachInputPath("/projects/locstore/raw/" + dt);
  }

}
