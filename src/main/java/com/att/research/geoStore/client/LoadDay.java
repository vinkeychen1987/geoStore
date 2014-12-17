package com.att.research.geoStore.client;

import com.att.research.geoStore.HashSerializer;
import com.att.research.geoStore.HBaseLoader;
import com.att.research.geoStore.RawParser;
import com.att.research.geoStore.Reporter;
import com.att.research.geoStore.Saver;
import com.att.research.geoStore.Triples;
import com.att.research.geoStore.WifiParser;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
public class LoadDay {

  private String dt_string;
  private String dt_prev_string;
  private String dt_next_string;
  private String dt_next2_string;
  private Date dtObj;
  private int ts;
  private SimpleDateFormat sdf;
  private FileSystem fs;
  private Calendar c;
  private Configuration conf;

  /**
   * Default constructor for creating a LoadDay instance.
   *
   * @param dateToProcess  date in yyyy/MM/dd format, for which data will be loaded
   * @param conf_input     Configuration object, should have been parsed by GenericOptionsParser
   * @throws Exception
   */
  public LoadDay(String dateToProcess, Configuration conf_input) throws Exception {
    conf = conf_input;
    dt_string = dateToProcess;
    fs = FileSystem.get(conf);
  }

  /**
   * Runs all of loading proceedures for the day associated to a given instance
   * of LocstoreLoadClient. Specifically, it parses and saves the metadata
   * (mcell, venue, and pre-proceed wifi), loads
   * the raw location records to HDFS, batch loads the raw records into the
   * geohash and entity HBase tables, and calculates the triples (TN, IMSI, IMEI)
   * map.
   *
   * @throws Exception
   */
  public void loadAllData() throws Exception {
    //RawParser rldp = new RawParser(dt_string, conf);
    //rldp.currentInputPaths();
    //rldp.run();

    //Triples tp = new Triples(dt_string, dt_prev_string, conf);
    //tp.run();

    //Saver sv = new Saver(dt_string, conf);
    //sv.run();

    Reporter rep = new Reporter(dt_string, conf);
    rep.run();
    rep.save();
  }

  public void loadHBase() throws Exception {
    String mcellPath = getMcellPath();

    HBaseLoader hbl_geohash = new HBaseLoader(dt_string, "locstore.geohash", conf);
    hbl_geohash.run();
    hbl_geohash.load();

    HBaseLoader hbl_entity = new HBaseLoader(dt_string, "locstore.entity", conf);
    hbl_entity.run();
    hbl_entity.load();
  }

}
