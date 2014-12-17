package com.att.research.locstore.client;

import com.att.research.geoStore.Aggregator;
import com.att.research.geoStore.HashSerializer;

import java.util.Date;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Small wrapper class to process a collection of days. This is the
 * only class in the locstore package with a <i>main</i> method
 * for calling directly from the command line or other scripting
 * languages.
 *
 * BatchLoad is meant as an example of how to load a sequence of days
 * using the locstore package. End-users are encouraged to implement their
 * own versions of BatchLoad, calling LoadDay or other lower-level
 * public classes directly.
 *
 * @author  Taylor Arnold
 * @see     LoadDay
 * @see     HBaseTableCreator
 * @since   0.2
 */
public final class BatchLoad {

  /**
   * This method takes an arbitrary number of date strings and
   * processes them in sequence using the LoadDay
   * class. It is assumed, but not enforced that this will be
   * a contiguous sequence of days. All of the days are loaded
   * in a single first pass, and only then saved into the 'daily'
   * HDFS directory. The latter will only happen for a given
   * day if the previous two days have been loaded; therefore
   * the last date in the sequence will typically not be saved
   * into 'daily'.
   *
   * @param args  arguments passed to GenericOptionsParser, followed
   *              by dates to process in
   * @throws      Exception
   */
  public static void main(final String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] rargs = new GenericOptionsParser(conf, args).getRemainingArgs();

    boolean hbaseFlag = false;
    boolean hbaseMakeTables = false;
    String callType = "";
    String callVal = "";
    int ndays = 0;

    for ( String arg : rargs ) {
      if (arg.equals("-hbase")) hbaseFlag = true;
      if (arg.equals("-hbaseMakeTables")) hbaseMakeTables = true;
      if (arg.contains("=")) {
        String vname=arg.substring(0,arg.indexOf('='));
        String vval=arg.substring(arg.indexOf('=') + 1);
        if (vname.equals("-day")) {
          callType = "raw";
          callVal = vval;
          if (ndays == 0) ndays = 1;
        } else if (vname.equals("-ndays")) {
          callType = "raw";
          try {
            ndays = Integer.parseInt(vval);
          } catch (Exception e) {
            ndays = 1;
          }
        } else if (vname.equals("-bmonth")) {
          callType = "bmonth";
          callVal = vval;
        } else if (vname.equals("-traj")) {
          callType = "traj";
          callVal = vval;
        }
      }
    }

    if (hbaseMakeTables) {
      HBaseTableCreator htc = new HBaseTableCreator();
      htc.delete("locstore.entity");
      htc.delete("locstore.geohash");
      htc.delete("locstore.mcell");
      htc.delete("locstore.triples");
      htc.delete("locstore.venue");
      htc.create("locstore.entity");
      htc.create("locstore.geohash");
      htc.create("locstore.mcell");
      htc.create("locstore.triples");
      htc.create("locstore.venue");
    }

    if (callType.equals("raw")) {

      LoadDay llc;
      String thisDate;
      if (ndays == 1) {
        llc = new LoadDay(callVal, conf);
        llc.loadAllData();
        if (hbaseFlag) llc.loadHBase();
      } else {
        Calendar c = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        c.setTime(sdf.parse(callVal));

        for (int i = 0; i < ndays; i++) {
          thisDate = sdf.format(c.getTime());
          c.add(Calendar.DATE, 1);

          llc = new LoadDay(thisDate, conf);
          llc.loadAllData();
          if (hbaseFlag) llc.loadHBase();
        }
      }

    } else if (callType.equals("bmonth")) {
      Combiner cb;
      for (int i = 0; i < 1; i++) {
        String inputDir = "/projects/locstore/daily/" + callVal + "/*/part-r-00[01]" +
                          (new Integer(i)).toString() + "*";
        String outputDir = "/projects/locstore/monthly/" + callVal + "/" + (new Integer(i)).toString();
        System.out.println(inputDir);

        cb = new Combiner(inputDir, outputDir, 250, conf);
        cb.run();
      }
    } else if (callType.equals("traj")) {

    } else {

    }

  }

}
