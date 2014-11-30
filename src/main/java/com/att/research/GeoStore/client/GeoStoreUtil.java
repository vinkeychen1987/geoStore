package com.att.research.GeoStore.client;

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
public final class GeoStoreUtil {

  /**
   * This is the primary user-facing class for calling GeoStore
   * functions.
   *
   * @param args  arguments passed to GenericOptionsParser, followed
   *              by dates to process in
   * @throws      Exception
   */
  public static void main(final String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] rargs = new GenericOptionsParser(conf, args).getRemainingArgs();

    ProcessInput llc;
  }

}