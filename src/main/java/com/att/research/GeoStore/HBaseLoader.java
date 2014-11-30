package com.att.research.GeoStore;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

/**
 * This class allows for pushing the raw parsed location data into
 * one of two HBase tables. One table stores the data in a way
 * conducive to querying by entity id (imsi), whereas the other
 * table allows for geospatial-temporal queries. The class also
 * allows for pushing the meta data from the mcell and triples
 * tables into hbase.
 *
 * The class must first be 'run', using the inherited run method, before
 * using the load method to load the data into the HBase table. The latter
 * method should be relatively quick as the first step does most of the
 * time-consuming work.
 *
 * @author Taylor Arnold
 * @see MapRedJob
 * @since 0.2
 */
public class HBaseLoader extends MapRedJob {

  private String hbase_table = "";
  private HTable hTable;

  /**
   * Default constructor for creating a LocstoreLoadClient instance for a particular
   * day and table. The timestamp refers to
   * the record locations and not the actual time of the locates; some
   * data from previous time periods (in some extreme cases, many months
   * prior) will likely be included in the output.
   *
   * @param dateToProcess    date in yyyy/MM/dd format, for which data will be loaded
   * @param hbaseTableName   'locstore.geohash' for loading into the geospatially
   *                         indexed HBase table, 'locstore.entity' for loading into
   *                         the entity indexed table, 'locstore.mcell' for loading
   *                         the mcell table, or 'locstore.triples' for loading the triples map
   * @throws IOException
   */
  public HBaseLoader(String dateToProcess, String hbaseTableName, Configuration conf_in) throws IOException {
    conf = conf_in;
    hbase_table = hbaseTableName;
    dt = dateToProcess;
    output_dir = "/tmp/" + UUID.randomUUID().toString();
    job_name = this.getClass().getName() + hbase_table + ":" + dt;

    createHadoopConfig(true);
    conf.set("hbase.table.name", hbaseTableName);
    createHBaseConfig();

    initHadoopJob(HBaseLoaderMap.class, null, TextInputFormat.class, -1);
    setMapOutClasses(ImmutableBytesWritable.class, KeyValue.class);
    initHBaseJob();

    if (hbaseTableName.equals("locstore.entity") | hbaseTableName.equals("locstore.geohash")) {
      attachInputPath("/projects/locstore/raw/" + dt);
    } else if (hbaseTableName.equals("locstore.triples")) {
      attachInputPath("/projects/locstore/triples/" + dt);
    } else if (hbaseTableName.equals("locstore.venue")) {
      attachInputPath("/projects/datalake/wifi/PHB/" + dt + "/aws_venue_export_" +
                      dt.replace("/", "") + ".csv");
    } else {
      // mcell table; need to add path manually with the addThisPath method
    }
  }

  /**
   * Loads the data processed by the mapreduce job into the relevant
   * HBase table. Must be called after the run method.
   *
   * @throws Exception
   */
  public void load() throws Exception {
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    loader.doBulkLoad(new Path(output_dir), hTable);
    removeHdfsDirectory(output_dir);
  }

  /**
   * Allows for adding any file to the InputPath of the hadoop job;
   * currently used for loading the mcell table because unlike the
   * other tables this is a particular file rather than a directory
   * of files.
   *
   * @param inputString  the full pathname to add to the hadoop job
   * @throws Exception
   */
  public void addThisPath(String inputString) throws Exception {
    FileInputFormat.addInputPath(job, new Path(inputString));
  }

  protected void createHBaseConfig() throws IOException {
    Configuration hconf = HBaseConfiguration.create();
    HBaseConfiguration.addHbaseResources(conf);
    hTable = new HTable(hconf, hbase_table);
  }

  protected void initHBaseJob() throws IOException {
    HFileOutputFormat.configureIncrementalLoad(job, hTable);
  }

}
