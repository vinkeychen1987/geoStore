package com.att.research.GeoStore.client;

import java.io.IOException;
import java.util.UUID;
import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.compress.Compression;

/**
 * A class for constructing and deleting HBase tables. Intelligent
 * splits are provided for the entity, geohash, and mcell tables.
 *
 * @author Taylor Arnold
 * @since 0.3
 */
public class HBaseTableCreator {

  private HBaseAdmin hba;

  private static String[] entity_splits = new String[] {
    "016645146","033244946","049881954","066539666","083118585",
    "099787686","116444755","133113266","149776556","166492275",
    "183173653","199810656","216502573","233177755","249877995",
    "266564865","283306575","299998246","316719356","333418335",
    "350069576","366763435","383387286","400131156","416741735",
    "433422785","450040326","466647406","483242263","499978845",
    "516607545","533375575","550022606","566714256","583341606",
    "600073215","616690845","633356216","650064145","666737734",
    "683428745","700155056","716738546","733330415","749991535",
    "766654915","783275575","799994855","816630146","833308535",
    "849953905","866533385","883151966","899872694","916596055",
    "933319895","950066275","966730506","983334876"};

  private static String[] geohash_splits = new String[] {
    "0hnd","0t2c","1hnd","1tnd","29jd","2trd","3cu9","40pd","4hq9",
    "52qd","5krd","65q9","6trd","7knd","8bqd","92ed","9jbc","b4r9",
    "bbq9","c3rd","c7rd","ctjd","d6y9","dsy9","e6rd","eum9","f5q9",
    "frv9","g4q9","gpnd","h8rd","hkv9","j8jd","jkv9","k2jd","kdu9",
    "m3pd","mjnd","n4pd","nppd","p5rd","pkrd","q3pd","qjx9","r3pd",
    "r5rd","rny9","s7pd","szy9","trnd","ucq9","v3jd","vpv9","w3pd",
    "wxy9","xkv9","y8q9","ymq9","z8q9"};

  private static String[] mcell_splits = new String[] {"2196857984","5560232584","6416614868","6492334333"};

  private static String[] triples_splits = new String[] {"400000000","600000000","650000000"};

  private static String[] venue_splits = new String[] {"mcd35497.det"};

  /**
   * Constructor for HBaseTableCreator; will throw relevant errors if the script cannot connect
   * to a running instance.
   *
   * @throws IOException
   * @throws NoSuchFieldException
   * @throws IOException
   */
  public HBaseTableCreator() throws MasterNotRunningException, ZooKeeperConnectionException, IOException  {
    Configuration conf = HBaseConfiguration.create();
    hba = new HBaseAdmin( conf );
  }

  /**
   * Create an HBase table with appropriate splits and compression. Will only create one of the three
   * tables used by the locstore package. Will silently ignore requests to construct existing tables.
   *
   * @param tableName  name of the HBase table to construct; either locstore.entity, locstore.geohash,
   *                   locstore.triples or locstore.mcell
   * @throws IOException
   * @throws NoSuchFieldException
   * @throws IOException
   * @throws IllegalAccessException
   */
  public void create(String tableName) throws NoSuchFieldException, IOException, IllegalAccessException {
    if (tableName.equals("locstore.entity")) createThisTable(tableName, entity_splits);
    else if (tableName.equals("locstore.geohash")) createThisTable(tableName, geohash_splits);
    else if (tableName.equals("locstore.mcell")) createThisTable(tableName, mcell_splits);
    else if (tableName.equals("locstore.triples")) createThisTable(tableName, triples_splits);
    else if (tableName.equals("locstore.venue")) createThisTable(tableName, venue_splits);
    else throw new IOException("Invalid Table Name.");
  }

  /**
   * Disable and delete an HBase table. Will only act on one of the three tables used by the locstore
   * package. Will silently ignore requests to delete non-exisiting tables.
   *
   * @param tableName  name of the HBase table to disable and delete; either locstore.entity,
   *                   locstore.geohash, locstore.triples or locstore.mcell
   * @throws IOException
   * @throws NoSuchFieldException
   * @throws IOException
   * @throws IllegalAccessException
   */
  public void delete(String tableName) throws NoSuchFieldException, IOException, IllegalAccessException {
    if (tableName.equals("locstore.entity")) deleteThisTable(tableName);
    else if (tableName.equals("locstore.geohash")) deleteThisTable(tableName);
    else if (tableName.equals("locstore.mcell")) deleteThisTable(tableName);
    else if (tableName.equals("locstore.triples")) deleteThisTable(tableName);
    else if (tableName.equals("locstore.venue")) deleteThisTable(tableName);
    else throw new IOException("Invalid Table Name.");
  }

  private void createThisTable(String tableName, String[] splits)
      throws NoSuchFieldException, IOException, IllegalAccessException {

    if (!hba.tableExists(tableName) ) {
      byte[][] bsplit = new byte[splits.length][];
      for (int i = 0; i < splits.length; i++) bsplit[i] = splits[i].getBytes();

      TableName tn = TableName.valueOf(tableName);
      HTableDescriptor htd = new HTableDescriptor(tn);

      HColumnDescriptor hcd = new HColumnDescriptor("d");
      hcd.setBloomFilterType(BloomType.valueOf("ROW"));
      hcd.setDataBlockEncoding(DataBlockEncoding.valueOf("DIFF"));
      hcd.setCompressionType(Compression.Algorithm.valueOf("SNAPPY"));
      hcd.setScope(0);
      hcd.setKeepDeletedCells(false);
      hcd.setMinVersions(0);
      htd.addFamily(hcd);

      hba.createTable(htd, bsplit);
    }
  }

  private void deleteThisTable(String tableName) throws IOException {
    if (hba.tableExists(tableName) ) {
      hba.disableTable(tableName);
      hba.deleteTable(tableName);
    }
  }

}