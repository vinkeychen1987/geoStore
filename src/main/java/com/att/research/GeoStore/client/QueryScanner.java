package com.att.research.GeoStore.client;

import java.util.List;
import java.util.ArrayList;
import java.lang.Math;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.HTable;

/**
 * Query client for pulling data from either the entity or geohash
 * HBase tables.
 *
 * @author Taylor Arnold
 * @see QueryScanner
 * @since 0.3
 *
 */
public class QueryScanner {
  private Cell[] cells;
  private HTable t;
  private boolean entityFlag;

  private String[] start_key;
  private String[] end_key;
  private int n_keys;
  private List<byte[]> familyColumn;
  private List<byte[]> column;
  private List<Scan> scanners;
  private ResultScanner rs = null;

  private int scanIndex = 0; // the scanner are we are currently working on

  /**
   * Returns an array of strings which give the scan ranges needed to search over
   * a particular geospatial-temporal box.
   *
   * @param   table        an HTable object; generally should be locstore.entity or locstore.geohash
   * @param   skeys        an array of the starting keys for the scan
   * @param   ekeys        an array of the ending keys for the scan
   * @param   entity_flag  flag for whether this is linking to an entity table or the geohash table
   */
  public void initGeoScanner(HTable table, String[] skeys, String[] ekeys, boolean entity_flag) {
    entityFlag = entity_flag;
    start_key = skeys;
    end_key = ekeys;
    t = table;
    familyColumn = new ArrayList<byte[]>();
    column = new ArrayList<byte[]>();
    scanners = new ArrayList<Scan>();
    n_keys = start_key.length;
  }

  /**
   * Run this prior to initScans in order to place family and column restrictions on the
   * HBase scans. If not run, all results will be returned
   *
   * @param   family_in    column family to add to the search results
   * @param   column_in    column inside the column family to add to the search results
   */
  public void restrict(byte[] family_in, byte[] column_in) {
    familyColumn.add(family_in);
    column.add(column_in);
  }

  /**
   * After all calls to restrict have been placed, run this to construct the
   * HBase Scan objects.
   *
   */
  public void initScans() {
    for (int i = 0; i < n_keys; i++) {
      byte[] b0 = start_key[i].getBytes();
      byte[] b1 = end_key[i].getBytes();
      Scan s = new Scan(b0, b1);
      s.setCaching(10000);
      if (familyColumn.size() > 0) {
        for (int j = 0; j < familyColumn.size(); j++)
          s.addColumn(familyColumn.get(j), column.get(j));
      }
      scanners.add(s);
    }
  }

  /**
   * Fetchs the next maxElements objects from the set of scanners, and
   * returns results as as string array. One result is giving in each
   * element, and fields are pipe-deliminated. The results mirror those
   * written to HDFS by LocDataSaver.
   *
   * @param   maxElements maximum number of HBase rows to return (may
   *          return more <b>results</b>, which corrispond to cells
   *          rather than rows.)
   * @return  string array of the results, which are pipe-deliminated
   */
  public String[] fetch(int maxElements) throws java.io.IOException {
    Result[] res;
    res = nextResults(maxElements);
    return parseResults(res);
  }

  /**
   * Fetchs the objects from the set of scanners, and writes them to disk. Results
   * are pulled in batches, before writing to disk. This allows for pulling more
   * data than can fit into memory. Unlike the fetch method, this can only be
   * called one per instance.
   *
   * @param   fileName             full pathname for where the output should be stored;
   *                               will overwrite a file if the path already exists.
   * @param   maxElementsPerBatch  maximum number of HBase rows to return before
   *                               writing the results to the disk
   * @param   maxBatch             maximum number of batchs of data to grab.
   * @param   nanocubes            flag for whether the output should instead be
   *                               a five column CSV file for ingesting into nanocubes
   */
  public void fetchToDisk(String fileName, int maxElementsPerBatch, int maxBatch, boolean nanocubes)
      throws FileNotFoundException, java.io.IOException {

    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    for(int j = 0; j < maxBatch; j++) {
      Result[] res;
      res = nextResults(maxElementsPerBatch);
      String[] res_string = new String[1];
      if (!nanocubes) res_string = parseResults(res);
      if (nanocubes) res_string = parseResultsNC(res) ;

      for(int i = 0; i < res_string.length; i++) {
        writer.write(res_string[i]);
        writer.newLine();
      }
    }
    writer.close();
  }

  private Result[] nextResults(int maxElements) throws java.io.IOException{
    if (scanIndex >= n_keys) return null;

    List<Result> res = new ArrayList<Result>();
    int numResults = 0;

    // This should be true if and only if it is the first call to nextResults
    if (rs == null) {
      rs = t.getScanner(scanners.get(scanIndex));
    }

    while (scanIndex < n_keys && numResults < maxElements ) {
      Result[] this_res;
      this_res = rs.next(maxElements - numResults);

      if (this_res.length == 0) {
        scanIndex++;
        if (scanIndex >= n_keys) break;
        rs = t.getScanner(scanners.get(scanIndex));
      } else {
        numResults += this_res.length;
        for (int j = 0; j < this_res.length; j++) {
          res.add(this_res[j]);
        }
      }
    }

    Result[] resultsOut = new Result[numResults];
    for (int i = 0; i < numResults; i++) {
      resultsOut[i] = res.get(i);
    }

    return resultsOut;
  }

  private String[] parseResults(Result[] r) {
    int m = r.length;
    int N = 0;
    int iter = 0;
    Result[] res;
    for (int j = 0; j < m; j++) {
      N += r[j].size();
    }
    String[] val = new String[N];
    for (int j = 0; j < m; j++) {
      res = r;
      cells = r[j].rawCells();
      int n = cells.length;
      for (int i = 0; i < n; i++) {
        Cell c = cells[i];
        String row = new String(c.getRowArray(), c.getRowOffset(), c.getRowLength());
        String col = new String(c.getQualifierArray(), c.getQualifierOffset(),
                                c.getQualifierLength());
        String rval = new String(c.getValueArray(), c.getValueOffset(), c.getValueLength());
        try{

          if (entityFlag) {
            val[iter] = (new StringBuilder(row.substring(0,9)).reverse().toString()) + "|" +
                        row.substring(9,19) + "|" + col + "|" + rval;
          } else {
            val[iter] = row.substring(15) + "|" + row.substring(5,15) + "|" + col + "|" + rval;
          }
        } catch (StringIndexOutOfBoundsException e) {
          val[iter] = "||" + rval;
        }
        iter++;
      }
    }
    return val;
  }

  private String[] parseResultsNC(Result[] r) {
    int m = r.length;
    int N = 0;
    int iter = 0;
    Result[] res;
    for (int j = 0; j < m; j++) {
      N += r[j].size();
    }
    String[] val = new String[N];
    for (int j = 0; j < m; j++) {
      res = r;
      cells = r[j].rawCells();
      int n = cells.length;
      for (int i = 0; i < n; i++) {
        Cell c = cells[i];
        String row = new String(c.getRowArray(), c.getRowOffset(), c.getRowLength());
        String col = new String(c.getQualifierArray(), c.getQualifierOffset(),
                                c.getQualifierLength());
        try{
          String[] rval = (new String(c.getValueArray(), c.getValueOffset(),
                                      c.getValueLength())).split("\\|");
          String type = "UNKNOWN";

          if (rval[0].equals("1")) type = "SMSD";
          if (rval[0].equals("2")) type = "AWSV";
          if (rval[0].equals("3")) type = "AWSD_3G";
          if (rval[0].equals("4")) type = "AWSD_4G";
          if (rval[0].equals("5")) type = "NELOS";
          if (rval[0].equals("6")) type = "CLOSENUPH";
          if (rval[0].equals("6")) type = "WIFI";

          if (entityFlag) {
            val[iter] = row.substring(9,19) + "," + rval[4] + "," + rval[5] + "," +
                        (new StringBuilder(row.substring(0,9)).reverse().toString()) + "," +
                        type;
          } else {
            val[iter] = row.substring(5,15) + "," + rval[4] + "," + rval[5] + "," +
                        row.substring(15) + "," + type;
          }

        } catch (StringIndexOutOfBoundsException e) {
          val[iter] = ",,,,";
        } catch (Exception e) {
          val[iter] = ",,,,";
        }
        iter++;
      }
    }
    return val;
  }

}
