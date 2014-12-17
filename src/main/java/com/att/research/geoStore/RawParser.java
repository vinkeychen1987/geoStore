package com.att.research.geoStore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Method for running a map reduce job to pull the raw data records
 * from the data lake. The output consists of one row per locate in
 * a consistent format across locate types; the output is not reduced
 * or sorted in any way.
 *
 * @author Taylor Arnold
 * @see MapRedJob
 * @since 0.2
 */
public class RawParser extends MapRedJob {

  /**
   * Default constructor for creating a LocstoreLoadClient instance
   * for pulling a particular day of data. The timestamp refers to
   * the record locations and not the actual time of the locates; some
   * data from previous time periods (in some extreme cases, many months
   * prior) will likely be included in the output.
   *
   * @param dateToProcess  date in yyyy/MM/dd format, for which data will be loaded
   * @throws Exception
   */
  public RawParser(String dateToProcess, Configuration conf_in) throws IOException {
    conf = conf_in;
    dt = dateToProcess;
    output_dir = "/projects/locstore/raw/" + dt;
    job_name = this.getClass().getName() + ":" + dt;

    createHadoopConfig(true);

    initHadoopJob(RawParserMap.class, null, null, TextInputFormat.class, 0);
    setMapOutClasses(Text.class, Text.class);
    setOutputClasses(Text.class, Text.class);

    if (dateToProcess.substring(8,10).equals("00")) {
      // Set day to "00" in order to run the hole plugging code
      String datePrefix = dateToProcess.substring(0,7);
      attachInputPath("/projects/datalake/scamp-plug/wireless/SMSD/" + datePrefix);
      attachInputPath("/projects/datalake/scamp-plug/wireless/AWSV/" + datePrefix);
      attachInputPath("/projects/datalake/scamp-plug/wireless/AWSD/" + datePrefix);

      addCache("/projects/locstore/meta/mcell/" + datePrefix + "/30/mcell_hashtable");
    } else {
      attachInputPath("/projects/datalake/scamp-plug/wireless/SMSD/" + dt);
      attachInputPath("/projects/datalake/scamp-plug/wireless/AWSV/" + dt);
      attachInputPath("/projects/datalake/scamp-plug/wireless/AWSD/" + dt);
      attachInputPath("/projects/datalake/scamp3/wireless/SMSD/" + dt);
      attachInputPath("/projects/datalake/scamp3/wireless/AWSV/" + dt);
      attachInputPath("/projects/datalake/scamp3/wireless/AWSD/" + dt);
      attachInputPath("/projects/datalake/nelos2/locations/" + dt);
      attachInputPath("/projects/datalake/closenuph/" + dt);
      attachInputPath("/projects/locstore/meta/wifi/" + dt);

      addCache("/projects/locstore/meta/mcell/" + dt + "/mcell_hashtable");
    }
  }

}

class RawParserMap extends
 Mapper<LongWritable, Text, Text, Text> {

  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd@kk:mm:ss");

  Hashtable<String, String[]> laccid_meta = null;
  Text keyout = new Text();
  Text valout = new Text();

  @Override
  protected void setup(Context context) throws IOException,
   InterruptedException {
    Configuration c = context.getConfiguration();
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

    loadLaccidMeta();
  }

  protected void loadLaccidMeta() {
    try {
      FileInputStream fis = new FileInputStream("mcell_hashtable");
      ObjectInputStream ois = new ObjectInputStream(fis);
      laccid_meta = (Hashtable<String,String[]>) ois.readObject();
      ois.close();
      fis.close();
    } catch (IOException e) {
      return;
    } catch (ClassNotFoundException e) {
      return;
    }
  }

  public void writeVal(LocationRecord lr, Context context) throws IOException, InterruptedException {
    keyout.set(lr.createImsiKey());
    valout.set(lr.createRawRecord());
    context.write(keyout, valout);
  }

  @Override
  public void map(LongWritable key, Text value, Context context)
   throws IOException, InterruptedException {

    int i = 0;
    try {
      LocationMultiRecord lmr = new LocationMultiRecord(value.toString(), laccid_meta);
      LocationRecord[] lrs = lmr.getLocationRecords();
      for (i = 0; i < lrs.length; i++) {
        writeVal(lrs[i], context);
      }
    } catch (Exception e) {
      LocationRecord lr = (new LocationMultiRecord("",laccid_meta)).getLocationRecords()[0];
      lr.parseCode = ParseErrors.UNKNOWN_ERROR;
      if (i != 0) lr.parseCode = ParseErrors.UNKNOWN_MULTI_ERROR;
      writeVal(lr, context);
    }
  }
}

