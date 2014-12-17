package com.att.research.locstore;

import java.util.Hashtable;

import junit.framework.TestCase;

import com.att.research.locstore.ParseErrors;
import com.att.research.locstore.LocationType;
import com.att.research.locstore.LocationRecord;
import com.att.research.locstore.LocationMultiRecord;

public class LocationMultiRecordTest extends TestCase {

  Hashtable<String, String[]> laccid_meta = null;

  public LocationMultiRecordTest(String name) {
    super(name);
  }

  // Create a testing mcell table
  @Override
  protected void setUp() {
    laccid_meta = new Hashtable<String, String[]>();

    String[] row_3g = new String[3];
    String[] row_4g = new String[3];
    String[] row_5g = new String[3];
    String id_3g = "0003G_TEST";
    String id_4g = "004G_TEST";
    String id_5g = "005G_TEST";

    row_3g[0] = "42.042345";
    row_3g[1] = "-87.425352";
    row_3g[2] = "dp3z4tdf3t";
    row_4g[0] = "42.042345";
    row_4g[1] = "-87.425352";
    row_4g[2] = "dp3z4tdf3t";
    row_5g[0] = "40.00000";
    row_5g[1] = "-90.0000000";
    row_5g[2] = "dppppppppp";

    laccid_meta.put(id_3g, row_3g);
    laccid_meta.put(id_4g, row_4g);
    laccid_meta.put(id_5g, row_5g);
  }

  /**
   * Test good inputs of all input types
   */
  public void testParserGoodNelos() throws Exception {
    String line = "2014-09-17@04:59:16.610|1|310170681982862|-117.143073|32.757687|0|8|4";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);
    assertEquals("310170681982862", lmr.imsi);
    assertEquals(null, lmr.imei);
    assertEquals(null, lmr.tnOrig);
    assertEquals(null, lmr.tnTerm);
    assertEquals(null, lmr.location[0]);
    assertEquals("9mudq7752e", lmr.geohash[0]);
    assertEquals(LocationType.NELOS, lmr.type);
    assertEquals((Integer) null, lmr.seq[0]);
    assertEquals((Integer) 1410929956, lmr.ts[0]);
    assertEquals((Integer) null, lmr.dur[0]);
    assertEquals((Integer) 8, lmr.subtype);
    assertEquals((Integer) null, lmr.cft);
    assertEquals((Integer) null, lmr.ct);
    assertEquals((Integer) 200, lmr.acc[0]);
    assertEquals((Integer) 0, lmr.use);
    assertEquals((Integer) null, lmr.vup);
    assertEquals((Integer) null, lmr.vdn);
    assertEquals(32.757687, lmr.lat[0]);
    assertEquals(-117.143073, lmr.lon[0]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[0]);
  }

  public void testParserGoodSMS() throws Exception {
    String line = "2014-09-17@09:42:06|13134607191||14043142975||13123149086|16787618001|4||83694368|[]|[]|4|0133320045322207|[0003G_TEST]|577326172|1||||0|824|9874|15253|17|||";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);
    assertEquals("310410577326172", lmr.imsi);
    assertEquals("0133320045322207", lmr.imei);
    assertEquals("13134607191", lmr.tnOrig);
    assertEquals("14043142975", lmr.tnTerm);
    assertEquals("0003G_TEST", lmr.location[0]);
    assertEquals("dp3z4tdf3t", lmr.geohash[0]);
    assertEquals(LocationType.SMSD, lmr.type);
    assertEquals((Integer) null, lmr.seq[0]);
    assertEquals((Integer) 1410946926, lmr.ts[0]);
    assertEquals((Integer) 1, lmr.subtype);
    assertEquals((Integer) null, lmr.dur[0]);
    assertEquals((Integer) 0, lmr.cft);
    assertEquals((Integer) 4, lmr.ct);
    assertEquals((Integer) 1000, lmr.acc[0]);
    assertEquals((Integer) 0, lmr.use);
    assertEquals((Integer) null, lmr.vup);
    assertEquals((Integer) null, lmr.vdn);
    assertEquals(42.042345, lmr.lat[0]);
    assertEquals(-87.425352, lmr.lon[0]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[0]);
  }

  public void testParserGoodAWSVOneTower() throws Exception {
    String line = "2014-09-17@18:33:24|12516892652||18889122731||104s|14047259676||6|50331648|2101266|18038595||||[]|[]|0138090014045303|[0003G_TEST]|707694319|2|17||[251281]|[3]|[]|[306]|[12]||62|[]|[1633]|17805|2769|2679|22||||[106]|";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);
    assertEquals("310410707694319", lmr.imsi);
    assertEquals("0138090014045303", lmr.imei);
    assertEquals("12516892652", lmr.tnOrig);
    assertEquals("18889122731", lmr.tnTerm);
    assertEquals("0003G_TEST", lmr.location[0]);
    assertEquals("dp3z4tdf3t", lmr.geohash[0]);
    assertEquals(LocationType.AWSV, lmr.type);
    assertEquals((Integer) 0, lmr.seq[0]);
    assertEquals((Integer) (1410978804 - 2), lmr.ts[0]);
    assertEquals((Integer) 0, lmr.subtype);
    assertEquals((Integer) 106, lmr.dur[0]);
    assertEquals((Integer) 6, lmr.cft);
    assertEquals((Integer) 0, lmr.ct);
    assertEquals((Integer) 1000, lmr.acc[0]);
    assertEquals((Integer) 0, lmr.use);
    assertEquals((Integer) null, lmr.vup);
    assertEquals((Integer) null, lmr.vdn);
    assertEquals(42.042345, lmr.lat[0]);
    assertEquals(-87.425352, lmr.lon[0]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[0]);
  }

  // Still expect this to be "FINISH_OKAY" with bad duration because only one tower
  public void testParserGoodAWSVOneTowerBadDuration() throws Exception {
    String line = "2014-09-17@18:33:24|12516892652||18889122731||104s|14047259676||6|50331648|2101266|18038595||||[]|[]|0138090014045303|[0003G_TEST]|707694319|2|17||[251281]|[3]|[]|[306]|[12]||62|[]|[1633]|17805|2769|2679|22||||[xxx]|";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);
    assertEquals("310410707694319", lmr.imsi);
    assertEquals("0138090014045303", lmr.imei);
    assertEquals("12516892652", lmr.tnOrig);
    assertEquals("18889122731", lmr.tnTerm);
    assertEquals("0003G_TEST", lmr.location[0]);
    assertEquals("dp3z4tdf3t", lmr.geohash[0]);
    assertEquals(LocationType.AWSV, lmr.type);
    assertEquals((Integer) 0, lmr.seq[0]);
    assertEquals((Integer) (1410978804 - 2), lmr.ts[0]);
    assertEquals((Integer) 0, lmr.subtype);
    assertEquals((Integer) (104 + 2), lmr.dur[0]);
    assertEquals((Integer) 6, lmr.cft);
    assertEquals((Integer) 0, lmr.ct);
    assertEquals((Integer) 1000, lmr.acc[0]);
    assertEquals((Integer) 0, lmr.use);
    assertEquals((Integer) null, lmr.vup);
    assertEquals((Integer) null, lmr.vdn);
    assertEquals(42.042345, lmr.lat[0]);
    assertEquals(-87.425352, lmr.lon[0]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[0]);
  }

  public void testParserGoodAWSVMultiTower() throws Exception {
    String line = "2014-09-17@18:28:48|12514591946||13346634186||379s|14047259676||7|50331689|2102290|152263747||||[3346634186]|[4]|3554310534010017|[0003G_TEST:004G_TEST]|619918224|16|64||[251281:6664]|[3:18]|[]|[730]|[12]||62|[]|[363]|17529|2769|2644|22|||20|[390:5]|";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);

    assertEquals("310410619918224", lmr.imsi);
    assertEquals("3554310534010017", lmr.imei);
    assertEquals("12514591946", lmr.tnOrig);
    assertEquals("13346634186", lmr.tnTerm);
    assertEquals("0003G_TEST", lmr.location[0]);
    assertEquals("004G_TEST", lmr.location[1]);
    assertEquals("dp3z4tdf3t", lmr.geohash[0]);
    assertEquals("dp3z4tdf3t", lmr.geohash[1]);
    assertEquals(LocationType.AWSV, lmr.type);
    assertEquals((Integer) 0, lmr.seq[0]);
    assertEquals((Integer) 1, lmr.seq[1]);
    assertEquals((Integer) (1410978528 - 16), lmr.ts[0]);
    assertEquals((Integer) (1410978528 - 16 + 390), lmr.ts[1]);
    assertEquals((Integer) 0, lmr.subtype);
    assertEquals((Integer) 390, lmr.dur[0]);
    assertEquals((Integer) 5, lmr.dur[1]);
    assertEquals((Integer) 7, lmr.cft);
    assertEquals((Integer) 0, lmr.ct);
    assertEquals((Integer) 1000, lmr.acc[0]);
    assertEquals((Integer) 1000, lmr.acc[1]);
    assertEquals((Integer) 0, lmr.use);
    assertEquals((Integer) null, lmr.vup);
    assertEquals((Integer) null, lmr.vdn);
    assertEquals(42.042345, lmr.lat[0]);
    assertEquals(-87.425352, lmr.lon[0]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[0]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[1]);
  }

  public void testParserGoodAWSDOneTowerBadDuration() throws Exception {
    String line = "2014-09-17@17:50:16|12077456801||4|8s|573004913|1991|4472||0133330027401707|22|71311493|868135||[004G_TEST]|29|phone||198.228.201.205|135.211.84.196|10.66.210.168|NYCMNYCZ31SPG03||6||14116|65988|3|68|||2|||||||||||||";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);
    assertEquals("310410573004913", lmr.imsi);
    assertEquals("0133330027401707", lmr.imei);
    assertEquals("12077456801", lmr.tnOrig);
    assertEquals(null, lmr.tnTerm);
    assertEquals("004G_TEST", lmr.location[0]);
    assertEquals("dp3z4tdf3t", lmr.geohash[0]);
    assertEquals(LocationType.AWSD, lmr.type);
    assertEquals((Integer) 1410976216, lmr.ts[0]);
    assertEquals((Integer) 8, lmr.dur[0]);
    assertEquals((Integer) 29, lmr.subtype);
    assertEquals((Integer) 22, lmr.cft);
    assertEquals((Integer) null, lmr.ct);
    assertEquals((Integer) 1991, lmr.vup);
    assertEquals((Integer) 4472, lmr.vdn);
    assertEquals(42.042345, lmr.lat[0]);
    assertEquals(-87.425352, lmr.lon[0]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[0]);
  }

  public void testParserGoodAWSDOneTower() throws Exception {
    String line = "2014-09-17@17:50:16|12077456801||4|8s|573004913|1991|4472||0133330027401707|22|71311493|868135||[004G_TEST]|29|phone||198.228.201.205|135.211.84.196|10.66.210.168|NYCMNYCZ31SPG03||6||14116|65988|3|68|||2||||||[3600]|||||||";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);
    assertEquals("310410573004913", lmr.imsi);
    assertEquals("0133330027401707", lmr.imei);
    assertEquals("12077456801", lmr.tnOrig);
    assertEquals(null, lmr.tnTerm);
    assertEquals("004G_TEST", lmr.location[0]);
    assertEquals("dp3z4tdf3t", lmr.geohash[0]);
    assertEquals(LocationType.AWSD, lmr.type);
    assertEquals((Integer) 1410976216, lmr.ts[0]);
    assertEquals((Integer) 3600, lmr.dur[0]);
    assertEquals((Integer) 29, lmr.subtype);
    assertEquals((Integer) 22, lmr.cft);
    assertEquals((Integer) null, lmr.ct);
    assertEquals((Integer) 1991, lmr.vup);
    assertEquals((Integer) 4472, lmr.vdn);
    assertEquals(42.042345, lmr.lat[0]);
    assertEquals(-87.425352, lmr.lon[0]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[0]);
  }

  public void testParserGoodAWSDMultiTower() throws Exception {
    String line = "2014-09-17@17:43:34|17326183870||4|419s|543442410|34520|79419|||20|71307429|868159||[004G_TEST:005G_TEST:004G_TEST:004G_TEST:004G_TEST]|29|phone||198.228.201.205|198.228.201.204|10.86.49.129|NYCMNYCZ31SPG03||111||13714|65988|2|67|||145||||||[0:1:180:60:110]|||||||";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);
    assertEquals("310410543442410", lmr.imsi);
    assertEquals("", lmr.imei); // this is correct; imei is missing, but should be "" not null
    assertEquals("17326183870", lmr.tnOrig);
    assertEquals(null, lmr.tnTerm);
    assertEquals("004G_TEST", lmr.location[0]);
    assertEquals("005G_TEST", lmr.location[1]);
    assertEquals("004G_TEST", lmr.location[2]);
    assertEquals("004G_TEST", lmr.location[3]);
    assertEquals("004G_TEST", lmr.location[4]);
    assertEquals("dp3z4tdf3t", lmr.geohash[0]);
    assertEquals("dppppppppp", lmr.geohash[1]);
    assertEquals("dp3z4tdf3t", lmr.geohash[2]);
    assertEquals("dp3z4tdf3t", lmr.geohash[3]);
    assertEquals("dp3z4tdf3t", lmr.geohash[4]);
    assertEquals(LocationType.AWSD, lmr.type);
    assertEquals((Integer) (0), lmr.seq[0]);
    assertEquals((Integer) (1), lmr.seq[1]);
    assertEquals((Integer) (2), lmr.seq[2]);
    assertEquals((Integer) (3), lmr.seq[3]);
    assertEquals((Integer) (4), lmr.seq[4]);
    assertEquals((Integer) (1410975814), lmr.ts[0]);
    assertEquals((Integer) (1410975814 + 0), lmr.ts[1]);
    assertEquals((Integer) (1410975814 + 0 + 1), lmr.ts[2]);
    assertEquals((Integer) (1410975814 + 0 + 1 + 180), lmr.ts[3]);
    assertEquals((Integer) (1410975814 + 0 + 1 + 180 + 60), lmr.ts[4]);
    assertEquals((Integer) 0, lmr.dur[0]);
    assertEquals((Integer) 1, lmr.dur[1]);
    assertEquals((Integer) 180, lmr.dur[2]);
    assertEquals((Integer) 60, lmr.dur[3]);
    assertEquals((Integer) 110, lmr.dur[4]);
    assertEquals((Integer) 29, lmr.subtype);
    assertEquals((Integer) 20, lmr.cft);
    assertEquals((Integer) null, lmr.ct);
    assertEquals((Integer) 34520, lmr.vup);
    assertEquals((Integer) 79419, lmr.vdn);
    assertEquals(42.042345, lmr.lat[0]);
    assertEquals(-87.425352, lmr.lon[0]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[0]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[1]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[2]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[3]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[4]);
  }

  public void testParserBadDurAWSDMultiTower() throws Exception {
    String line = "2014-09-17@17:43:34|17326183870||4|419s|543442410|34520|79419|||20|71307429|868159||[004G_TEST:005G_TEST:004G_TEST:004G_TEST:004G_TEST]|29|phone||198.228.201.205|198.228.201.204|10.86.49.129|NYCMNYCZ31SPG03||111||13714|65988|2|67|||145||||||[0:1:x:60:110]|||||||";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);

    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[0]);
    assertEquals(5, lmr.dur.length);

    assertEquals("310410543442410", lmr.imsi);
    assertEquals("", lmr.imei); // this is correct; imei is missing, but should be "" not null
    assertEquals("17326183870", lmr.tnOrig);
    assertEquals(null, lmr.tnTerm);
    assertEquals("004G_TEST", lmr.location[0]);
    assertEquals("005G_TEST", lmr.location[1]);
    assertEquals("004G_TEST", lmr.location[2]);
    assertEquals("004G_TEST", lmr.location[3]);
    assertEquals("004G_TEST", lmr.location[4]);
    assertEquals("dp3z4tdf3t", lmr.geohash[0]);
    assertEquals("dppppppppp", lmr.geohash[1]);
    assertEquals("dp3z4tdf3t", lmr.geohash[2]);
    assertEquals("dp3z4tdf3t", lmr.geohash[3]);
    assertEquals("dp3z4tdf3t", lmr.geohash[4]);
    assertEquals(LocationType.AWSD, lmr.type);
    assertEquals((Integer) (0), lmr.seq[0]);
    assertEquals((Integer) (1), lmr.seq[1]);
    assertEquals((Integer) (2), lmr.seq[2]);
    assertEquals((Integer) (3), lmr.seq[3]);
    assertEquals((Integer) (4), lmr.seq[4]);
    assertEquals((Integer) (1410975814), lmr.ts[0]);
    assertEquals((Integer) (1410975814 + 0), lmr.ts[1]);
    assertEquals((Integer) (1410975814 + 0 + 1), lmr.ts[2]);
    assertEquals((Integer) (null), lmr.ts[3]);
    assertEquals((Integer) (1410975814 + 419), lmr.ts[4]);
    assertEquals((Integer) 0, lmr.dur[0]);
    assertEquals((Integer) 1, lmr.dur[1]);
    assertEquals((Integer) null, lmr.dur[2]);
    assertEquals((Integer) 60, lmr.dur[3]);
    assertEquals((Integer) 110, lmr.dur[4]);
    assertEquals((Integer) 29, lmr.subtype);
    assertEquals((Integer) 20, lmr.cft);
    assertEquals((Integer) null, lmr.ct);
    assertEquals((Integer) 34520, lmr.vup);
    assertEquals((Integer) 79419, lmr.vdn);
    assertEquals(42.042345, lmr.lat[0]);
    assertEquals(-87.425352, lmr.lon[0]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[0]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[1]);
    assertEquals(ParseErrors.DURATION_PARSE_ERROR, lmr.parseCode[2]);
    assertEquals(ParseErrors.BAD_PRIOR_DURATION, lmr.parseCode[3]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[4]);
  }

  public void testParserGoodCloseNUPH() throws Exception {
    String line = "90118545639148,4G_TEST,38.975723,-76.485779,mnd,alu,9,1410832836,20140916-020036,12162625084,01343000:488375:12162625084,dcac96c2";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);
    assertEquals("90118545639148", lmr.imsi);
    assertEquals(null, lmr.imei);
    assertEquals(null, lmr.tnOrig);
    assertEquals(null, lmr.tnTerm);
    assertEquals("004G_TEST", lmr.location[0]);
    assertEquals("dqctex2f01", lmr.geohash[0]);
    assertEquals(LocationType.CLOSENUPH, lmr.type);
    assertEquals((Integer) 1410832836, lmr.ts[0]);
    assertEquals((Integer) null, lmr.dur[0]);
    assertEquals((Integer) 1, lmr.subtype);
    assertEquals((Integer) null, lmr.cft);
    assertEquals((Integer) null, lmr.ct);
    assertEquals((Integer) null, lmr.vup);
    assertEquals((Integer) null, lmr.vdn);
    assertEquals(38.975723, lmr.lat[0]);
    assertEquals(-76.485779, lmr.lon[0]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[0]);
  }

  public void testParserGoodWifi() throws Exception {
    String line = "310410662778249|0133330027401707|fxo1540.atl|1411141821|1933|33.449532|-86.822922|djfq2333xk|1455|1300|5083431234";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);
    assertEquals("310410662778249", lmr.imsi);
    assertEquals("0133330027401707", lmr.imei);
    assertEquals("5083431234", lmr.tnOrig);
    assertEquals(null, lmr.tnTerm);
    assertEquals("fxo1540.atl", lmr.location[0]);
    assertEquals("djfq2333xk", lmr.geohash[0]);
    assertEquals(LocationType.WIFI, lmr.type);
    assertEquals((Integer) 1411141821, lmr.ts[0]);
    assertEquals((Integer) 1933, lmr.dur[0]);
    assertEquals((Integer) null, lmr.subtype);
    assertEquals((Integer) null, lmr.cft);
    assertEquals((Integer) null, lmr.ct);
    assertEquals((Integer) 1455, lmr.vup);
    assertEquals((Integer) 1300, lmr.vdn);
    assertEquals(33.449532, lmr.lat[0]);
    assertEquals(-86.822922, lmr.lon[0]);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[0]);
  }

  // /**
  //  * Test modes of failure specific to particular location type(s)
  //  */
  public void testParserNelosTempError() throws Exception {
    String line = "2014-09-17@04:59:16.610|2|310410681982862|-117.143073|32.757687|0|8|4";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);
    assertEquals(ParseErrors.NELOS_TEMPORARY_IMSI, lmr.parseCode[0]);
  }

  public void testParserBadLaccid() throws Exception {
    String line = "2014-09-17@17:43:34|17326183870||4|419s|543442410|34520|79419|||20|71307429|868159||[BADLACCID]|29|phone||198.228.201.205|198.228.201.204|10.86.49.129|NYCMNYCZ31SPG03||111||13714|65988|2|67|||145||||||[180]|||||||";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);
    assertEquals(ParseErrors.NO_LACCID_MATCH, lmr.parseCode[0]);
  }

  public void testParserStickyAwsd3G() throws Exception {
    String line = "2014-09-17@17:50:16|12077456801||4|8s|573004913|1991|4472||0133330027401707|22|71311493|868135||[0003G_TEST]|29|phone||198.228.201.205|135.211.84.196|10.66.210.168|NYCMNYCZ31SPG03||6||14116|65988|3|68|||2|||||||||||||";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);
    assertEquals(ParseErrors.AWSD_STICKY_3G_RECORD, lmr.parseCode[0]);
  }

  public void testParserStickyAwsdSgw4G() throws Exception {
    String line = "2014-09-17@17:50:16|12077456801||4|8s|573004913|1991|4472||0133330027401707|22|71311493|868135||[004G_TEST]|28|phone||198.228.201.205|135.211.84.196|10.66.210.168|NYCMNYCZ31SPG03||6||14116|65988|3|68|||2|||||||||||||";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);
    assertEquals(ParseErrors.AWSD_SGW_RECORD, lmr.parseCode[0]);
  }


  // /**
  //  * Test that AWSV and AWSD are emitting correct LocationRecord arrays
  //  */
  public void testParserGoodAWSDMultiTowerLRArray() throws Exception {
    String line = "2014-09-17@17:43:34|17326183870||4|419s|543442410|34520|79419|||20|71307429|868159||[004G_TEST:004G_TEST:004G_TEST:004G_TEST:004G_TEST]|29|phone||198.228.201.205|198.228.201.204|10.86.49.129|NYCMNYCZ31SPG03||111||13714|65988|2|67|||145||||||[0:1:180:60:110]|||||||";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);
    assertEquals(ParseErrors.FINISH_OKAY, lmr.parseCode[0]);

    LocationRecord[] lr = lmr.getLocationRecords();

    assertEquals(5, lmr.location.length);
    assertEquals(5, lmr.geohash.length);
    assertEquals(5, lmr.seq.length);
    assertEquals(5, lmr.ts.length);
    assertEquals(5, lmr.dur.length);
    //assertEquals(5, lmr.acc.length);
    assertEquals(5, lmr.lat.length);
    assertEquals(5, lmr.lon.length);
    assertEquals(5, lmr.parseCode.length);


    assertEquals(5, lr.length);

    for (int i = 0; i < 5; i++) {
      assertEquals("004G_TEST", lr[i].location);
      assertEquals("dp3z4tdf3t", lr[i].geohash);
      assertEquals(42.042345, lr[i].lat);
      assertEquals(-87.425352, lr[i].lon);
    }
    assertEquals((Integer) 1410975814, lr[0].ts);
    assertEquals((Integer) 1410975814, lr[1].ts);
    assertEquals((Integer) 1410975815, lr[2].ts);
    assertEquals((Integer) 1410975995, lr[3].ts);
    assertEquals((Integer) 1410976055, lr[4].ts);
  }

  public void testParserBadAWSDMultiTowerLRArrayNoGeoMulti() throws Exception {
    String line = "2014-09-17@17:43:34|17326183870||4|419s|543442410|34520|79419|||20|71307429|868159||[004G_TEST:004G_TEST:004G_TEST:BADLACCID:004G_TEST]|29|phone||198.228.201.205|198.228.201.204|10.86.49.129|NYCMNYCZ31SPG03||111||13714|65988|2|67|||145||||||[0:1:180:60:110]|||||||";
    LocationMultiRecord lmr = new LocationMultiRecord(line, laccid_meta);
    LocationRecord[] lr = lmr.getLocationRecords();
    assertEquals(5, lr.length);

    for (int i = 0; i < 3; i++) {
      assertEquals("004G_TEST", lr[i].location);
      assertEquals("dp3z4tdf3t", lr[i].geohash);
      assertEquals(42.042345, lr[i].lat);
      assertEquals(-87.425352, lr[i].lon);
      assertEquals(ParseErrors.FINISH_OKAY, lr[i].parseCode);
    }

    assertEquals("BADLACCID", lr[3].location);
    assertEquals(null, lr[3].geohash);
    assertEquals(null, lr[3].lat);
    assertEquals(null, lr[3].lon);
    assertEquals(ParseErrors.NO_LACCID_MATCH, lr[3].parseCode);

    assertEquals(ParseErrors.FINISH_OKAY, lr[4].parseCode);
    assertEquals("004G_TEST", lr[4].location);
    assertEquals("dp3z4tdf3t", lmr.geohash[4]);
    assertEquals(42.042345, lr[4].lat);
    assertEquals(-87.425352, lr[4].lon);

  }

}