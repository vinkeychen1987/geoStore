package com.att.research.geoStore;

/**
 * A class for holding a single location record. That is, it represents
 * an (entity, location, timestamp) triple, along with other metadata
 * concerning the locate. These other attributes include the location
 * method, the accuracy, and when available, the duration. In practice
 * this class will generally be constructed by calling getLocationRecords
 * on an instance of LocationMultiRecord.
 *
 * @author Taylor Arnold
 * @see RawParser
 * @see Saver
 * @since 0.1
 */
public class LocationRecord
{
  /** full (normally 15 digit) international mobile subscriber identity */
  public String imsi = null;

  /** international mobile station equipment identity */
  public String imei = null;

  /** originating telephone number */
  public String tnOrig = null;

  /** terminating telephone number (voice and text only) */
  public String tnTerm = null;

  /** value of the locate LACCID, WiFi venue ID, or (for other locates) the 10 digit geohash */
  public String location = null;

  /** a ten digit geohash as a base-32 string */
  public String geohash = null;

  /** see LocationType for details */
  public LocationType type = LocationType.UNKNOWN;

  /** tower sequence number for multi-tower AWSV and AWSD records */
  public Integer seq = null;

  /** timestamp in seconds from 1970-01-01T00:00:00, in UTC */
  public Integer ts = null;

  /** array of locate durations in seconds */
  public Integer dur = null;

  /** location subtype; method for NELOS, feed for CDRs, error for CNUPH */
  public Integer subtype = null;

  /** cause for termination */
  public Integer cft = null;

  /** call type for sms and voice records */
  public Integer ct = null;

  /** accuracy, in metres */
  public Integer acc = null;

  /** use code; signals what conditions exist for exclusion of this record */
  public Integer use = null;

  /** volume up for data and wifi */
  public Integer vup = null;

  /** volume down for data and wifi */
  public Integer vdn = null;

  /** latitude of the locate */
  public Double lat = null;

  /** longitude of the locate */
  public Double lon = null;

  /** flag for whether record was succesfully parsed by constructor */
  public ParseErrors parseCode = ParseErrors.UNKNOWN_ERROR;

  /**
   * Explicit constructor for LocationRecord
   *
   * @param  imsi_in       full international mobile subscriber identity (imsi); usually 15 digits
   * @param  imei_in       international mobile station equipment identity (imei); usually 15 digits
   * @param  tnOrig_in     originating telephone number
   * @param  tnTerm_in     terminating telephone number
   * @param  location_in   input location string; either laccid, wifi id, or geohash
   * @param  geohash_in    ten digit geohash for the locate
   * @param  type_in       LocationType code
   * @param  seq_in        tower sequence number for multi-tower AWSV and AWSD records; index starts at 0
   * @param  ts_in         timestamp; in seconds from 1970-01-01T00:00:00, in UTC
   * @param  dur_in        duration in seconds
   * @param  subtype_in    location subtype; method for NELOS, feed for CDRs
   * @param  cft_in        cause for termination
   * @param  ct_in         call type for sms and voice records
   * @param  acc           accuracy in metres
   * @param  use           use code
   * @param  vup_in        volume up for data and wifi
   * @param  vdn_in        volume down for data and wifi
   * @param  lat_in        latitude value for the locate
   * @param  lon_in        longitude value for the locate
   */
  public LocationRecord(String imsi_in,
                        String imei_in,
                        String tnOrig_in,
                        String tnTerm_in,
                        String location_in,
                        String geohash_in,
                        LocationType type_in,
                        Integer seq_in,
                        Integer ts_in,
                        Integer dur_in,
                        Integer subtype_in,
                        Integer cft_in,
                        Integer ct_in,
                        Integer acc_in,
                        Integer use_in,
                        Integer vup_in,
                        Integer vdn_in,
                        Double lat_in,
                        Double lon_in,
                        ParseErrors parseCode_in)
  {
    imsi = imsi_in;
    imei = imei_in;
    tnOrig = tnOrig_in;
    tnTerm = tnTerm_in;
    location = location_in;
    geohash = geohash_in;
    type = type_in;
    seq = seq_in;
    ts = ts_in;
    dur = dur_in;
    subtype = subtype_in;
    cft = cft_in;
    ct = ct_in;
    acc = acc_in;
    use = use_in;
    vup = vup_in;
    vdn = vdn_in;
    lat = lat_in;
    lon = lon_in;
    parseCode = parseCode_in;
  }

  public LocationRecord(String rawRecord) {
      String[] line = rawRecord.split("\\|", -1);

      if (line.length != 20) {
        parseCode = ParseErrors.BAD_INPUT_LINE;
        return;
      }

      imsi      = parseString(line[0]);
      imei      = parseString(line[1]);
      tnOrig    = parseString(line[2]);
      tnTerm    = parseString(line[3]);
      location  = parseString(line[4]);
      geohash   = parseString(line[5]);
      type      = parseLocationType(line[6]);
      seq       = parseInteger(line[7]);
      ts        = parseInteger(line[8]);
      dur       = parseInteger(line[9]);
      subtype   = parseInteger(line[10]);
      cft       = parseInteger(line[11]);
      ct        = parseInteger(line[12]);
      acc       = parseInteger(line[13]);
      use       = parseInteger(line[14]);
      vup       = parseInteger(line[15]);
      vdn       = parseInteger(line[16]);
      lat       = parseDouble(line[17]);
      lon       = parseDouble(line[18]);
      parseCode = parseParseErrors(line[19]);
  }

  public String createImsiKey() {
    return stringToString(imsi);
  }

  public String createRawRecord() {
    String output = stringToString(imei) + "|" + stringToString(tnOrig) + "|" +
                    stringToString(tnTerm) + "|" + stringToString(location) + "|" +
                    stringToString(geohash) + "|" + locationTypeToString(type) + "|" +
                    intToString(seq) + "|" + intToString(ts) + "|" + intToString(dur) + "|" +
                    intToString(subtype) + "|" + intToString(cft) + "|" + intToString(ct) + "|" +
                    intToString(acc) + "|" + intToString(use) + "|" + intToString(vup) + "|" +
                    intToString(vdn) + "|" + doubleToString(lat) + "|" + doubleToString(lon) + "|" +
                    parseErrorsToString(parseCode);
    return output;
  }

  public String createFlatRecord() {
    String output = intToString(ts) + "|" + intToString(seq) + "|" +
                    intToString(dur) + "|" + stringToString(location) + "|" +
                    intToString(locationTypeToInt(type)) + "|" + intToString(subtype) + "|" +
                    intToString(cft) + "|" + intToString(ct) + "|" +
                    intToString(acc) + "|" + intToString(use) + "|" +
                    doubleToString(lat) + "|" + doubleToString(lon);
    return output;
  }

  public String createHbaseValue() {
    String output = intToString(type.ordinal()) + "|" + intToString(dur) + "|" +
                    intToString(subtype) + "|" + doubleToString(lat) + "|" +
                    doubleToString(lon);
    return output;
  }


  public boolean parseErrorOkay() {
    boolean output = false;
    if(parseCode == ParseErrors.FINISH_OKAY ||
        parseCode == ParseErrors.FINISH_OKAY_NON_ATT) {
      output = true;
    }
    return output;
  }

  private String parseString(String input) {
    if (!input.equals("")) {
      return(input);
    } else {
      return(null);
    }
  }
  private Integer parseInteger(String input) {
    try {
      return(Integer.parseInt(input));
    } catch (Exception e) {
      return(null);
    }
  }
  private Double parseDouble(String input) {
    try {
      return(Double.parseDouble(input));
    } catch (Exception e) {
      return(null);
    }
  }
  private LocationType parseLocationType(String input) {
    try {
      return(LocationType.valueOf(input));
    } catch (Exception e) {
      return(null);
    }
  }
  private ParseErrors parseParseErrors(String input) {
    try {
      return(ParseErrors.valueOf(input));
    } catch (Exception e) {
      return(null);
    }
  }
  private String intToString(Integer input) {
    if (input != null) {
      return(Integer.toString(input));
    } else {
      return("");
    }
  }
  private String doubleToString(Double input) {
    if (input != null) {
      return(Double.toString(input));
    } else {
      return("");
    }
  }
  private String stringToString(String input) {
    if (input != null) {
      return(input);
    } else {
      return("");
    }
  }
  private Integer locationTypeToInt(LocationType input) {
    if (input != null) {
      return(input.ordinal());
    } else {
      return(null);
    }
  }
  private Integer parseErrorsToInt(ParseErrors input) {
    if (input != null) {
      return(input.ordinal());
    } else {
      return(null);
    }
  }
  private String locationTypeToString(LocationType input) {
    if (input != null) {
      return(input.toString());
    } else {
      return(null);
    }
  }
  private String parseErrorsToString(ParseErrors input) {
    if (input != null) {
      return(input.toString());
    } else {
      return(null);
    }
  }
}
