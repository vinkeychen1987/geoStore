package com.att.research.GeoStore;

import ch.hsr.geohash.GeoHash;

/**
 * A class for holding a single location record. That is, it represents
 * an (entity, location, timestamp) triple, along with other metadata
 * concerning the locate.
 *
 * @author Taylor Arnold
 * @see Saver
 * @since 0.1
 */
public class LocationRecord
{
  /** Attributes of a location record */
  public String entity = null;
  public String record = null;
  public Long timestamp = null;
  public Double lat = null;
  public Double lon = null;
  public String geohash = null;

  /**
   * Explicit constructor for LocationRecord; calculates
   * geohash from the latitude and longitude
   *
   */
  public LocationRecord(String entity_in,
                        String record_in,
                        Long timestamp_in,
                        Double lat_in,
                        Double lon_in)
  {
    entity = entity_in;
    record = record_in;
    timestamp = timestamp_in;
    lat = lat_in;
    lon = lon_in;
    try {
      geohash = GeoHash.withBitPrecision(lat, lon, 50).toBase32();
    } catch (Exception e) {
      geohash = "";
    }
  }

  /**
   * Explicit constructor for LocationRecord; takes geohash
   * as an input.
   *
   */
  public LocationRecord(String entity_in,
                        String record_in,
                        Long timestamp_in,
                        Double lat_in,
                        Double lon_in,
                        String geohash_in)
  {
    entity = entity_in;
    record = record_in;
    timestamp = timestamp_in;
    lat = lat_in;
    lon = lon_in;
    geohash = geohash_in;
  }

  /**
   * Parse LocationRecord from a string.
   *
   */
  public LocationRecord(String input)
  {
    String[] inputArray = input.split("\\|", -1);
    if (inputArray.length >= 5) {
      entity = inputArray[0];
      record = inputArray[1];
      // timestamp = inputArray[2];
      // lat = inputArray[3];
      // lon = inputArray[4];
    } else {
      return;
    }
    if (inputArray.length >= 6) {
      geohash = inputArray[5];
    } else {
      try {
        geohash = GeoHash.withBitPrecision(lat, lon, 50).toBase32();
      } catch (Exception e) {
        geohash = "";
      }
    }
  }

  public String createFlatKey() {
    return (entity);
  }

  public String createEntityHBaseKey() {
    return (entity);
  }

  public String createGeoHBaseKey() {
    return (entity);
  }

  public String createEntityHBaseCol() {
    return (entity);
  }

  public String createGeoHBaseCol() {
    return (entity);
  }

  public String createFlatValue() {
    return (entity);
  }

  public String createEntityHBaseValue() {
    return (entity);
  }

  public String createGeoHBaseValue() {
    return (entity);
  }

}
