package com.att.research.GeoStore.client;

import java.lang.Math;
import java.lang.StringBuilder;
import java.lang.System;
import java.util.Date;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.ArrayList;

import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.BoundingBox;
import ch.hsr.geohash.util.GeoHashSizeTable;
import ch.hsr.geohash.util.TwoGeoHashBoundingBox;
import ch.hsr.geohash.util.BoundingBoxGeoHashIterator;

/**
 * A static class for calculating the temporal-geohash list needed to cover a given
 * bounding box.
 *
 * @author Taylor Arnold
 * @see QueryScanner
 * @since 0.3
 *
 */
public class GeoHashBox {

  /**
   * Returns an array of strings which give the scan ranges needed to search over
   * a particular geospatial-temporal box.
   *
   * @param   ts0   starting unix timestamp
   * @param   ts1   ending unix timestamp
   * @param   lat0  latitude at one corner of the geospatial box; can be larger or smaller latitude
   * @param   lat1  latitude at the other corner of the geospatial box
   * @param   lon0  longitude at one corner of the geospatial box; can be larger or smaller longitude
   * @param   lon1  longitude at the other corner of the geospatial box
   * @return  a String array, which should be interpreted as a two-column matrix in
   *          column major format. The first colum gives the start key and the second
   *          column gives the end key, which each row constituting a seperate scan.
   * @throws  Exception
   */
  public static String[] getBox(int ts0, int ts1, double lat0, double lat1, double lon0, double lon1)
      throws Exception {

    List<String> search_hashes = new ArrayList<String>();

    BoundingBox bb = new BoundingBox(lat0, lat1, lon0, lon1);
    int numbits = GeoHashSizeTable.numberOfBitsForOverlappingGeoHash(bb);
    TwoGeoHashBoundingBox tgh = TwoGeoHashBoundingBox.withBitPrecision(bb, 25);
    BoundingBoxGeoHashIterator iter = new BoundingBoxGeoHashIterator(tgh);

    while (iter.hasNext()) {
      GeoHash gh = iter.next();
      search_hashes.add(new StringBuilder(gh.toBase32().substring(0,5)).reverse().toString());
    }

    String day0_string = Integer.toString(ts0);
    String day1_string = Integer.toString(ts1) + "z";

    String[] search_index = new String[2 * search_hashes.size()];

    int shs = search_hashes.size();
    for (int i = 0; i < shs; i++) {
      search_index[i] = search_hashes.get(i) + day0_string;
      search_index[i + shs] = search_hashes.get(i) + day1_string;
    }

    return search_index;
  }
}
