/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.depra.json;


import java.text.DateFormat;
import java.util.Date;

import io.crums.util.json.JsonEntityWriter;
import io.crums.util.json.simple.JSONObject;
import io.crums.model.json.CrumParser;
import io.crums.model.json.CrumTrailParser;
import io.crums.sldg.time.TrailedRow;
import io.crums.util.IntegralStrings;

/**
 * 
 */
public class TrailedRowWriter implements JsonEntityWriter<TrailedRow> {
  
  /**
   * Stateless default instance. UTC only; no {@linkplain #WIT} date.
   */
  public final static TrailedRowWriter DEFAULT_INSTANCE = new TrailedRowWriter();
  /**
   * Row number JSON tag.
   */
  public final static String RN = RowHashWriter.RN;
  /**
   * Row hash JSON tag. Hex value.
   */
  public final static String RH = RowHashWriter.RH;
  /**
   * Witness JSON tag. Present only if there's date-formatting.
   * Its value is a pretty date string.
   */
  public final static String WIT = "wit";
  /**
   * CrumTrail JSON tag.
   */
  public final static String TRAIL = "trail";
  /**
   * CrumTrail ref URL JSON tag.
   */
  public final static String TREF = "tref";
  /**
   * Trail root hash JSON tag.
   */
  public final static String TROOT = "troot";
  
  
  
  
  private final DateFormat witDateFormat;
  
  
  /**
   * Default constructor with no date formatter. UTC only; no {@linkplain #WIT} date.
   * Stateless.
   */
  public TrailedRowWriter() {
    this(null);
  }
  
  /**
   * Constructs an instance with the given optional date formatter.
   * 
   * @param witDateFormat may be null
   */
  public TrailedRowWriter(DateFormat witDateFormat) {
    this.witDateFormat = witDateFormat;
  }

  
  
  
  
  
  
  
  
  @Override
  public JSONObject injectEntity(TrailedRow trailedRow, JSONObject jObj) {
    return injectEntity(trailedRow, jObj, witDateFormat);
  }
  
  
  
  
  
  public JSONObject injectEntity(TrailedRow trailedRow, JSONObject jObj, DateFormat witDateFormat) {
    RowHashWriter.INSTANCE.injectEntity(trailedRow, jObj);
    if (witDateFormat != null)
      jObj.put(WIT, witDateFormat.format(new Date(trailedRow.utc())));
    jObj.put(TRAIL, CrumTrailParser.INSTANCE.toJsonObject(trailedRow.trail()));
    jObj.put(TREF, trailedRow.trail().getRefUrl());
    jObj.put(TROOT, IntegralStrings.toHex(trailedRow.trail().rootHash()));
    return jObj;
  }
  
  
  
  public JSONObject injectSlim(TrailedRow trailedRow, JSONObject jObj) {
    return injectSlim(trailedRow, jObj, witDateFormat);
  }
  

  public JSONObject injectSlim(
      TrailedRow trailedRow, JSONObject jObj, DateFormat witDateFormat) {

    RowHashWriter.INSTANCE.injectEntity(trailedRow, jObj);
    if (witDateFormat != null)
      jObj.put(WIT, witDateFormat.format(new Date(trailedRow.utc())));
    else
      jObj.put(CrumParser.UTC, trailedRow.utc());
    jObj.put(TREF, trailedRow.trail().getRefUrl());
    jObj.put(TROOT, IntegralStrings.toHex(trailedRow.trail().rootHash()));
    return jObj;
  }
  
  
  public JsonEntityWriter<TrailedRow> toSlim() {
    return new JsonEntityWriter<TrailedRow>() {
      @Override
      public JSONObject injectEntity(TrailedRow trailedRow, JSONObject jObj) {
        return injectSlim(trailedRow, jObj);
      }
    };
  }

}




