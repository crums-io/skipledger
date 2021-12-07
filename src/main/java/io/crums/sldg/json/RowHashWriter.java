/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;


import java.nio.ByteBuffer;

import io.crums.sldg.RowHash;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SldgConstants;
import io.crums.util.IntegralStrings;
import io.crums.util.json.JsonEntityWriter;
import io.crums.util.json.simple.JSONObject;


/**
 * 
 */
public class RowHashWriter implements JsonEntityWriter<RowHash> {
  
  public final static RowHashWriter INSTANCE = new RowHashWriter();
  
  public final static String RN = "rn";
  public final static String RH = "rh";

  
  
  public JSONObject injectRowHash(long rn, ByteBuffer rowHash, JSONObject jObj) {
    SkipLedger.checkRealRowNumber(rn);
    if (rowHash.remaining() != SldgConstants.HASH_WIDTH)
      throw new IllegalArgumentException(
          "rowHash buffer with " + rowHash.remaining() + " bytes: " + rowHash);
    return injectImpl(rn, rowHash, jObj);
  }
  
  
  @SuppressWarnings("unchecked")
  private JSONObject injectImpl(long rn, ByteBuffer rowHash, JSONObject jObj) {
    jObj.put(RN, rn);
    jObj.put(RH, IntegralStrings.toHex(rowHash));
    return jObj;
  }
  

  @Override
  public JSONObject injectEntity(RowHash row, JSONObject jObj) {
    return injectImpl(row.rowNumber(), row.hash(), jObj);
  }

}
