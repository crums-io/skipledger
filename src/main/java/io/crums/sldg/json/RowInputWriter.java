/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;


import java.nio.ByteBuffer;

import io.crums.sldg.Row;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SldgConstants;
import io.crums.util.IntegralStrings;
import io.crums.util.json.JsonEntityWriter;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class RowInputWriter implements JsonEntityWriter<Row> {
  
  public final static RowInputWriter INSTANCE = new RowInputWriter();
  
  public final static String RN = RowHashWriter.RN;
  
  public final static String IH = "ih";

  @Override
  public JSONObject injectEntity(Row row, JSONObject jObj) {
    return injectImpl(row.rowNumber(), row.inputHash(), jObj);
  }
  
  
  private JSONObject injectImpl(long rn, ByteBuffer inputHash, JSONObject jObj) {
    jObj.put(RN, rn);
    jObj.put(IH, IntegralStrings.toHex(inputHash));
    return jObj;
  }
  
  
  public JSONObject injectInputHash(long rn, ByteBuffer inputHash, JSONObject jObj) {
    SkipLedger.checkRealRowNumber(rn);
    if (inputHash.remaining() != SldgConstants.HASH_WIDTH)
      throw new IllegalArgumentException(
          "inputHash buffer with " + inputHash.remaining() + " bytes: " + inputHash);
    return injectImpl(rn, inputHash, jObj);
  }

}
