/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;

import io.crums.util.json.simple.JSONObject;

import io.crums.sldg.RowHash;
import io.crums.util.IntegralStrings;


/**
 * 
 */
public class RowHashWriter implements JsonEntityWriter<RowHash> {
  
  public final static RowHashWriter INSTANCE = new RowHashWriter();
  
  public final static String RN = "rn";
  public final static String RH = "rh";

  

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject injectEntity(RowHash row, JSONObject jObj) {
    jObj.put(RN, row.rowNumber());
    jObj.put(RH, IntegralStrings.toHex(row.hash()));
    return jObj;
  }

}
