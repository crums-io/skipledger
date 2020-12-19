/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.json;


import java.nio.ByteBuffer;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import io.crums.sldg.Ledger;
import io.crums.sldg.Row;
import io.crums.sldg.SerialRow;
import io.crums.sldg.SldgConstants;
import io.crums.util.IntegralStrings;

/**
 * 
 */
public class RowParser {
  
  /**
   * Row number tag.
   */
  public final static String RN = "rn";
  
  /**
   * Entry hash tag.
   */
  public final static String E = "e";
  
  /**
   * Skip pointers array tag.
   */
  public final static String P = "p";

  
  /**
   * Instances of the base class are stateless.
   */
  public final static RowParser INSTANCE = new RowParser();
  
  
  
  
  
  
  @SuppressWarnings("unchecked")
  public JSONObject toJsonObject(Row row) {
    
    JSONObject jRow = new JSONObject();
    jRow.put(RN, row.rowNumber());
    jRow.put(E, IntegralStrings.toHex(row.inputHash()));
    
    JSONArray ptrs = new JSONArray();
    for (int level = 0, count = row.prevLevels(); level < count; ++level)
      ptrs.add( IntegralStrings.toHex(row.prevHash(level)) );
    
    jRow.put(P, ptrs);
    return jRow;
  }
  
  
  
  public Row toRow(String json) {
    try {
      return toRow((JSONObject) new JSONParser().parse(json));
    } catch (ParseException px) {
      throw new IllegalArgumentException("malformed json: " + json);
    }
  }
  
  
  
  public Row toRow(JSONObject jObj) {
    try {
      long rowNumber = ((Number) jObj.get(RN)).longValue();
      int ptrs = Ledger.skipCount(rowNumber);
      
      ByteBuffer data = ByteBuffer.allocate(SldgConstants.HASH_WIDTH * (1 + ptrs));
      data.put( IntegralStrings.hexToBytes(jObj.get(E).toString()) );
      
      JSONArray jPtrs = (JSONArray) jObj.get(P);
      for (int level = 0; level < ptrs; ++level)
        data.put( IntegralStrings.hexToBytes(jPtrs.get(level).toString()) );
      
      return new SerialRow(rowNumber, data.flip());
      
    } catch (RuntimeException rx) {
      throw new IllegalArgumentException("row json: " + jObj, rx);
    }
  }
  
  

}