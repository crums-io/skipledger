/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src.sql.config;


import java.util.Optional;

import io.crums.sldg.src.sql.SqlLedger;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * Ledger definition queries.
 * 
 * @see LedgerDef#LedgerDef(String, String, Optional)
 * @see LedgerDef#PARSER
 */
public record LedgerDef(
    String sizeQuery, String rowByNoQuery,
    Optional<Integer> maxBlobSize) {
  
  /** JSON parser. */
  public final static JsonEntityParser<LedgerDef> PARSER =  new Parser();
  
  
  /**
   * Full Constructor.
   * 
   * @param sizeQuery         {@code SELECT COUNT(*).. } query type
   * @param rowByNoQuery      row-by-number prepared-statement string
   * @param maxBlobSize       the maximum blob size in bytes
   * 
   * @see SqlLedger#DEFAULT_MAX_BLOB_SIZE
   */
  public LedgerDef {
    sizeQuery = sizeQuery.trim();
    rowByNoQuery = rowByNoQuery.trim();
    if (sizeQuery.isEmpty())
      throw new IllegalArgumentException("empty sizeQuery");
    if (rowByNoQuery.isEmpty())
      throw new IllegalArgumentException("empty rowByNoQuery");
    if (maxBlobSize.filter(sz -> sz < 0).isPresent())
      throw new IllegalArgumentException(
          "netative maxBlobSize: " + maxBlobSize.get());
  }
  
  

  /** JSON parser. */
  public static class Parser implements JsonEntityParser<LedgerDef> {
    
    public final static String SIZE_QUERY_KEY = "size_query";
    public final static String ROW_QUERY_KEY = "row_query";
    public final static String MAX_BLOB_SIZE = "max_blob_size";

    @Override
    public JSONObject injectEntity(LedgerDef def, JSONObject jObj) {
      jObj.put(SIZE_QUERY_KEY, def.sizeQuery());
      jObj.put(ROW_QUERY_KEY, def.rowByNoQuery());
      return jObj;
    }
  
    @Override
    public LedgerDef toEntity(JSONObject jObj) throws JsonParsingException {
      var sizeQuery = JsonUtils.getString(jObj, SIZE_QUERY_KEY, true);
      var rowQuery = JsonUtils.getString(jObj, ROW_QUERY_KEY, true);
      var maxBlobSize =
          Optional.ofNullable(JsonUtils.getNumber(jObj, MAX_BLOB_SIZE, false))
          .map(Number::intValue);
      
      try {
        return new LedgerDef(sizeQuery, rowQuery, maxBlobSize);
      } catch (Exception x) {
        throw new JsonParsingException(x);
      }
    }
    
  }

}




