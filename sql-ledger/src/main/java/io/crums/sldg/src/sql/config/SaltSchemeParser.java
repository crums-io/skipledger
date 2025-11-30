/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src.sql.config;

import io.crums.sldg.src.SaltScheme;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;

/**
 * {@linkplain SaltScheme} parser.
 * 
 */
public class SaltSchemeParser implements JsonEntityParser<SaltScheme> {
  
  public final static SaltSchemeParser INSTANCE = new SaltSchemeParser();
  
  /** @see SaltScheme#isPositive() */
  public final static String POSITVE = "ssip";
  /** @see SaltScheme#cellIndices() */
  public final static String INDICES = "ssi";
  
  

  @Override
  public JSONObject injectEntity(SaltScheme saltScheme, JSONObject jObj) {
    saltScheme = SaltScheme.wrap(saltScheme);
    
    jObj.put(POSITVE, saltScheme.isPositive());
    
    int[] indices = saltScheme.cellIndices();
    var jArray = new JSONArray(indices.length);
    for (var index : indices)
      jArray.add(index);
    
    jObj.put(INDICES, jArray);
    return jObj;
  }

  @Override
  public SaltScheme toEntity(JSONObject jObj) throws JsonParsingException {
    
    boolean positive = JsonUtils.getBoolean(jObj, POSITVE);
    var jArray = JsonUtils.getJsonArray(jObj, INDICES, true);
    int[] indices = new int[jArray.size()];
    {
      int index = 0;
      try {
        for (; index < indices.length; ++index)
          indices[index] = ((Number) jArray.get(index)).intValue();
      } catch (ClassCastException ccx) {
        throw new JsonParsingException(
            "expected integer value for %s[%d]; actual value is not a number: %s"
            .formatted(INDICES, index, jArray.get(index)));
      }
    }
    
    try {
      return SaltScheme.of(indices, positive);
    
    } catch (Exception x) {
      throw new JsonParsingException(
          "illegal salt scheme arguments: " + x.getMessage(), x);
    }
  }

}
