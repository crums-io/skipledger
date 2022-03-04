/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import static io.crums.util.json.JsonUtils.getJsonObject;
import static io.crums.util.json.JsonUtils.getNumber;

import io.crums.reports.pdf.LineSpec;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class LineSpecParser implements RefContextedParser<LineSpec> {
  
  
  public final static LineSpecParser INSTANCE = new LineSpecParser();
  
  // For now, no references
//  public final static String LINE_REF = "lineRef";
  
  public final static String WIDTH = "width";
  public final static String COLOR = "color";
  

  
  public JSONObject injectLineSpec(LineSpec line, JSONObject jObj, RefContext context) {
    jObj.put(WIDTH, line.getWidth());
    if (line.hasColor())
      jObj.put(COLOR, ColorParser.INSTANCE.toJsonObject(line.getColor(), context));
    return jObj;
  }
  
  
  public LineSpec toLineSpec(JSONObject jObj, RefContext context) throws JsonParsingException {
    float width = getNumber(jObj, WIDTH, true).floatValue();
    var jColor = getJsonObject(jObj, COLOR, false);
    var color = jColor == null ? null : ColorParser.INSTANCE.toColor(jColor, context);
    
    try {
      return new LineSpec(width, color);
    
    } catch (IllegalArgumentException iax) {
      throw new JsonParsingException("after parsing " + jObj + ": " + iax.getMessage(), iax);
    }
  }


  @Override
  public LineSpec toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
    return toLineSpec(jObj, context);
  }


  @Override
  public JSONObject injectEntity(LineSpec entity, JSONObject jObj, RefContext context) {
    return injectLineSpec(entity, jObj, context);
  }

}
