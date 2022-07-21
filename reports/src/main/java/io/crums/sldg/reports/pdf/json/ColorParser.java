/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.awt.Color;

import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class ColorParser implements ContextedParser<Color> {
  
  public final static ColorParser INSTANCE = new ColorParser();
  
  /** @deprecated does not belong inside the JSON object; should be ref'ed from outside, not here. */
  public final static String C_REF = "cRef";

  public final static String R = "r";
  public final static String B = "b";
  public final static String G = "g";
  
  
  
  public Color toColor(JSONObject jObj, RefContext context) throws JsonParsingException {
    var ref = JsonUtils.getString(jObj, C_REF, false);
    if (ref != null)
      return context.getColor(ref);
    int r = JsonUtils.getInt(jObj, R, 0);
    int g = JsonUtils.getInt(jObj, G, 0);
    int b = JsonUtils.getInt(jObj, B, 0);
    try {
      return new Color(r,g,b);
    } catch (IllegalArgumentException iax) {
      throw new JsonParsingException("Color construction failed: " + iax.getMessage(), iax);
    }
  }
  
  
  
  public JSONObject injectColor(Color color, JSONObject jObj, RefContext context) {
    var ref = context.findRef(color);
    if (ref.isPresent())
      jObj.put(C_REF, ref.get());
    else {
      int red = color.getRed();
      int green = color.getGreen();
      int blue = color.getBlue();
      if (red + green + blue == 0 || red != 0)
        jObj.put(R, color.getRed());
      if (green != 0)
        jObj.put(G, green);
      if (blue != 0)
        jObj.put(B, blue);
    }
    return jObj;
  }

  @Override
  public Color toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
    return toColor(jObj, context);
  }

  @Override
  public JSONObject injectEntity(Color entity, JSONObject jObj, RefContext context) {
    return injectColor(entity, jObj, context);
  }

}
