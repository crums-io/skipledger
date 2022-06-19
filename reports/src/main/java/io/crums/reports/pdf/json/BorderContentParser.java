/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import java.util.Locale;

import io.crums.reports.pdf.Align;
import io.crums.reports.pdf.BorderContent;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class BorderContentParser implements ContextedParser<BorderContent> {
  
  public final static BorderContentParser INSTANCE = new BorderContentParser();
  
  public final static String TEXT = "text";
  public final static String FONT = "font";
  public final static String ALIGN_H = "alignH";
  public final static String PAGINATE = "paginate";
  
  
  public JSONObject injectBorderContent(BorderContent borderContent, JSONObject jObj, RefContext context) {
    jObj.put(TEXT, borderContent.getPhrase());
    jObj.put(FONT, FontSpecParser.INSTANCE.toJsonObject(borderContent.getFont(), context));
    if (!borderContent.getAlignment().isDefault())
      jObj.put(ALIGN_H, borderContent.getAlignment().name().toLowerCase(Locale.ROOT));
    if (borderContent.isPaginated())
      jObj.put(PAGINATE, true);
    return jObj;
  }
  
  
  public BorderContent toBorderContent(JSONObject jObj, RefContext context) throws JsonParsingException {
    var phrase = JsonUtils.getString(jObj, TEXT, true);
    var font = FontSpecParser.INSTANCE.toFontSpec(JsonUtils.getJsonObject(jObj, FONT, true), context);
    Align.H align;
    {
      var alignment = JsonUtils.getString(jObj, ALIGN_H, false);
      align = alignment == null ? Align.DEFAULT_H : Align.H.valueOf(alignment.toUpperCase(Locale.ROOT));
    }
    boolean paginate = JsonUtils.getBoolean(jObj, PAGINATE, false);
    return new BorderContent(phrase, font, align, paginate);
  }


  @Override
  public BorderContent toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
    return toBorderContent(jObj, context);
  }


  @Override
  public JSONObject injectEntity(BorderContent borderContent, JSONObject jObj, RefContext context) {
    return injectBorderContent(borderContent, jObj, context);
  }
  

}







