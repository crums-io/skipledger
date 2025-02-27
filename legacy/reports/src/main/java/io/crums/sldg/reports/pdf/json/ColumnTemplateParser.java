/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;

import io.crums.sldg.reports.pdf.ColumnTemplate;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class ColumnTemplateParser implements ContextedParser<ColumnTemplate> {
  
  public final static ColumnTemplateParser INSTANCE = new ColumnTemplateParser();
  
  
  public final static String FORMAT = "format";
  public final static String PROTO_SRC = "protoSrc";

  @Override
  public ColumnTemplate toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
    var format =
        CellFormatParser.INSTANCE.toEntity(
            JsonUtils.getJsonObject(jObj, FORMAT, true),
            context);
    var proto = SourcedCellParser.INSTANCE.parseIfPresent(jObj, PROTO_SRC, context);
    
    return new ColumnTemplate(format, proto);
  }

  @Override
  public JSONObject injectEntity(ColumnTemplate column, JSONObject jObj, RefContext context) {
    jObj.put(
        FORMAT,
        CellFormatParser.INSTANCE.toJsonObject(column.getFormat(), context));
    if (column.usesSource())
      jObj.put(
          PROTO_SRC,
          SourcedCellParser.INSTANCE.toJsonObject(column.getProtoSrc(), context));
    return jObj;
  }

}
