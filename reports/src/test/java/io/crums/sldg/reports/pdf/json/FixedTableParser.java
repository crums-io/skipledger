/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.nio.ByteBuffer;
import java.util.Map;

import io.crums.sldg.reports.pdf.FixedTable;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class FixedTableParser extends RefedImageParser<FixedTable> {
  
  /**
   * A stateless <em>sans ref</em> instance.
   */
  public final static FixedTableParser INSTANCE = new FixedTableParser();
  
  public final static String RC = "rc";
  public final static String DEFAULT_CELL = "defaultCell";
  public final static String INDEX = "index";
  public final static String CELLS = "cells";
  
  private final static CellDataParser CELL_PARSER = CellDataParser.SANS_REF_INSTANCE;
  
  
  
  /**
   * Creates an instance with no referenced images.
   * 
   * @see #INSTANCE
   */
  public FixedTableParser() {  }
  
  
  public FixedTableParser(Map<String, ByteBuffer> refedImages) {
    super(refedImages);
  }
  
  
  
  

  
  
  public JSONObject injectFixedTable(FixedTable table, JSONObject jObj, RefContext context) {
    jObj.put(RC, table.getRowCount());
    LegacyTableTemplateParser.INSTANCE.injectTableTemplate(table, jObj, context);
    table.getDefaultCell().ifPresent(
        tc -> jObj.put(DEFAULT_CELL, CELL_PARSER.toJsonObject(tc, context)));
    return jObj;
  }
  
  
  

  
  @Override
  public FixedTable toEntity(JSONObject jObj) throws JsonParsingException {
    return toFixedTable(jObj, refedImages);
  }
  
  
  
  
  public FixedTable toFixedTable(
      JSONObject jObj, Map<String,ByteBuffer> refedImages) throws JsonParsingException {
    
    return toFixedTable(jObj, RefContext.ofImageRefs(refedImages));
  }
  

  public FixedTable toFixedTable(JSONObject jObj, RefContext context) throws JsonParsingException {
    
    var protoTable = LegacyTableTemplateParser.INSTANCE.toTableTemplate(jObj, context);
    int rows = JsonUtils.getInt(jObj, RC);
    FixedTable fixedTable;
    try {
      fixedTable = new FixedTable(protoTable, rows);
    } catch (Exception x) {
      throw new JsonParsingException("on instantiation: " + x.getMessage(), x);
    }

    var jCell = JsonUtils.getJsonObject(jObj, DEFAULT_CELL, false);
    if (jCell != null)
      fixedTable.setDefaultCell(CELL_PARSER.toTextCell(jCell, context));
    
    return fixedTable;
  }


  @Override
  public FixedTable toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
    return toFixedTable(jObj, context);
  }


  @Override
  public JSONObject injectEntity(FixedTable entity, JSONObject jObj, RefContext context) {
    return injectFixedTable(entity, jObj, context);
  }

}




