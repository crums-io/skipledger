/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import io.crums.reports.pdf.CellData;
import io.crums.reports.pdf.FixedTable;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONArray;
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
    TableTemplateParser.INSTANCE.injectTableTemplate(table, jObj, context);
    table.getDefaultCell().ifPresent(
        tc -> jObj.put(DEFAULT_CELL, CELL_PARSER.toJsonObject(tc, context)));
    if (!table.isEmpty()) {
      var jCells = new JSONArray();
      table.getNonDefaultFixedCells().entrySet()
        .forEach(e -> jCells.add(toJsonObject(e, context)));
      jObj.put(CELLS, jCells);
    }
    return jObj;
  }
  
  
  
  
  
  private JSONObject  toJsonObject(Entry<Integer, CellData> e, RefContext context) {
    var cObj = new JSONObject();
    cObj.put(INDEX, e.getKey());
    return CELL_PARSER.injectCellData(e.getValue(), cObj, context);
  }

  
  @Override
  public FixedTable toEntity(JSONObject jObj) throws JsonParsingException {
    return toFixedTable(jObj, refedImages);
  }
  
  
  
  
  public FixedTable toFixedTable(
      JSONObject jObj, Map<String,ByteBuffer> refedImages) throws JsonParsingException {
    
    return toFixedTable(jObj, RefContext.of(refedImages));
  }
  

  public FixedTable toFixedTable(JSONObject jObj, RefContext context) throws JsonParsingException {
    
    var protoTable = TableTemplateParser.INSTANCE.toTableTemplate(jObj, context);
    int rows = JsonUtils.getInt(jObj, RC);
    FixedTable fixedTable;
    try {
      fixedTable = new FixedTable(protoTable, rows);
    } catch (Exception x) {
      throw new JsonParsingException("on instantiation: " + x.getMessage(), x);
    }
    {
      var jCells = JsonUtils.getJsonArray(jObj, CELLS, false);
      if (jCells != null) try {
        for (var o : jCells) {
          var jCell = (JSONObject) o;
          int index = JsonUtils.getInt(jCell, INDEX);
          var fixedCell = CELL_PARSER.toCellData(jCell, context);
          fixedTable.setFixedCell(index, fixedCell);
        }
      } catch (IllegalArgumentException | IndexOutOfBoundsException | UncheckedIOException x) {
        throw new JsonParsingException("on parsing '" + CELLS + "': " + x.getMessage(), x);
      } catch (ClassCastException ccx) {
        throw new JsonParsingException("type mismatch in JSON array: " + ccx.getMessage(), ccx);
      }
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




