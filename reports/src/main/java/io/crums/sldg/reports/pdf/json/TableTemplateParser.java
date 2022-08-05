/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;

import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Consumer;

import io.crums.sldg.reports.pdf.CellData;
import io.crums.sldg.reports.pdf.ColumnTemplate;
import io.crums.sldg.reports.pdf.LineSpec;
import io.crums.sldg.reports.pdf.TableTemplate;
import io.crums.util.Lists;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class TableTemplateParser implements ContextedParser<TableTemplate> {
  
  public final static TableTemplateParser INSTANCE = new TableTemplateParser();
  
  public final static String COLS = "cols";
  
  public final static String CC = "cc";
  public final static String CDEF = "cdef";

  public final static String COL_WIDTHS = "colWidths";
  

  public final static String GRID_LINES = "gridLines";
  
  public final static String BORDERS = "borders";
  public final static String BORDER_H = "borderH";
  public final static String BORDER_V = "borderV";

  public final static String LINES = "lines";
  public final static String LINE_H = "lineH";
  public final static String LINE_V = "lineV";


  public final static String INDEX = "index";
  public final static String CELLS = "cells";
  
  
  

  @Override
  public TableTemplate toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
    TableTemplate table;
    {
      List<ColumnTemplate> columns;
      var jCols = JsonUtils.getJsonArray(jObj, COLS, false);
      if (jCols == null) {
        int count = JsonUtils.getInt(jObj, CC);
        if (count < 1)
          throw new JsonParsingException("illegal '" + CC + "': " + count);
        var column =
            ColumnTemplateParser.INSTANCE.toEntity(
                JsonUtils.getJsonObject(jObj, CDEF, true),
                context) ;
        columns = Lists.repeatedList(column, count);
      } else if (jCols.isEmpty())
        throw new JsonParsingException("empty '" + COLS + "'");
      else
        columns = ColumnTemplateParser.INSTANCE.toEntityList(jCols, context);
      
      table = new TableTemplate(columns);
    }
    
    var jWidths = JsonUtils.getJsonArray(jObj, COL_WIDTHS, false);
    if (jWidths != null)
      table.setColumnWidths(JsonUtils.toNumbers(jWidths));
    
    setLine(table::setGridLines, GRID_LINES, jObj, context);
    setLine(table::setTableBorders, BORDERS, jObj, context);
    setLine(table::setHorizontalBorder, BORDER_H, jObj, context);
    setLine(table::setVerticalBorder, BORDER_V, jObj, context);
    setLine(table::setDefaultLines, LINES, jObj, context);
    setLine(table::setDefaultHorizontalLine, LINE_H, jObj, context);
    setLine(table::setDefaultVerticalLine, LINE_V, jObj, context);
    
    var jCells = JsonUtils.getJsonArray(jObj, CELLS, false);
    if (jCells != null) try {
      for (var o : jCells) {
        var jCell = (JSONObject) o;
        int index = JsonUtils.getInt(jCell, INDEX);
        var fixedCell = CellDataParser.INSTANCE.toCellData(jCell, context);
        table.setFixedCell(index, fixedCell);
      }
    } catch (IllegalArgumentException | IndexOutOfBoundsException | UncheckedIOException x) {
      throw new JsonParsingException("on parsing '" + CELLS + "': " + x.getMessage(), x);
    } catch (ClassCastException ccx) {
      throw new JsonParsingException("type mismatch in JSON array: " + ccx.getMessage(), ccx);
    }
    
    return table;
  }
  

  private void setLine(Consumer<LineSpec> setter, String jName, JSONObject jObj, RefContext context) {
    var jLine = JsonUtils.getJsonObject(jObj, jName, false);
    if (jLine != null)
      setter.accept(LineSpecParser.INSTANCE.toLineSpec(jLine, context));
  }
  
  
  
  
  
  

  @Override
  public JSONObject injectEntity(TableTemplate table, JSONObject jObj, RefContext context) {
    var columns = table.getColumns();
    boolean sameElements;
    {
      boolean same = true;
      var sample = columns.get(0);
      for (int index = columns.size(); index-- > 1 && same; )
        same = columns.get(index).equals(sample);
      sameElements = same;
    }
    var colParser = ColumnTemplateParser.INSTANCE;
    if (sameElements) {
      jObj.put(CC, columns.size());
      jObj.put(CDEF, colParser.toJsonObject(columns.get(0), context));
    } else {
      jObj.put(COLS, colParser.toJsonArray(columns, context));
    }
    
    table.getColumnWidths().ifPresent(
        widths -> {
          var jArray = new JSONArray(widths.length);
          for (float w : widths) {
            jArray.add(w);
          }
          jObj.put(COL_WIDTHS, jArray);
        });

    // set grid lines
    if (table.sameGridLines()) {
      putLine(GRID_LINES, table.getVerticalBorder(), jObj, context);
    } else {
      if (table.sameBorders()) {
        putLine(BORDERS, table.getVerticalBorder(), jObj, context);
      } else {
        putLine(BORDER_H, table.getHorizontalBorder(), jObj, context);
        putLine(BORDER_V, table.getVerticalBorder(), jObj, context);
      }
      
      if (table.sameDefaultLines()) {
        putLine(LINES, table.getDefaultVerticalLine(), jObj, context);
      } else {
        putLine(LINE_H, table.getDefaultHorizontalLine(), jObj, context);
        putLine(LINE_V, table.getDefaultVerticalLine(), jObj, context);
      }
    }

    // set the cells last, our json is in insertion-order (just for clarity)
    var fixedCells = table.getFixedCells();
    if (!fixedCells.isEmpty()) {
      var jCells = new JSONArray();
      fixedCells.entrySet().forEach(e -> jCells.add(entryJsonObj(e, context)));
      jObj.put(CELLS, jCells);
    }
    
    return jObj;
  }

  
  private JSONObject  entryJsonObj(Entry<Integer, CellData> e, RefContext context) {
    var cObj = new JSONObject();
    cObj.put(INDEX, e.getKey());
    return CellDataParser.INSTANCE.injectCellData(e.getValue(), cObj, context);
  }
  
  private void putLine(String name, LineSpec line, JSONObject jObj, RefContext context) {
    if (!line.isBlank())
      jObj.put(name, LineSpecParser.INSTANCE.toJsonObject(line, context));
  }

}
