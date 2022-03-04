/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import static io.crums.util.json.JsonUtils.*;

import java.util.List;
import java.util.function.Consumer;

import io.crums.reports.pdf.CellFormat;
import io.crums.reports.pdf.LineSpec;
import io.crums.reports.pdf.TableTemplate;
import io.crums.util.Lists;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class TableTemplateParser implements JsonEntityParser<TableTemplate> {

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

  
  
  
  @Override
  public JSONObject injectEntity(TableTemplate table, JSONObject jObj) {
    return injectTableTemplate(table, jObj, RefContext.EMPTY);
  }
  
  
  
  public JSONObject toJsonObject(TableTemplate table, RefContext context) {
    return injectTableTemplate(table, new JSONObject(), context);
  }
  
  
  public JSONObject injectTableTemplate(TableTemplate table, JSONObject jObj, RefContext context) {
    var columns = table.getColumns();
    boolean sameElements;
    {
      boolean same = true;
      var sample = columns.get(0);
      for (int index = columns.size(); index-- > 1 && same; )
        same = columns.get(index).equals(sample);
      sameElements = same;
    }
    var colParser = CellFormatParser.INSTANCE;
    if (sameElements) {
      jObj.put(CC, columns.size());
      jObj.put(CDEF, colParser.toJsonObject(columns.get(0), context));
    } else {
      jObj.put(COLS, colParser.toJsonArray(columns, context));
    }
    
    var colWidths = table.getColumnWidths();
    if (colWidths.isPresent()) {
      var jWidths = new JSONArray();
      for (var width : colWidths.get())
        jWidths.add(width);
      jObj.put(COL_WIDTHS, jWidths);
    }
    
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
    return jObj;
  }
  
  
  private void putLine(String name, LineSpec line, JSONObject jObj, RefContext context) {
    if (line.isBlank())
      return;
    jObj.put(name, LineSpecParser.INSTANCE.toJsonObject(line, context));
  }

  @Override
  public TableTemplate toEntity(JSONObject jObj) throws JsonParsingException {
    return toTableTemplate(jObj, RefContext.EMPTY);
  }
  
  
  
  public TableTemplate toTableTemplate(JSONObject jObj, RefContext context) throws JsonParsingException {
    TableTemplate table;
    {
      List<CellFormat> columns;
      var jCols = getJsonArray(jObj, COLS, false);
      if (jCols == null) {
        int count = getInt(jObj, CC);
        if (count < 1)
          throw new JsonParsingException("illegal '" + CC + "': " + count);
        var jColumn = getJsonObject(jObj, CDEF, true);
        var column = CellFormatParser.INSTANCE.toCellFormat(jColumn, context);
        columns = Lists.repeatedList(column, count);
      } else if (jCols.isEmpty()) {
        throw new JsonParsingException("empty '" + COLS + "'");
      } else {
        columns = CellFormatParser.INSTANCE.toEntityList(jCols, context);
      }
      table = new TableTemplate(columns);
    }
    
    var jWidths = getJsonArray(jObj, COL_WIDTHS, false);
    
    if (jWidths != null)
      table.setColumnWidths(JsonUtils.toNumbers(jWidths));
    setLine(table::setGridLines, GRID_LINES, jObj, context);
    setLine(table::setTableBorders, BORDERS, jObj, context);
    setLine(table::setHorizontalBorder, BORDER_H, jObj, context);
    setLine(table::setVerticalBorder, BORDER_V, jObj, context);
    setLine(table::setDefaultLines, LINES, jObj, context);
    setLine(table::setDefaultHorizontalLine, LINE_H, jObj, context);
    setLine(table::setDefaultVerticalLine, LINE_V, jObj, context);
    
    return table;
  }
  
  private void setLine(Consumer<LineSpec> setter, String jName, JSONObject jObj, RefContext context) {
    var jLine = getJsonObject(jObj, jName, false);
    if (jLine != null)
      setter.accept(LineSpecParser.INSTANCE.toLineSpec(jLine, context));
  }

}
