/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.util.ArrayList;
import java.util.List;

import io.crums.sldg.reports.pdf.CellDataProvider;
import io.crums.sldg.reports.pdf.CellFormat;
import io.crums.sldg.reports.pdf.SourcedCell;
import io.crums.sldg.reports.pdf.CellDataProvider.DateProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.ImageProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.NumberProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.StringProvider;
import io.crums.sldg.reports.pdf.SourcedCell.ColumnCell;
import io.crums.sldg.reports.pdf.SourcedCell.DateCell;
import io.crums.sldg.reports.pdf.SourcedCell.MultiStringCell;
import io.crums.sldg.reports.pdf.SourcedCell.NumberCell;
import io.crums.sldg.reports.pdf.SourcedCell.SourcedImage;
import io.crums.sldg.reports.pdf.SourcedCell.StringCell;
import io.crums.sldg.reports.pdf.SourcedCell.Sum;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class SourcedCellParser implements ContextedParser<SourcedCell> {
  
  public final static SourcedCellParser INSTANCE = new SourcedCellParser();
  
  public final static String TYPE = "type";

  public final static String STRING = "string";
  public final static String NUMBER = "number";
  public final static String MULTI = "multi";
  public final static String DATE = "date";
  public final static String IMAGE = "image";
  public final static String SUM = "sum";
  
  public final static String PROVIDER = "provider";
  public final static String FORMAT = "format";
  public final static String COL_INDEX = "colIndex";
  public final static String COL_INDEXES = "colIndexes";

  public final static String SEP = "sep";
  public final static String FUNC = "func";
  public final static String COL_FUNC = "colFunc";
  

  @Override
  public SourcedCell toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
    String type = JsonUtils.getString(jObj, TYPE, true);
    CellDataProvider<?> provider =
        CellDataProviderParser.INSTANCE.toEntity(
            JsonUtils.getJsonObject(jObj, PROVIDER, true));
    
    CellFormat format = CellFormatParser.INSTANCE.parseIfPresent(jObj, FORMAT, context);
    
    try {
      switch (type) {
      case STRING:
        return new StringCell(
            getColumnIndex(jObj),
            (StringProvider) provider,
            format);
      case NUMBER:
        return new NumberCell(
            getColumnIndex(jObj),
            (NumberProvider) provider,
            NumberFuncParser.INSTANCE.parseIfPresent(jObj, FUNC),
            format);
      case MULTI:
        return new MultiStringCell(
            getColumnIndexes(jObj),
            (StringProvider) provider,
            format).setSeparator(JsonUtils.getString(jObj, SEP, false));
      case DATE:
        return new DateCell(
            getColumnIndex(jObj),
            (DateProvider) provider,
            format);
      case SUM:
        return new Sum(
            getColumnIndexes(jObj),
            NumberFuncParser.INSTANCE.parseIfPresent(jObj, COL_FUNC),
            (NumberProvider) provider,
            NumberFuncParser.INSTANCE.parseIfPresent(jObj, FUNC),
            format);
      case IMAGE:
        return new SourcedImage(
            getColumnIndex(jObj),
            (ImageProvider) provider,
            format);
      default:
        throw new JsonParsingException("unrecognized '" + TYPE + "': " + type);
      }
    } catch (NullPointerException | IllegalArgumentException e) {
      throw new JsonParsingException(e);
    } catch (ClassCastException ccx) {
      throw new JsonParsingException("provider/type mismatch: " + ccx.getMessage(), ccx);
    }
  }
  
  
  private int getColumnIndex(JSONObject jObj) {
    return JsonUtils.getInt(jObj, COL_INDEX);
  }
  
  private List<Integer> getColumnIndexes(JSONObject jObj) {
    
    var jArray = JsonUtils.getList(jObj, COL_INDEXES, true);
    if (jArray.isEmpty())
      return List.of();
    var typed = new ArrayList<Integer>(jArray.size());
    for (var o : jArray) {
      if (o instanceof Integer i)
        typed.add(i);
      else if (o instanceof Number n)
        typed.add(n.intValue());
      else
        throw new JsonParsingException("expected integer; actuals was '" + o + "'");
    }
    return typed;
  }
  
  
  

  @Override
  public JSONObject injectEntity(SourcedCell cell, JSONObject jObj, RefContext context) {
    
    String type;
    ColumnCell generic = null;
    NumberCell num = null;
    MultiStringCell multi = null;
    Sum sum = null;
    if (cell instanceof StringCell c) {
      type = STRING;
      generic = c;
    } else if (cell instanceof NumberCell c) {
      type = NUMBER;
      generic = num = c;
    } else if (cell instanceof MultiStringCell m) {
      type = MULTI;
      multi = m;
    } else if (cell instanceof DateCell c) {
      type = DATE;
      generic = c;
    } else if (cell instanceof Sum s) {
      type = SUM;
      sum = s;
    } else if (cell instanceof SourcedImage i) {
      type = IMAGE;
      generic = i;
    } else
      throw new IllegalArgumentException(
          "unsupported sourced cell type " + cell.getClass() + ": " + cell);
    
    jObj.put(TYPE, type);
    if (generic != null) {
      jObj.put(COL_INDEX, generic.getColumnIndex());
      if (num != null && num.func() != null)
        jObj.put(FUNC, NumberFuncParser.INSTANCE.toJsonObject(num.func()));
    
    } else if (multi != null) {
      jObj.put(COL_INDEXES, multi.getColumnIndexes());
      var sep = multi.getSeparator();
      if (!MultiStringCell.DEFAULT_SEP.equals(sep))
        jObj.put(SEP, sep);
      
    } else if (sum != null) {
      jObj.put(COL_INDEXES, sum.getColumnIndexes());
      var colsFunc = sum.getColumnsFunc();
      if (colsFunc != null)
        jObj.put(COL_FUNC, NumberFuncParser.INSTANCE.toJsonObject(colsFunc));
      if (sum.func() != null)
        jObj.put(FUNC, NumberFuncParser.INSTANCE.toJsonObject(sum.func()));
    } else {
      throw new RuntimeException("Unaccounted cell type '" + cell.getClass() + "': " + cell);
    }

    jObj.put(PROVIDER, CellDataProviderParser.INSTANCE.toJsonObject(cell.provider()));
    cell.getFormat().ifPresent(
        f -> jObj.put(FORMAT, CellFormatParser.INSTANCE.toJsonObject(f, context)));
    
    return jObj;
  }

  
  
  
  
  
  

}
