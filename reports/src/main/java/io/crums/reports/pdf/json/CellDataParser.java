/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import java.nio.ByteBuffer;
import java.util.Map;

import io.crums.reports.pdf.CellData;
import io.crums.reports.pdf.CellFormat;
import io.crums.reports.pdf.CellData.ImageCell;
import io.crums.reports.pdf.CellData.TextCell;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class CellDataParser extends RefedImageParser<CellData>{
  
  public final static CellDataParser SANS_REF_INSTANCE = new CellDataParser();
  
  public final static String CELL_REF = "cellRef";
  
  public final static String TEXT = "text";
  
  public final static String IMG_REF = "imageRef";
  public final static String W = "w";
  public final static String H = "h";
  
  
  public final static String FORMAT = "format";
  
  
  
  
  
  
  
  
  
  public CellDataParser() {  }
  
  public CellDataParser(Map<String, ByteBuffer> refedImages) {
    super(refedImages);
  }
  
  
  public CellData toCellData(JSONObject jObj, RefContext context) throws JsonParsingException {
    try {
      return toCellDataImpl(jObj, context);
    } catch (JsonParsingException jsx) {
      throw jsx;
    } catch (Exception x) {
      throw new JsonParsingException("on parsing " + jObj + ": " + x.getMessage(), x);
    }
  }
  
  
  
  public TextCell toTextCell(JSONObject jObj, RefContext context) throws JsonParsingException {
    try {
      return (TextCell) toCellData(jObj, context);
    } catch (ClassCastException ccx) {
      throw new JsonParsingException("not a text cell: " + jObj);
    }
  }
  
  
  
  
  
  @Override
  public CellData toEntity(JSONObject jObj) throws JsonParsingException {
    return toCellData(jObj, RefContext.ofImageRefs(refedImages));
  }

  protected CellData toCellDataImpl(JSONObject jObj, RefContext context) {
    
    var ref = JsonUtils.getString(jObj, CELL_REF, false);
    if (ref != null)
      return context.getCellData(ref);
    
    CellFormat format;
    {
      var jFormat = JsonUtils.getJsonObject(jObj, FORMAT, false);
      format = jFormat == null ? null : CellFormatParser.INSTANCE.toCellFormat(jFormat, context);
    }
    String text = JsonUtils.getString(jObj, TEXT, false);
    if (text != null)
      return new TextCell(text, format);
    String imageRef = JsonUtils.getString(jObj, IMG_REF, false);
    if (imageRef != null) {
      var buffer = context.getBytes(imageRef);
      float width = JsonUtils.getNumber(jObj, W, true).floatValue();
      float height = JsonUtils.getNumber(jObj, H, true).floatValue();
      buffer.clear();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return CellData.forImage(imageRef, bytes, width, height, format);
    }
    throw new JsonParsingException("one of {" + TEXT + "," + IMG_REF + "} must be defined for cell data: " + jObj);
  }
  
  public JSONObject injectCellData(CellData cell, JSONObject jObj, RefContext context) {
    var ref = context.findRef(cell);
    if (ref.isPresent())
      jObj.put(CELL_REF, ref.get());
    else {
      if (cell instanceof TextCell txtCell)
        jObj.put(TEXT, txtCell.getText());
      else if (cell instanceof ImageCell imgCell) {
        jObj.put(IMG_REF, imgCell.getRef());
        jObj.put(W, imgCell.getWidth());
        jObj.put(H, imgCell.getHeight());
      } else
        throw new IllegalArgumentException("cell type not supported: " + cell);
      
      cell.getFormat().ifPresent(
          cf -> jObj.put(FORMAT, CellFormatParser.INSTANCE.toJsonObject(cf, context)));
    }
    return jObj;
  }
  
  
  

  @Override
  public CellData toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
    return toCellData(jObj, context);
  }

  @Override
  public JSONObject injectEntity(CellData entity, JSONObject jObj, RefContext context) {
    return injectCellData(entity, jObj, context);
  }
  

}
