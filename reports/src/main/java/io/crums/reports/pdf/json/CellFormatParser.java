/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;

import static io.crums.util.json.JsonUtils.getBoolean;
import static io.crums.util.json.JsonUtils.getFloat;
import static io.crums.util.json.JsonUtils.getJsonObject;
import static io.crums.util.json.JsonUtils.getString;
import static io.crums.util.json.JsonUtils.putNonzero;
import static io.crums.util.json.JsonUtils.putPositive;

import java.util.Locale;

import io.crums.reports.pdf.Align;
import io.crums.reports.pdf.CellFormat;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class CellFormatParser implements RefContextedParser<CellFormat> {
  
  
  public final static CellFormatParser INSTANCE = new CellFormatParser();
  
  public final static String CF_REF = "cfRef";
  
  public final static String FONT = "font";

  public final static String LEAD = "lead";

  public final static String PAD = "pad";

  /**
   * Horizontal alignment.
   */
  public final static String ALIGN_H = "alignH";

  /**
   * Vertical alignment.
   */
  public final static String ALIGN_V = "alignV";

  public final static String ASCENDER = "ascender";

  public final static String DESCENDER = "descender";

  public final static String BORDERS = "borders";
  
  public final static String BG_COLOR = "bgColor";
  
  
  private final static boolean DEFAULT_ENDER = true;

  
  
  
  
  public JSONObject injectCellFormat(CellFormat format, JSONObject jObj, RefContext context) {
    var ref = context.findRef(format);
    if (ref.isPresent())
      jObj.put(CF_REF, ref.get());
    else {
      var font = format.getFont();
      var jFont = FontSpecParser.INSTANCE.toJsonObject(font, context);
      jObj.put(FONT, jFont);
      if (format.isAscender() != DEFAULT_ENDER)
        jObj.put(ASCENDER, format.isAscender());
      if (format.isDescender() != DEFAULT_ENDER)
        jObj.put(DESCENDER, format.isDescender());
      putNonzero(jObj, LEAD, format.getLeading());
      putPositive(jObj, PAD, format.getPadding());
      if (!format.getAlignH().isDefault())
        jObj.put(ALIGN_H, format.getAlignH().name().toLowerCase(Locale.ROOT));
      if (!format.getAlignV().isDefault())
        jObj.put(ALIGN_V, format.getAlignV().name().toLowerCase(Locale.ROOT));
      format.getBackgroundColor().ifPresent(
          c -> jObj.put(BG_COLOR, ColorParser.INSTANCE.toJsonObject(c, context)));
    }
    return jObj;
  }
  
  
  
  public CellFormat toCellFormat(JSONObject jObj, RefContext context) throws JsonParsingException {
    var ref = JsonUtils.getString(jObj, CF_REF, false);
    if (ref != null)
      return context.getCellFormat(ref);
    
    var font = FontSpecParser.INSTANCE.toFontSpec(getJsonObject(jObj, FONT, true), context);
    var format = new CellFormat(font);
    try {
      format.setLeading(getFloat(jObj, LEAD, 0));
      format.setPadding(getFloat(jObj, PAD, 0));
      
      var alignment = getString(jObj, ALIGN_H, false);
      if (alignment != null)
        format.setAlignH(Align.H.valueOf(alignment.toUpperCase(Locale.ROOT)));
      
      alignment = getString(jObj, ALIGN_V, false);
      if (alignment != null)
        format.setAlignV(Align.V.valueOf(alignment.toUpperCase(Locale.ROOT)));
      
      format.setAscender(getBoolean(jObj, ASCENDER, DEFAULT_ENDER));
      format.setDescender(getBoolean(jObj, DESCENDER, DEFAULT_ENDER));
      
    } catch (JsonParsingException jsx) {
      throw jsx;
    } catch (RuntimeException iax) {
      throw new JsonParsingException(iax);
    }
    
    var jBgColor = JsonUtils.getJsonObject(jObj, BG_COLOR, false);
    if (jBgColor != null)
      format.setBackgroundColor(ColorParser.INSTANCE.toColor(jBgColor, context));
    
    return format;
  }



  @Override
  public CellFormat toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
    return toCellFormat(jObj, context);
  }



  @Override
  public JSONObject injectEntity(CellFormat entity, JSONObject jObj, RefContext context) {
    return injectCellFormat(entity, jObj, context);
  }

}
