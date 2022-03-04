/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;

import java.awt.Color;
import java.util.StringTokenizer;

import com.lowagie.text.Font;

import io.crums.reports.pdf.FontSpec;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class FontSpecParser implements RefContextedParser<FontSpec> {
  
  public final static FontSpecParser INSTANCE = new FontSpecParser();
  
  public final static String FONT_REF = "fontRef";
  
  public final static String NAME = "name";
  public final static String SIZE = "size";
  public final static String STYLE = "style";
  public final static String COLOR = "color";

  public final static String STYLE_DELIMITER = ";";
  public final static String BOLD = "bold";
  public final static String ITALIC = "italic";
  public final static String UNDERLINE = "underline";
  
  
  
  
  
  
  
  public FontSpec toFontSpec(JSONObject jObj, RefContext context) throws JsonParsingException {
    var ref = JsonUtils.getString(jObj, FONT_REF, false);
    if (ref != null)
      return context.getFont(ref);
    String name = JsonUtils.getString(jObj, NAME, true);
    float size = JsonUtils.getNumber(jObj, SIZE, true).floatValue();
    int intStyle = Font.NORMAL;
    {
      var style = JsonUtils.getString(jObj, STYLE, false);
      if (style != null && !style.isBlank()) {
        var tokenizer = new StringTokenizer(style, STYLE_DELIMITER + " ");
        while (tokenizer.hasMoreTokens()) {
          var token = tokenizer.nextToken();
          switch (token) {
          case BOLD:  intStyle |= Font.BOLD; break;
          case ITALIC: intStyle |= Font.ITALIC; break;
          case UNDERLINE: intStyle |= Font.UNDERLINE; break;
          default:
            throw new JsonParsingException("unrecognized font style: " + style);
          }
        }
      }
    }
    Color color;
    var jColor = JsonUtils.getJsonObject(jObj, COLOR, false);
    color = jColor == null ? Color.BLACK : ColorParser.INSTANCE.toColor(jColor, context);
    return new FontSpec(name, size, intStyle, color);
  }
  
  
  
  
  public JSONObject injectFontSpec(FontSpec font, JSONObject jObj, RefContext context) {
    var ref = context.findRef(font);
    if (ref.isPresent())
      jObj.put(FONT_REF, ref.get());
    else {
      jObj.put(NAME, font.getName());
      jObj.put(SIZE, font.getSize());
      
      int style = font.getStyle();
      if (style != Font.NORMAL) {
        StringBuilder jStyle = new StringBuilder(32);
        appendStyle(style, Font.BOLD, BOLD, jStyle);
        appendStyle(style, Font.ITALIC, ITALIC, jStyle);
        appendStyle(style, Font.UNDERLINE, UNDERLINE, jStyle);
        if (jStyle.length() != 0)
          jObj.put(STYLE, jStyle.toString());
      }
      
      Color color = font.getColor();
      if (!Color.BLACK.equals(color)) {
        var jColor = ColorParser.INSTANCE.toJsonObject(color, context);
        jObj.put(COLOR, jColor);
      }
    }
    return jObj;
  }
  


  private void appendStyle(int style, int targetStyle, String styleToken, StringBuilder jStyle) {
    if ((style & targetStyle) != 0) {
      if (jStyle.length() != 0)
        jStyle.append(STYLE_DELIMITER);
      jStyle.append(styleToken);
    }
  }


  @Override
  public FontSpec toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
    return toFontSpec(jObj, context);
  }


  @Override
  public JSONObject injectEntity(FontSpec entity, JSONObject jObj, RefContext context) {
    return injectFontSpec(entity, jObj, context);
  }

}







