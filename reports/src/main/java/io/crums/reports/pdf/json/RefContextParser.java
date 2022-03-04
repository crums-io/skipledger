/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import static io.crums.util.json.JsonUtils.getJsonObject;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.simple.JSONObject;

/**
 * Parses {@linkplain RefContext}s. This itself is <em>not</em> a {@linkplain RefContextedParser}
 * instance, however it's well aware of that interface: as it builds it's reference dictionary,
 * it allows newly referenced objects to be composed of already defined references.
 * <p>
 * Order, therefore matters. In the ordered list below, each category may optionally reference
 * objects in the previous categories:
 * </p><p>
 * <ol>
 * <li>Colors</li>
 * <li>Fonts</li>
 * <li>Cell Formats</li>
 * <li>Cell Data (which may also reference images)</li>
 * </ol>
 * Image bytes are stored externally and are not represented as JSON.
 * </p>
 */
public class RefContextParser implements JsonEntityParser<RefContext> {
  
  /**
   * Stateless, sans-image-ref instance.
   * 
   * @see #toRefContext(JSONObject, Map)
   */
  public final static RefContextParser INSTANCE = new RefContextParser();
  
  
  public final static String COLORS = "colors";
  public final static String FONTS = "fonts";
  public final static String CELL_FORMATS = "cellFormats";
  public final static String CELLS = "cells";
  
  
  
  
  private final Map<String, ByteBuffer> imageRefs;
  
  
  
  public RefContextParser() {
    imageRefs = Map.of();
  }
  
  
  public RefContextParser(Map<String, ByteBuffer> imageRefs) {
    this.imageRefs = Objects.requireNonNull(imageRefs, "null imageRefs");
  }
  
  
  
  @Override
  public JSONObject injectEntity(RefContext context, JSONObject jObj) {
    injectColors(context, jObj);
    injectFonts(context, jObj);
    injectCellFormats(context, jObj);
    injectCellData(context, jObj);
    return jObj;
  }
  
  
  private void injectColors(RefContext context, JSONObject jObj) {
    var colorRefs = context.colorRefs();
    if (colorRefs.isEmpty())
      return;
    var jRefs = new JSONObject();
    for (var e : colorRefs.entrySet())
      jRefs.put(e.getKey(), ColorParser.INSTANCE.toJsonObject(e.getValue()));
    jObj.put(COLORS, jRefs);
  }
  
  private void injectFonts(RefContext context, JSONObject jObj) {
    var fontRefs = context.fontRefs();
    if (fontRefs.isEmpty())
      return;
    var jRefs = new JSONObject();
    var maskedCtx = RefContext.of(null, null, null, context.colorRefs(), null);
    for (var e : fontRefs.entrySet())
      jRefs.put(e.getKey(), FontSpecParser.INSTANCE.toJsonObject(e.getValue(), maskedCtx));
    jObj.put(FONTS, jRefs);
  }
  
  
  private void injectCellFormats(RefContext context, JSONObject jObj) {
    var formatRefs = context.cellFormatRefs();
    if (formatRefs.isEmpty())
      return;
    var jRefs = new JSONObject();
    var maskedCtx = RefContext.of(null, null, null, context.colorRefs(), context.fontRefs());
    for (var e : formatRefs.entrySet())
      jRefs.put(e.getKey(), CellFormatParser.INSTANCE.toJsonObject(e.getValue(), maskedCtx));
    jObj.put(CELL_FORMATS, jRefs);
  }
  
  
  private void injectCellData(RefContext context, JSONObject jObj) {
    var cellRefs = context.cellDataRefs();
    if (cellRefs.isEmpty())
      return;
    var jRefs = new JSONObject();
    var maskedCtx = RefContext.of(
        null, context.cellFormatRefs(), null, context.colorRefs(), context.fontRefs());
    for (var e : cellRefs.entrySet())
      jRefs.put(e.getKey(), CellDataParser.SANS_REF_INSTANCE.toJsonObject(e.getValue(), maskedCtx));
    jObj.put(CELLS, jRefs);
  }
  
  

  @Override
  public RefContext toEntity(JSONObject jObj) throws JsonParsingException {
    return toRefContext(jObj, imageRefs);
  }
  
  
  /**
   * Parses and returns a {@linkplain RefContext} instance using the given image references
   * (loaded somehow externally).
   * 
   * @param jObj
   * @param imageRefs
   * @return
   */
  public RefContext toRefContext(JSONObject jObj, Map<String, ByteBuffer> imageRefs) {
    var context = new EditableRefContext(imageRefs);
    // order matters: successive layers can build on the previous ones
    buildColors(context, jObj);
    buildFonts(context, jObj);
    buildCellFormats(context, jObj);
    buildCellData(context, jObj);
    return context;
  }


  private void buildColors(RefContext context, JSONObject jObj) {
    buildMap(
        context.colorRefs(),
        getJsonObject(jObj, COLORS, false),
        ColorParser.INSTANCE,
        RefContext.EMPTY);
  }
  
  private void buildFonts(RefContext context, JSONObject jObj) {
    buildMap(
        context.fontRefs(),
        getJsonObject(jObj, FONTS, false),
        FontSpecParser.INSTANCE,
        context);
  }
  
  
  private void buildCellFormats(RefContext context, JSONObject jObj) {
    buildMap(
        context.cellFormatRefs(),
        getJsonObject(jObj, CELL_FORMATS, false),
        CellFormatParser.INSTANCE,
        context);
  }


  private void buildCellData(EditableRefContext context, JSONObject jObj) {
    buildMap(
        context.cellDataRefs(),
        getJsonObject(jObj, CELLS, false),
        CellDataParser.SANS_REF_INSTANCE,
        context);
  }
  
  
  private <T> void buildMap(Map<String,T> map, JSONObject jRefs, RefContextedParser<T> parser, RefContext context) {
    if (jRefs == null)
      return;
    try {
      
      jRefs.entrySet().forEach(e ->
          map.put(
              e.getKey().toString(),
              parser.toEntity((JSONObject) e.getValue(), context)));
    
    } catch (ClassCastException ccx) {
      throw new JsonParsingException("on parsing " + jRefs + ": " + ccx.getMessage(), ccx);
    }
  }

}

















