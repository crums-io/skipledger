/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import static io.crums.util.json.JsonUtils.getJsonObject;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

//import io.crums.sldg.reports.pdf.input.NumberArg;
import io.crums.sldg.reports.pdf.input.Param;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * Parses {@linkplain RefContext}s. This itself is <em>not</em> a {@linkplain ContextedParser}
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
 * Image bytes are stored externally and are not represented as JSON. They are designed
 * to be provided programmatically (or via configuration).
 * </p>
 * <h3>User Input</h3>
 * <p>
 * {@linkplain NumberArg}s represent user provided input. (There will be a {@code StringArg}
 * type in a later release.) {@code NumberArg}s are only ever defined in the JSON for a
 * {@linkplain RefContext} and are only <em>referenced</em> (never defined) in other JSON
 * objects.
 * </p><p>
 * The parser does not provide a direct way to build the context object pre-injected
 * with user input. (A development version did.) 2 reasons it doesn't:
 * <ol>
 * <li>It's the final (built) object's responsibility to interrogate the user for input.</li>
 * <li>It would force a 2-pass JSON build process: one to discover the necessary user input,
 * a second to provide it.</li>
 * </ol>
 * For these reasons, {@code NumberArgs} are constructed without input (they're intialized to
 * {@code 0} when their param does not provide a default).
 * </p>
 * 
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
  public final static String INPUTS = "inputs";
  
  public final static String INPUT_TYPE = "type";
  public final static String INPUT_TYPE_NUMBER = "number";
  public final static String INPUT_DESC = "desc";
  public final static String INPUT_DEFAULT = "default";
  
  
  
  
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
    injectParams(context, jObj);
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
    var maskedCtx = RefContext.of(null, null, null, context.colorRefs(), null, null);
    for (var e : fontRefs.entrySet())
      jRefs.put(e.getKey(), FontSpecParser.INSTANCE.toJsonObject(e.getValue(), maskedCtx));
    jObj.put(FONTS, jRefs);
  }
  
  
  private void injectCellFormats(RefContext context, JSONObject jObj) {
    var formatRefs = context.cellFormatRefs();
    if (formatRefs.isEmpty())
      return;
    var jRefs = new JSONObject();
    var maskedCtx = RefContext.of(null, null, null, context.colorRefs(), context.fontRefs(), null);
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
        null, context.cellFormatRefs(), null, context.colorRefs(), context.fontRefs(), null);
    for (var e : cellRefs.entrySet())
      jRefs.put(e.getKey(), CellDataParser.INSTANCE.toJsonObject(e.getValue(), maskedCtx));
    jObj.put(CELLS, jRefs);
  }
  
  
  private void injectParams(RefContext context, JSONObject jObj) {
    final int count = context.numberArgs().size();
    if (count == 0)
      return;
    var jParamMap = new JSONObject();
    for (var e : context.numberArgs().entrySet()) {
      var jParam = new JSONObject();
      jParam.put(INPUT_TYPE, INPUT_TYPE_NUMBER);
      var param = e.getValue().param();
      param.getDescription().ifPresent(desc -> jParam.put(INPUT_DESC, desc));
      param.getDefaultValue().ifPresent(value -> jParam.put(INPUT_DEFAULT, value));
      if (jParamMap.put(param.name(), jParam) != null)
        throw new IllegalArgumentException(
            "multiple parameters named '" + param.name() + "'");
    }
    jObj.put(INPUTS, jParamMap);
  }
  
  

  @Override
  public RefContext toEntity(JSONObject jObj) throws JsonParsingException {
    return toRefContext(jObj, imageRefs);
  }
  
  
  /**
   * Parses and returns a {@linkplain RefContext} instance using the given image references
   * (loaded somehow externally). The returned instance has no inputs.
   */
  public RefContext toRefContext(JSONObject jObj, Map<String, ByteBuffer> imageRefs) {
    var context = new EditableRefContext(imageRefs);
    buildParams(context, jObj);
    // order matters: successive layers can build on previous ones
    buildColors(context, jObj);
    buildFonts(context, jObj);
    buildCellFormats(context, jObj);
    buildCellData(context, jObj);
    return context;
  }



//  private void buildParams(EditableRefContext context, Map<String, Number> inputs, JSONObject jObj) {
  private void buildParams(EditableRefContext context, JSONObject jObj) {
    var jParams = JsonUtils.getJsonObject(jObj, INPUTS, false);
    if (jParams == null) {
      return;
    }
    for (var key : jParams.keySet()) {
      var name = key.toString();
      var jParam = JsonUtils.getJsonObject(jParams, name, true);
      {
        var type = JsonUtils.getString(jParam, INPUT_TYPE, true);
        if (! INPUT_TYPE_NUMBER.equals(type))
          throw new JsonParsingException("unknown '" + INPUT_TYPE + "': " + type);
      }
      var desc = JsonUtils.getString(jParam, INPUT_DESC, false);
      Number defaultValue = JsonUtils.getNumber(jParam, INPUT_DEFAULT, false);
//      Number inputValue = inputs.get(name);
//      if (inputValue == null && defaultValue == null)
//        throw new UnmatchedInputException("missing required input '" + name + "'");
      
      var param = new Param<Number>(name, desc, defaultValue);
      if (!context.putNumberArg(param))
        throw new JsonParsingException("duplicate definitions for param '" + param.name() + "'");
    }
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
        CellDataParser.INSTANCE,
        context);
  }
  
  
  
  
  private <T> void buildMap(Map<String,T> map, JSONObject jRefs, ContextedParser<T> parser, RefContext context) {
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

















