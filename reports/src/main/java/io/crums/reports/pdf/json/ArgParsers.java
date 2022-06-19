/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import java.util.Map;
import java.util.function.Function;

import io.crums.reports.pdf.model.NumberArg;
import io.crums.reports.pdf.model.Param;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * Input-related (argument) parsers.
 */
public class ArgParsers {
  
  private ArgParsers() {  }
  
  

  /**
   * {@linkplain Param} parser.
   * <p>
   * The present implementation handles numbers and strings. These are used in
   * predicate parsing. Predicate handling for the latter category (strings) will
   * come later.
   * </p>
   */
  public static class ParamParser implements JsonEntityParser<Param<?>> {
    
    /** Instances are stateless. */
    public final static ParamParser INSTANCE = new ParamParser();
    
    public final static String NAME = "name";
    
    public final static String DESC = "desc";
    
    public final static String DEFAULT = "default";
    
    
    
    
  
    @Override
    public JSONObject injectEntity(Param<?> argName, JSONObject jObj) {
      jObj.put(NAME, argName.name());
      argName.getDescription().ifPresent(d -> jObj.put(DESC, d));
      argName.getDefaultValue().ifPresent(d -> jObj.put(DEFAULT, d));
      return jObj;
    }
  
    
    @Override
    public Param<?> toEntity(JSONObject jObj) throws JsonParsingException {
      String name = JsonUtils.getString(jObj, NAME, true);
      String desc = JsonUtils.getString(jObj, DESC, false);
      Object defaultVal = jObj.get(DEFAULT);
      if (defaultVal == null)
        return new Param<>(name, desc);
      if (defaultVal instanceof Number num)
        return new Param<>(name, desc, num);
      if (defaultVal instanceof String s)
        return new Param<>(name, desc, s);
      throw new JsonParsingException(
          "illegal default value setting. \"" + DEFAULT + "\": " + defaultVal);
    }
  
  }
  
  
  /**
   * {@code NumberArg} parser. This parser has an intentional quirk:
   * on the read path, it takes a number dictionary
   * (intended as input from outside the JSON).
   * 
   * @see NumberArgParser#toEntity(JSONObject, Map)
   */
  public static class NumberArgParser implements JsonEntityParser<NumberArg> {
    
    
    public final static NumberArgParser INSTANCE = new NumberArgParser();
    
    
    /**
     * On the write-path, the actual number value is not written; it's expected
     * to be later provided as input.
     */
    @Override
    public JSONObject injectEntity(NumberArg number, JSONObject jObj) {
      return jObj;
    }

    
    @Override
    public NumberArg toEntity(JSONObject jObj) throws JsonParsingException {
      return toEntity(jObj, defaultInput());
    }
    
    /**
     * Deserializes and returns an instance using <em>both</em> arguments.
     * 
     * @param jObj  in fact, only contains a {@code Param}
     * @param input look up function for input; may return {@code null}, in which case defaulted (from {@code ArgName}).
     * 
     * @throws JsonParsingException if the {@code input} returns null and paramater is not defaulted.
     */
    public NumberArg toEntity(JSONObject jObj, Function<String, Number> input) throws JsonParsingException {
      @SuppressWarnings("unchecked")
      var argName = (Param<Number>) ParamParser.INSTANCE.toEntity(jObj);
      Number value = input.apply(argName.name());
      if (value == null) {
        value = argName.defaultValue();
        if (value == null)
          throw new MissingInputException("missing input for parameter '" + argName.name() + "'");
      }
      return new NumberArg(argName, value);
    }
    
    
    /** Returns a "zero" function (one that always returns 0. */
    protected Function<String, Number> defaultInput() {
      return s -> 0;
    }
    
  }

}































