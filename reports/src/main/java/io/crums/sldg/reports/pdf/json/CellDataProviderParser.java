/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import io.crums.sldg.reports.pdf.CellDataProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.BaseTextProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.DateProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.ImageProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.NumberProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.PatternedProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.StringProvider;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class CellDataProviderParser implements JsonEntityParser<CellDataProvider<?>> {
  
  public final static CellDataProviderParser INSTANCE = new CellDataProviderParser();

  public final static String TYPE = "type";
  
  public final static String STRING = "string";
  public final static String NUMBER = "number";
  public final static String DATE = "date";
  public final static String IMAGE = "image";
  public final static String SUM = "sum";

  public final static String PATTERN = "pattern";
  public final static String PREFIX = "prefix";
  public final static String POSTFIX = "postfix";

  public final static String W = "w";
  public final static String H = "h";
  
  
  @Override
  public JSONObject injectEntity(CellDataProvider<?> provider, JSONObject jObj) {
    if (provider instanceof StringProvider sp) {
      jObj.put(TYPE, STRING);
      injectPrepostfix(sp, jObj);
    } else if (provider instanceof NumberProvider np) {
      jObj.put(TYPE, NUMBER);
      injectPatterned(np, jObj);
    } else if (provider instanceof DateProvider dp) {
      jObj.put(TYPE, DATE);
      injectPatterned(dp, jObj);
    } else if (provider instanceof ImageProvider ip) {
      jObj.put(TYPE, IMAGE);
      jObj.put(W, ip.getWidth());
      jObj.put(H, ip.getHeight());
    } else if (provider instanceof ImageProvider ip) {
      
    }
    return jObj;
  }
  
  
  private void injectPrepostfix(BaseTextProvider<?> provider, JSONObject jObj) {
    provider.prefix().ifPresent(prefix -> jObj.put(PREFIX, prefix));
    provider.postfix().ifPresent(postfix -> jObj.put(POSTFIX, postfix));
  }

  private void injectPatterned(PatternedProvider<?> provider, JSONObject jObj) {
    if (!provider.pattern().isEmpty())
      jObj.put(PATTERN, provider.pattern());
    injectPrepostfix(provider, jObj);
  }
  
  
  
  @Override
  public CellDataProvider<?> toEntity(JSONObject jObj) throws JsonParsingException {
    String type = JsonUtils.getString(jObj, TYPE, true);
    // a bit inefficient -- but cleaner this way
    var prefix = JsonUtils.getString(jObj, PREFIX, "");
    var postfix = JsonUtils.getString(jObj, POSTFIX, "");
    var pattern = JsonUtils.getString(jObj, PATTERN, "");
    return switch (type) {
    case STRING -> new StringProvider(prefix, postfix);
    case NUMBER -> new NumberProvider(pattern, prefix, postfix);
    case DATE   -> new DateProvider(pattern, prefix, postfix);
    case IMAGE  -> new ImageProvider(
                      JsonUtils.getNumber(jObj, W, true).floatValue(),
                      JsonUtils.getNumber(jObj, H, true).floatValue());
    default ->  throw new JsonParsingException("unknown cell data provider '" + TYPE + "': " + type);
    };
  }
  
  

}
