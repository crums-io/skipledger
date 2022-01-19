/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;


import static io.crums.util.IntegralStrings.hexToBytes;
import static io.crums.util.IntegralStrings.toHex;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.Date;

import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.src.BytesValue;
import io.crums.sldg.src.ColumnType;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.DateValue;
import io.crums.sldg.src.DoubleValue;
import io.crums.sldg.src.HashValue;
import io.crums.sldg.src.LongValue;
import io.crums.sldg.src.NullValue;
import io.crums.sldg.src.StringValue;

/**
 * 
 */
public class ColumnValueParser implements JsonEntityParser<ColumnValue> {
  
  
  public final static String TYPE = "type";
  public final static String SALT = "salt";
  public final static String VAL = "val";
  public final static String DATE = "date";
  
  
  
  
  
  
  
  
  private final DateFormat dateFormatter;
  
  
  /**
   * Creates a thead safe instance (with no date formatter).
   */
  public ColumnValueParser() {
    this(null);
  }
  
  /**
   * If {@code dateFormatter} is non-null then the instance is likely
   * <em>not thread safe</em>.
   * 
   * @param dateFormatter optional formating for date values
   * 
   * @see #ColumnValueParser()
   */
  public ColumnValueParser(DateFormat dateFormatter) {
    this.dateFormatter = dateFormatter;
  }
  
  
  
  
  

  

  public JSONObject toJsonObject(ColumnValue entity, DateFormat dateFormatter) {
    return injectEntity(entity, new JSONObject(), dateFormatter);
  }
  
  
  @Override
  public JSONObject injectEntity(ColumnValue columnValue, JSONObject jObj) {
    return injectEntity(columnValue, jObj, dateFormatter);
  }


  public JSONObject injectEntity(ColumnValue entity, JSONObject jObj, DateFormat dateFormatter) {
    var type = entity.getType();
    jObj.put(TYPE, type.symbol());
    
    if (entity.isSalted())
      jObj.put(SALT, toHex(entity.getSalt()));
    
    switch (type) {
    case NULL:
      break;
    case BYTES:
    case HASH:
      jObj.put(VAL, toHex(((BytesValue) entity).getBytes()));
      break;
    case DATE:
      if (dateFormatter != null)
        jObj.put(DATE, dateFormatter.format(new Date(((DateValue) entity).getUtc())));
    case LONG:
    case DOUBLE:
    case STRING:
      jObj.put(VAL, entity.getValue());
      break;
    }
    
    return jObj;
  }
  
  
  /**
   * Returns a JSON-compatible object representation of the given
   * {@code columnValue}'s {@linkplain ColumnValue#getValue() value}.
   * Type information is lost.
   * 
   * @param columnValue non-null
   * 
   * @return {@code null} <b>iff</b> the type is {@linkplain ColumnType#NULL NULL}}
   */
  public Object getJsonValue(ColumnValue columnValue) {
    switch (columnValue.getType()) {
    case BYTES:
    case HASH:    return toHex(((BytesValue) columnValue).getBytes());
    case NULL:
    case LONG:
    case DOUBLE:
    case DATE:
    case STRING:  return columnValue.getValue();
    default:
      throw new RuntimeException("unaccounted type " + columnValue.getType());
    }
  }

  
  
  @Override
  public ColumnValue toEntity(JSONObject jObj) throws JsonParsingException {
    
    ColumnType type;
    try {
      String symbol = JsonUtils.getString(jObj, TYPE, true);
      type = ColumnType.forSymbol(symbol);
    } catch (IllegalArgumentException iax) {
      throw new JsonParsingException(iax.getMessage(), iax);
    }
    
    ByteBuffer salt;
    try {
      String hex = JsonUtils.getString(jObj, SALT, false);
      if (hex == null) {
        salt = BufferUtils.NULL_BUFFER;
      } else {
        if (type.isHash())
          throw new JsonParsingException(
              "type '" + type.symbol() + "' does not accept a salt value: " + jObj);
        salt = ByteBuffer.wrap(hexToBytes(hex));
      }
    } catch (IllegalArgumentException iax) {
      throw new JsonParsingException(iax.getMessage(), iax);
    }
    
    
    if (type.isNull())
      return new NullValue(salt);
    
    Object value = jObj.get(VAL);
    switch (type) {
    case NULL:
      if (value != null)
        throw new JsonParsingException(
            "type '" + type.symbol() + "' does not accept a '"+ VAL + "': " + value);
      return new NullValue(salt);
            
    case BYTES:   return new BytesValue(hexToBytes(value.toString()), salt);
    case HASH:    return new HashValue(hexToBytes(value.toString()));
    case LONG:    return new LongValue((((Number) value).longValue()), salt);
    case DATE:    return new DateValue((((Number) value).longValue()), salt);
    case DOUBLE:  return new DoubleValue((((Number) value).doubleValue()), salt);
    case STRING:  return new StringValue(value.toString(), salt);
    default:
      throw new RuntimeException("unaccounted column type " + type);
    }
  }

}
