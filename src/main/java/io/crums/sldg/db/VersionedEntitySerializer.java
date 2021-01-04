/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.db;


import static io.crums.util.IntegralStrings.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.Objects;
import java.util.function.Function;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import io.crums.io.Serial;
import io.crums.io.channels.ChannelUtils;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.SldgException;
import io.crums.sldg.json.JsonEntityParser;

/**
 * 
 */
class VersionedEntitySerializer<T extends Serial> implements EntitySerializer<T> {
  
  private final JsonEntityParser<T> jsonParser;
  private final Function<InputStream, T> binaryLoader;
  
  private final byte entityType;

  /**
   * 
   */
  public VersionedEntitySerializer(
      JsonEntityParser<T> jsonParser,
      Function<InputStream, T> binaryLoader, byte entityType) {
    this.jsonParser = Objects.requireNonNull(jsonParser, "null jsonParser");
    this.binaryLoader = Objects.requireNonNull(binaryLoader, "null binaryLoader");
    this.entityType = entityType;
  }

  @Override
  public T loadJson(File file) {
    try (Reader reader = new FileReader(file)){
      
      JSONObject jObj = (JSONObject) new JSONParser().parse(reader);
      return jsonParser.toEntity(jObj);
    
    } catch (IOException iox) {
      throw new UncheckedIOException("on loading " + file, iox);
    } catch (ParseException px) {
      throw new IllegalArgumentException("malformed JSON in " + file, px);
    } catch (Exception x) {
      throw new SldgException("on loading JSON from " + file, x);
    }
  }

  @Override
  public T loadBinary(File file) {
    int version = 256;  // 1 + max byte value
    int type;
    try (FileInputStream fis = new FileInputStream(file)){
      // read, but o.w. skip over, the version byte
      version = fis.read();
      type = fis.read();
      if (type == entityType)
        return binaryLoader.apply(fis);
      
    } catch (Exception x) {
      throw new SldgException("on loading " + file + " (version byte '" + version + "')", x);
    }
    
    throw new IllegalArgumentException(
        "expected entity code <" + toHex(entityType) +
        ">; actual in file " + file + " is <" + toHex((byte) type) + ">");
  }

  @SuppressWarnings("unchecked")
  @Override
  public void writeJson(T entity, File file) {
    
    try (Writer writer = new FileWriter(file)) {
      
      JSONObject jEntity = jsonParser.toJsonObject(entity);
      jEntity.put(SldgConstants.VERSION_TAG, SldgConstants.VERSION);
      writer.write(jEntity.toJSONString());
    } catch (IOException iox) {
      throw new UncheckedIOException("on writing to " + file, iox);
    }

  }

  @Override
  public void writeBinary(T entity, File file) {

    try (FileOutputStream out = new FileOutputStream(file)) {
      
      out.write(SldgConstants.VERSION_BYTE);
      out.write(entityType);
      ChannelUtils.writeRemaining(out.getChannel(), entity.serialize());
    
    } catch (IOException iox) {
      throw new UncheckedIOException("on file argument " + file, iox);
    }
  }

}
