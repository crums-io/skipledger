/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;


import java.nio.ByteBuffer;
import java.util.Optional;

import io.crums.io.Serial;
import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.ByteFormatException;
import io.crums.sldg.json.SourceInfoParser;
import io.crums.sldg.src.SourceInfo;
import io.crums.util.BigShort;
import io.crums.util.Strings;

/**
 * Provides meta information. The information here is not cryptographically
 * validated. It is informational, and may be locale-specific. This just
 * encapsulates a {@linkplain SourceInfo} object.
 * 
 * <h2>Serial Format</h2>
 * <p>
 * Excepting its 3-byte header, the serial format is in fact JSON.
 * </p>
 * <h4>Structure</h4>
 * <p>
 * <pre>
 *    META_PACK := LEN JSON
 *    LEN := BYTE ^3    // 3-byte unsigned big endian value
 *    JSON := BYTE ^[LEN]
 * </pre>
 * </p>
 * 
 * @see SourceInfoParser for JSON format
 * @see #getSourceInfo()
 */
public class MetaPack implements Serial {
  
  public final static MetaPack EMPTY = new MetaPack(null);
  
  public static MetaPack load(ByteBuffer in) {
    int size = BigShort.getBigShort(in);
    if (size == 0)
      return EMPTY;
    
    if (in.remaining() != size) {
      if (in.remaining() < size)
        throw new ByteFormatException(
            "in-buffer contains fewer bytes (" + in.remaining() + ") than advertised (" + size + ")");
      else
        in = BufferUtils.slice(in, size);
    }
    String json = Strings.utf8String(in);
    var metaInfo = SourceInfoParser.INSTANCE.toEntity(json);
    return new MetaPack(metaInfo);
  }
  
  
  
  
  
  
  
  
  
  
  private final SourceInfo metaInfo;
  
  private volatile byte[] jsonBytes;

  /**
   * @param metaInfo optional (may be null)
   */
  public MetaPack(SourceInfo metaInfo) {
    this.metaInfo = metaInfo;
  }
  
  
  /**
   * Returns the optional {@linkplain SourceInfo}.
   */
  public Optional<SourceInfo> getSourceInfo() {
    return Optional.ofNullable(metaInfo);
  }
  
  
  /**
   * (Woulda been nice if {@code Optional} were an interface instead of a class.)
   * 
   * @return {@code getSourceInfo().isEmpty()}
   */
  public boolean isEmpty() {
    return metaInfo == null;
  }
  
  
  /**
   * @return <em>!</em> {@linkplain #isEmpty()}
   */
  public boolean isPresent() {
    return !isEmpty();
  }
  
  
  
  
  @Override
  public int serialSize() {
    return metaInfo == null ? 3 : 3 + jsonBytes().length;
  }
  
  
  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    if (metaInfo == null)
      BigShort.putBigShort(out, 0);
    else {
      var json = jsonBytes();
      BigShort.putBigShort(out, json.length);
      out.put(json);
    }
    return out;
  }
  
  
  
  
  private byte[] jsonBytes() {
    assert metaInfo != null;
    
    if (jsonBytes == null) {
      String jsonStr = SourceInfoParser.INSTANCE.toJsonObject(metaInfo).toJSONString();
      jsonBytes = Strings.utf8Bytes(jsonStr);
    }
    return jsonBytes;
  }
  

}









