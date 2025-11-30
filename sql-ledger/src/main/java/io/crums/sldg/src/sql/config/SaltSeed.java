/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src.sql.config;

import io.crums.util.Base64_32;
import io.crums.util.IntegralStrings;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * 32-byte salt. Secret.
 * 
 * @see #clear()
 * @see SaltSeed#PARSER
 */
public record SaltSeed(byte[] seed) {
  
  public final static int LENGTH = 32;

  /** JSON parser. */
  public final static JsonEntityParser<SaltSeed> PARSER = new Parser();
  
  
  public SaltSeed {
    if (seed.length != LENGTH)
      throw new IllegalArgumentException(
          "seed length must equal %d; actual given was %d"
          .formatted(LENGTH, seed.length));
  }
  
  /**
   * Zeroes the seed array. Since it's a secret, it ought it be cleared
   * once finished using it.
   * 
   * @see #isCleared()
   */
  public void clear() {
    for (int index = seed.length; index-- > 0; )
      seed[index] = 0;
  }
  
  /**
   * Returns {@code true} iff all the seeds bytes are zeroed.
   * 
   * @see #clear()
   */
  public boolean isCleared() {
    int index = seed.length;
    while (index-- > 0 && seed[index] == 0);
    return index == -1;
  }
  
  /**
   * Returns the seed in base64-32 encoding.
   * 
   * @return 43 base64-32 digits
   */
  public String base64_32() {
    return Base64_32.encode(seed);
  }
  
  /**
   * Returns the seed in hex.
   * 
   * @return 64 hex digits
   */
  public String hex() {
    return IntegralStrings.toHex(seed);
  }
  

  /** JSON parser. */
  public static class Parser implements JsonEntityParser<SaltSeed> {
    
    public final static String SALT_SEED = "salt_seed";

    @Override
    public JSONObject injectEntity(SaltSeed salt, JSONObject jObj) {
      if (salt.isCleared())
        throw new IllegalArgumentException("zeroed (cleared) salt");
      jObj.put(SALT_SEED, salt.base64_32());
      return jObj;
    }

    @Override
    public SaltSeed toEntity(JSONObject jObj) throws JsonParsingException {
      String encoded = JsonUtils.getString(jObj, SALT_SEED, true);
      byte[] seed;
      try {
        switch (encoded.length()) {
        case Base64_32.ENC_LEN:
          seed = Base64_32.decode(encoded);             break;
        case LENGTH * 2:
          seed = IntegralStrings.hexToBytes(encoded);   break;
        default:
          throw new JsonParsingException(
              "illegal '%s' value: %s".formatted(SALT_SEED, encoded));
        }
      } catch (JsonParsingException jpx) {
        throw jpx;
      } catch (Exception x) {
        throw new JsonParsingException(x);
      }
        
      return new SaltSeed(seed);
    }
    
  }
  
}








