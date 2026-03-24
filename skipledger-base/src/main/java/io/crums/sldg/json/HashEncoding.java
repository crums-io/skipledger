/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg.json;


import java.nio.ByteBuffer;

import io.crums.util.Base64_32;
import io.crums.util.IntegralStrings;

/**
 * ASCII encodings for 32-byte hash values.
 */
public enum HashEncoding {
  /**
   * Base64-32 encoding. 43 chars used
   * to represent a 32-byte value.
   * 
   * @see Base64_32
   */
  BASE64_32,
  /**
   * Hexadecimal encoding. 64 chars used
   * to represent a 32-byte value.
   */
  HEX;

  /**
   * Returns the given hash in encoded form.
   * 
   * @param hash  with exactly 32 bytes remaining
   */
  public String encode(ByteBuffer hash) {
    return
        this == HEX ?
            IntegralStrings.toHex(hash) :
            Base64_32.encode(hash);
  }
  
  /**
   * Returns the given hash in encoded form.
   * 
   * @param hash  with exactly 32 bytes remaining
   */
  public String encode(byte[] hash) {
    return
        this == HEX ?
            IntegralStrings.toHex(hash) :
            Base64_32.encode(hash);
  }
  
  /**
   * Decodes and returns a 32-byte value, whether in Base64-32 (43 chars)
   * or hex (64 chars).
   * 
   * @param hash 43 Base64-32 chars, or 64 hex chars
   */
  public byte[] decode(CharSequence hash) {
    // Fix for timechain issue #3
    // C.f. https://github.com/crums-io/timechain/issues/3
    //
    // return this == HEX ?
    //     IntegralStrings.hexToBytes(hash) :
    //     Base64_32.decode(hash);
    int len = hash.length();
    return
        switch (len) {
          case 43 -> Base64_32.decode(hash);
          case 64 -> IntegralStrings.hexToBytes(hash);
          default -> throw new IllegalArgumentException(
              "hash length must be 43 or 64: actual given is " + len +
              "; arg given " + hash);
        };


    
  }
  
  
  /**
   * Length of ASCII string representing a 32-byte value.
   * 
   * @return {@code this == HEX ? 64 : 43}
   */
  public int length() {
    return this == HEX ? 64 : 43;
  }
  
}















