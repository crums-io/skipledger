/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.hash;

import java.nio.ByteBuffer;

/**
 * The hashing algorithms we use are gathered here. The point is you
 * should be able to swap one out for another.
 */
public class Digests {
  
  
  /**
   * SHA-256 spec. Hash width: 32 bytes.
   */
  public final static Digest SHA_256 = new Digest() {
    
    private final ByteBuffer sentinel = ByteBuffer.allocate(32).asReadOnlyBuffer();

    @Override
    public int hashWidth() {
      return 32;
    }

    @Override
    public String hashAlgo() {
      return "SHA-256";
    }

    @Override
    public ByteBuffer sentinelHash() {
      return sentinel.asReadOnlyBuffer();
    }
    
  };
  
  
  
  // no instances
  private Digests() {  }

}
