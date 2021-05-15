/*
 * Copyright 2020-2021 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.SldgConstants.*;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import io.crums.util.hash.Digest;

/**
 * Sets the base row {@linkplain Digest}.
 * 
 * @see SldgConstants#DIGEST
 */
abstract class BaseRow extends Row {

  @Override
  public final int hashWidth() {
    return HASH_WIDTH;
  }

  @Override
  public final String hashAlgo() {
    return DIGEST.hashAlgo();
  }

  @Override
  public final MessageDigest newDigest() {
    return DIGEST.newDigest();
  }

  @Override
  public final ByteBuffer sentinelHash() {
    return DIGEST.sentinelHash();
  }
  
}


