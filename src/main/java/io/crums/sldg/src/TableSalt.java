/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Objects;

import io.crums.sldg.SldgConstants;

/**
 * A method to salt table cells with <em>unique</em> values per cell coordinate. In this model
 * the <em>seed salt</em> is kept secret.
 */
public class TableSalt {
  
  private final MessageDigest digest = SldgConstants.DIGEST.newDigest();
  
  private final ByteBuffer seed;
  
  
  /**
   * Constructs a new instance by copying the given the secret seed. By design, instances do not
   * expose this value.
   * 
   * @param seed 32-byte high-entropy, secret seed
   */
  public TableSalt(ByteBuffer seed) {
    if (Objects.requireNonNull(seed, "null seed").remaining() != SldgConstants.HASH_WIDTH)
      throw new IllegalArgumentException("illegal remaining bytes: " + seed);
    this.seed = ByteBuffer.allocate(SldgConstants.HASH_WIDTH).put(seed).flip().asReadOnlyBuffer();
  }
  
  
  
  
  /**
   * Returns a salt value that is unique to the given coordinates. This is designed so that it
   * does not give away the seed hash.
   * <p>
   * TODO: review procedure with cryptographers and make sure there are no known SHA-256 attacks that could
   * exploit this procedure in order to discover the seed (say by observing salted coordinates from the same
   * seed at scale).
   * 
   * C.f. https://crypto.stackexchange.com/questions/91525/does-my-sha-256-tablesalt-algo-give-away-the-seed-salt
   * </p>
   * 
   * @param row  row number
   * @param col  column number
   * 
   * @return 32-bytes
   */
  public synchronized ByteBuffer salt(long row, long col) {
    digest.reset();
    digest.update(seed.slice());
    ByteBuffer coordinates = ByteBuffer.allocate(16);
    coordinates.putLong(row).putLong(col).flip();
    digest.update(coordinates);
    return ByteBuffer.wrap(digest.digest());
  }
  
  
  
  

}
