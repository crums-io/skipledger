/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Objects;

import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.SldgConstants;

/**
 * A method to salt table cells with <em>unique</em> values per cell coordinate. In this model
 * the <em>seed salt</em> is kept secret.
 * 
 * <p>Since this seed is secret, this class manages erasing it with the {@linkplain #close()}
 * method.</p>
 */
public class TableSalt implements AutoCloseable {
  
  
  /**
   * Null instance. Does not salt; overrides {@linkplain #salt(long, long)} to return an
   * empty buffer. Stateless: closing this instance has no effect.
   */
  public final static TableSalt NULL_SALT = new TableSalt() {
    @Override
    public ByteBuffer salt(long row, long col) {
      return BufferUtils.NULL_BUFFER;
    }
    @Override
    public void close() { }
  };
  
  
  private final ByteBuffer seed;
  
  private final MessageDigest digest;
  
  public TableSalt(byte[] seed) {
    this(ByteBuffer.wrap(seed));
  }
  
  
  /**
   * Constructs a new instance by copying the given the secret seed. By design, instances do not
   * expose this value.
   * 
   * @param seed 32-byte high-entropy, secret seed
   */
  public TableSalt(ByteBuffer seed) {
    if (Objects.requireNonNull(seed, "null seed").remaining() != SldgConstants.HASH_WIDTH)
      throw new IllegalArgumentException("illegal remaining bytes: " + seed);
    this.seed = ByteBuffer.allocate(SldgConstants.HASH_WIDTH).put(seed).flip();
    this.digest = SldgConstants.DIGEST.newDigest();
  }
  
  
  private TableSalt() {
    seed = null;
    digest = null;
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
   * 
   * @throws IllegalStateException if closed
   * @see #close()
   */
  public synchronized ByteBuffer salt(long row, long col) throws IllegalStateException {
    if (!seed.hasRemaining())
      throw new IllegalStateException("instance is closed");
    digest.reset();
    digest.update(seed.slice());
    ByteBuffer coordinates = ByteBuffer.allocate(16);
    coordinates.putLong(row).putLong(col).flip();
    digest.update(coordinates);
    return ByteBuffer.wrap(digest.digest());
  }
  
  
  /**
   * Erases the instance's seed from memory. On return, {@linkplain #salt(long, long)}
   * throws an exception.
   */
  @Override
  public synchronized void close() {
    seed.put(SldgConstants.DIGEST.sentinelHash());
  }

}
