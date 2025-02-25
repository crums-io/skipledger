/*
 * Copyright 2021-2025 Babak Farhang
 */
package io.crums.sldg.salt;


import java.nio.ByteBuffer;
import java.security.MessageDigest;

/**
 * A method to salt table cells with <em>unique</em> values per cell coordinate. In this model
 * the <em>seed salt</em> is kept secret.
 * 
 * <h2>How to use</h2>
 * 
 * <p>Every cell-salt is derivable from the row-salt. If no cell value is redacted, then
 * just include the row-salt; if <em>any</em> cell is redacted, then the row-salt must
 * not be revealed, and each revealed cell value must be accompanied with its cell-salt.
 * </p><p>
 * Since this seed is secret, this class manages erasing it with the {@linkplain #close()}
 * method.
 * </p>
 * <h2>Thread-safe (mostly)</h2>
 * <p>
 * Excepting the {@linkplain #close()} method, instances are thread-safe.
 * </p>
 * 
 */
public class TableSalt implements AutoCloseable {
  

  
  
  /**
   * Returns the cell-salt. The cell-salt is directly inferrable from the row-salt,
   * which is why this method is static.
   * 
   * @param rowSalt     computed from {@linkplain #rowSalt(long, MessageDigest)}
   * @param cell        not bounds checked, usually 1-based
   * @param digest      not null
   * 
   * @return    hash of the concatenation of {@code [cell][rowSalt][~cell]}
   */
  public static byte[] cellSalt(byte[] rowSalt, long cell, MessageDigest digest) {
    var work = ByteBuffer.allocate(rowSalt.length + 16);
    work.putLong(cell).put(rowSalt).putLong(~cell).flip();
    digest.reset();
    digest.update(work);
    return digest.digest();
  }
  
  
  /**
   * Returns the cell-salt. The cell-salt is directly inferrable from the row-salt,
   * which is why this method is static.
   * 
   * @param rowSalt     computed from {@linkplain #rowSalt(long, MessageDigest)}.
   *                    <em>On return, contains no remaining bytes!</em>
   * @param cell        not bounds checked, usually 1-based
   * @param digest      not null
   * 
   * @return    hash of the concatenation of {@code [cell][rowSalt][~cell]}
   */
  public static byte[] cellSalt(ByteBuffer rowSalt, long cell, MessageDigest digest) {
    var work = ByteBuffer.allocate(rowSalt.remaining() + 16);
    work.putLong(cell).put(rowSalt).putLong(~cell).flip();
    digest.reset();
    digest.update(work);
    return digest.digest();
  }
  
  
  
  /** Minimum no. of bytes in seed. */
  public final static int MIN_SEED_BYTES = 8;
  
  private final byte[] seed;
  
  private final byte[] zeroed;
  
  
  
  /**
   * Constructs an instance with the given {@code seed} bytes. On return
   * the argument byte array is zero-ed.
   */
  public TableSalt(byte[] seed) {
    this(ByteBuffer.wrap(seed));
  }

  /**
   * Constructs an instance with the given {@code seed} bytes. On return, if
   * the argument is not read-only, then it is zero-ed.
   * 
   */
  public TableSalt(ByteBuffer seed) {
    final int seedSize = seed.remaining();
    if (seedSize < MIN_SEED_BYTES)
      throw new IllegalArgumentException(
          "not enought bytes in seed: " + seed + " - minimum is " + MIN_SEED_BYTES);
    
    seed.mark();
    this.seed = new byte[seedSize];
    seed.get(this.seed);
    
    // clear the input
    if (!seed.isReadOnly())
      for (seed.reset(); seed.hasRemaining(); seed.put((byte) 0));
    
    this.zeroed = new byte[seedSize + 16];
  }
  
  
  
  protected TableSalt(TableSalt copy) {
    this.seed = copy.seed;
    this.zeroed = copy.zeroed;
    if (!isOpen())
      throw new IllegalArgumentException(
          "attempt to copy closed instance: " + copy);
  }
  
  
  
  /**
   * Returns the row-salt.
   * 
   * <h4>Implementation Note</h4>
   * 
   * This uses the hash of a direct concatenation of the row number and the seed.
   * A higher entropy solution would be replace or compliment the row number with
   * the hash of the <em>previous</em> row. Not implemented because
   * <ol>
   * <li>it adds complexity,</li>
   * <li>for dubious gains.</li>
   * </ol>
   * (Listed so I won't revisit this idea.)
   * 
   * @param row         should be positive (not enforced)
   * @param digest      not null
   * 
   * @return   hash of the concatenation of {@code [row][seed][~row]}
   * @throws IllegalStateException if the instance was closed
   * @see #close()
   * @see #isOpen()
   */
  public byte[] rowSalt(long row, MessageDigest digest)
      throws IllegalStateException {
    var work = ByteBuffer.allocate(seed.length + 16);
    work.putLong(row).put(seed).putLong(~row).flip();
    digest.reset();
    digest.update(work);
    // zero secret
    work.clear();
    work.put(zeroed);
    byte[] salt = digest.digest();
    if (!isOpen())
      throw new IllegalStateException("instance is closed: " + this);
    return salt;
  }
  
  
  
  /**
   * Returns the cell-specific salt.
   * 
   * @param row         should be positive (not enforced)
   * @param cell        not bounds checked, usually 1-based
   * @param digest      not null
   * 
   * @return {@code cellSalt(rowSalt(row, digest), cell, digest)}
   * 
   * @see #rowSalt(long, MessageDigest)
   * @see #cellSalt(long, long, MessageDigest)
   */
  public byte[] cellSalt(long row, long cell, MessageDigest digest) {
    return
        cellSalt(
            rowSalt(row, digest), cell, digest);
  }
  
  
  
  
  
  
  /**
   * Tests whether the instance is open.
   * 
   * @return {@code false}, if {@linkplain #close()} has been invoked.
   */
  public final boolean isOpen() {
    return zeroed[0] == 0;
  }
	
  /**
   * Erases the instance's seed from memory.
   */
  @Override
  public void close() {
    zeroed[0] = 1;
    for (int index = seed.length; index-- > 0; )
      seed[index] = 0;
  }

}






