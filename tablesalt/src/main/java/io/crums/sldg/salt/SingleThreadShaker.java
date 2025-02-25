/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.salt;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ConcurrentModificationException;
import java.util.Objects;

/**
 * Utility that reuses the row-salt and the same digest. Instances are <em>not
 * safe under concurrent access</em>.
 * 
 * @see #rowSalt
 * @see #cellSalt(long, long)
 */
public class SingleThreadShaker extends TableSalt {
  
  private final MessageDigest digest;
  
  private long rowNo;
  private ByteBuffer rowSalt;
  
  
  
  private final boolean clearOnClose;

  /**
   * Constructor. Secret seed cleared on close.
   */
  public SingleThreadShaker(byte[] seed, MessageDigest digest) {
    this(ByteBuffer.wrap(seed), digest);
  }

  /**
   * Constructor. Secret seed cleared on close.
   */
  public SingleThreadShaker(ByteBuffer seed, MessageDigest digest) {
    super(seed);
    this.digest = Objects.requireNonNull(digest, "null digest");
    clearOnClose = true;
  }

  /**
   * Promotion constructor. <em>Secret seed <strong>not</strong> cleared on close.</em>
   * 
   * @param promote
   */
  public SingleThreadShaker(TableSalt promote, MessageDigest digest) {
    super(promote);
    this.digest = Objects.requireNonNull(digest, "null digest");
    clearOnClose = false;
  }

  
  
  /**
   * Returns the row-salt, for the given row no.
   * 
   * @return read-only buffer
   */
  public ByteBuffer rowSalt(long row) {
    if (row == rowNo && rowSalt != null)
      return rowSalt.asReadOnlyBuffer();

    this.rowNo = row;
    byte[] salt = rowSalt(row, digest);
    var out = ByteBuffer.wrap(salt);
    rowSalt = out;
    out = out.asReadOnlyBuffer();
    
    if (rowNo != row)
      throw new ConcurrentModificationException(
          "concurrent access detected on rows " + row + " / " + rowNo);
    
    return out;
  }
  
  
  /**
   * Return the cell salt for the given coordinates.
   */
  public byte[] cellSalt(long row, long cell) {
    return cellSalt( rowSalt(row), cell, digest);
  }

  
  /**
   * "Promoted" instances do not clear the secret seed.
   * 
   * @see SingleThreadShaker#SingleThreadShaker(TableSalt, MessageDigest)
   */
  @Override
  public void close() {
    if (clearOnClose)
      super.close();
  }
  

}

