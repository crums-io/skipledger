/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.db;


import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import io.crums.io.Serial;
import io.crums.sldg.Ledger;
import io.crums.util.Lists;

/**
 * 
 */
public abstract class RnEncoding implements Serial {
  
  /**
   * The defined types of {@linkplain RnEncoding}.
   */
  public enum Encoding {
    LISTED,
    SKIP,
    CAMEL;
    
    
    private final static Encoding[] ORDINALS = Encoding.values();
    
    public byte magic() {
      return (byte) this.ordinal();
    }
    
    public static Encoding forMagic(byte magic) {
      int ord = 0xff & magic;
      if (ord >= ORDINALS.length)
        throw new IllegalArgumentException("magic " + magic + " (0x" + Integer.toHexString(ord) + ")");
      return ORDINALS[ord];
    }
  }
  
  
  
  
  

  // implementations only defined here
  private RnEncoding() { }
  
  
  /**
   * Returns the encoding type.
   */
  public abstract Encoding type();
  
  
  /**
   * Returns the row numbers.
   * 
   * @return ascending list of row numbers
   */
  public abstract List<Long> rowNumbers();
  
  

  
  
  /**
   * Implementation for {@linkplain Encoding#LISTED} type.
   */
  public final static class Listed extends RnEncoding {
    
    private final List<Long> rowNumbers;
    
    /**
     * Constructor makes a defensive copy.
     * 
     * @param rowNumbers not empty, ascending
     */
    public Listed(List<Long> rowNumbers) {
      if (Objects.requireNonNull(rowNumbers, "null rowNumbers").isEmpty())
        throw new IllegalArgumentException("empty rowNumbers");
      
      Long[] array = new Long[rowNumbers.size()];
      long last = 0;
      for (int index = 0; index < array.length; ++index) {
        Long next = rowNumbers.get(index);
        if (next <= last)
          throw new IllegalArgumentException(
              "illegal or out-of-sequence rowNumber " + next + " at index " + index +
              " in " + rowNumbers);
        
        last = next;
        array[index] = next;
      }
      this.rowNumbers = Lists.asReadOnlyList(array);
    }

    @Override
    public int serialSize() {
      // size of int (count) + 8 * num_rows
      return 8 * rowNumbers.size() + 4;
    }

    @Override
    public ByteBuffer writeTo(ByteBuffer out) {
      out.putInt(rowNumbers.size());
      for (long rn : rowNumbers)
        out.putLong(rn);
      return out;
    }

    
    @Override
    public Encoding type() {
      return Encoding.LISTED;
    }

    @Override
    public List<Long> rowNumbers() {
      return rowNumbers;
    }
    
  }
  
  

  /**
   * Implementation for {@linkplain Encoding#SKIP} type.
   */
  public final static class Skip extends RnEncoding {
    
    private final List<Long> rowNumbers;
    
    
    
    
    public Skip(long lo, long hi) {
      this.rowNumbers = Ledger.skipPathNumbers(lo, hi);
    }

    /**
     * @return 16 (size of 2 longs)
     */
    @Override
    public int serialSize() {
      return 16;
    }

    @Override
    public ByteBuffer writeTo(ByteBuffer out) {
      long lo = rowNumbers.get(0);
      long hi = rowNumbers.get(rowNumbers.size() - 1);
      return out.putLong(lo).putLong(hi);
    }

    @Override
    public Encoding type() {
      return Encoding.SKIP;
    }

    @Override
    public List<Long> rowNumbers() {
      return rowNumbers;
    }
    
  }
  
  
  

  /**
   * Implementation for {@linkplain Encoding#CAMEL} type.
   */
  public final static class Camel extends RnEncoding {
    
    private final List<Long> rowNumbers;
    private final long mid;
    
    /**
     * Constructor.
     * 
     * @param lo &ge; 1
     * @param mid &gt; {@code lo}
     * @param hi &gt; {@code mid}
     */
    public Camel(long lo, long mid, long hi) {
      if (mid <= lo || mid >= hi)
        throw new IllegalArgumentException("lo " + lo + "; mid " + mid + "; hi " + hi);
      List<Long> head = Ledger.skipPathNumbers(lo, mid);
      List<Long> tail = Ledger.skipPathNumbers(mid, hi);
      
      this.rowNumbers = Lists.concat(head, tail.subList(1, tail.size()));
      this.mid = mid;
    }


    /**
     * @return 16 (size of 2 longs)
     */
    @Override
    public int serialSize() {
      return 24;
    }

    @Override
    public ByteBuffer writeTo(ByteBuffer out) {
      long lo = rowNumbers.get(0);
      long hi = rowNumbers.get(rowNumbers.size() - 1);
      return out.putLong(lo).putLong(mid).putLong(hi);
    }

    @Override
    public Encoding type() {
      return Encoding.CAMEL;
    }

    @Override
    public List<Long> rowNumbers() {
      return rowNumbers;
    }
    
  }
  
  
  
}
