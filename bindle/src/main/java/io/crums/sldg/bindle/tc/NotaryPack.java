/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle.tc;


import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Function;

import io.crums.io.Serial;
import io.crums.io.SerialFormatException;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.bindle.LedgerId;
import io.crums.sldg.bindle.LedgerType;
import io.crums.sldg.bindle.Nugget;
import io.crums.sldg.bindle.TimechainInfo;
import io.crums.tc.ChainParams;
import io.crums.util.Lists;

/**
 * Binary package containing witness proofs of a ledger's rows on a timechain.
 * Note, since a {@linkplain Nugget} may contain multiple notary
 * packs, there is no representation for an "empty" notary pack.
 */
public class NotaryPack implements Serial {
  
  
  /**
   * Notary pack builder.
   */
  public static class Builder {
    
    private final TreeMap<Long, NotarizedRow> notarizedRows = new TreeMap<>();
    
    private final LedgerId chainId;

    /**
     * 
     * @param chainId {@code chainId.info().type()} must be
     *                {@linkplain LedgerType#TIMECHAIN}
     */
    public Builder(LedgerId chainId) {
      this.chainId = chainId;
      if (!chainId.info().type().isTimechain())
        throw new IllegalArgumentException(
            "chainId %s must be a timechain; actual given type was %s"
            .formatted(chainId, chainId.info().type()));
    }
    
    
    /**
     * Returns the numeric ID of the timechain. The returned number
     * is <em>local</em> to the bindle.
     */
    public LedgerId chainId() {
      return chainId;
    }
    
    
    /**
     * Returns the number of notarized rows.
     */
    public int count() {
      return notarizedRows.size();
    }
    
    
    /**
     * Adds the given notarized row iff it is not already notarized
     * at an earlier date.
     * <p>
     * Note, <em>any lower numbered row notarized on a later (or equal)
     * UTC is removed.</em>
     * </p>
     * @param nr        notarized row
     * @return {@code true} iff the argument was added
     */
    public boolean addNotarizedRow(NotarizedRow nr) {
      
      // if already added at the same UTC (or earlier), OR
      // if the next row is notarized [on or] *before* nr
      // then return false
      {
        var tailMap = notarizedRows.tailMap(nr.rowNo());
        
        if (!tailMap.isEmpty()) {
          var nextNr = tailMap.firstEntry().getValue();
          
          if (nextNr.rowNo() == nr.rowNo()) {
            
            if (!nextNr.rowHash().equals(nr.rowHash()))
              throw new HashConflictException(
                  "existing notarized row [%d] hash conflicts with argument"
                  .formatted(nr.rowNo()));
            if (nextNr.utc() <= nr.utc())
              return false;
            else
              tailMap.remove(nr.rowNo());
          
          } else if (nextNr.utc() <= nr.utc())
            return false;
        }
      }
      
      // if any prev rows are notarized [on or] *after* nr
      // then remove those
      for (var headMap = notarizedRows.headMap(nr.rowNo()); !headMap.isEmpty(); ) {
        var prevNr = headMap.lastEntry().getValue();
        if (prevNr.utc() >= nr.utc()) {
          headMap.remove(prevNr.rowNo());
          notarizedRowDropped(nr);
        } else
          break;
      }
      
      notarizedRows.put(nr.rowNo(), nr); 
      return true;
    }
    
    
    
    protected void notarizedRowDropped(NotarizedRow dropped) {   }
    
    
    
    
    
    
    
    /**
     * Builds and returns a {@code NotaryPack} instance.
     * 
     * @throws IllegalStateException if no notarized rows have been added
     */
    public NotaryPack build() {
      final int count = count();
      if (count == 0)
        throw new IllegalStateException("builder is empty");
      NotarizedRow[] array =
          notarizedRows.values().toArray(new NotarizedRow[count]);
      
      return new NotaryPack(chainId, Lists.asReadOnlyList(array), true);
    }
    
    

  }
  
  
  private final LedgerId chainId;

  private final List<NotarizedRow> rows;
  
  /**
   * 
   * @param chainId not null
   * @param rows    not empty, and ordered strictly by ascending row no.s
   *                <em>and</em> witness times ({@code NotarizedRow.crum().utc()}
   * 
   * 
   */
  public NotaryPack(LedgerId chainId, List<NotarizedRow> rows) {
    this.chainId = chainId;
    this.rows = List.copyOf(rows);
    
    final int count = rows.size();
    if (count == 0)
      throw new IllegalArgumentException("empty rows");
    
    if (!chainId.type().isTimechain())
      throw new IllegalArgumentException(
          "chainId " + chainId + " is not a timechain");
    
    long prevRn = 0L;
    long prevUtc = 0L;
    for (int i = 0; i < count; ++i) {
      var nr = rows.get(i);
      if (nr == null)
        throw new NullPointerException(
            "at index %d in %s".formatted(i, rows));
      long nextRn = nr.rowNo();
      if (nextRn <= prevRn)
        throw new IllegalArgumentException(
            "out-of-sequence row no. %d (prev was %d) at [%d] in %s"
            .formatted(nextRn, prevRn, i, rows));
      prevRn = nextRn;
      
      long nextUtc = nr.crum().utc();
      if (nextUtc <= prevUtc)
        throw new IllegalArgumentException(
            """
            illegal (or redundant) witness times preceding index [%d]
            in %s -- prev UTC was %d; this UTC was %d"""
            .formatted(i, rows, prevUtc, nextUtc));
      prevUtc = nextUtc;
    }
    
    
  }
  
  
  public NotaryPack editChainId(Function<LedgerId, LedgerId> idMapper) {
    return editChainId(idMapper.apply(chainId));
  }
  
  
  public NotaryPack editChainId(LedgerId newId) {
    if (newId.equals(chainId))
      return this;
    if (!newId.type().isTimechain())
      throw new IllegalArgumentException(
          "newId type must be TIMECHAIN: " + newId);
    
    var params = ((TimechainInfo) newId.info()).params();
    if (!params.equalParams(chainParams()))
      throw new IllegalArgumentException(
          "chain params cannot be edited: expected %s; actual %s"
          .formatted(chainParams(), params));
    
    return new NotaryPack(newId, this.rows, false);
  }
  
  
  
  private NotaryPack(LedgerId chainId, List<NotarizedRow> rows, boolean ignored) {
    this.chainId = chainId;
    this.rows = rows;
  }
  
  
  
  /**
   * Returns an ordered list of notarized rows. Note, since every row hash
   * is dependent on the hash of previous rows, a notarized row also
   * implicitly notarizes the contents of all rows ahead of it.
   * 
   * <h4>Invariants</h4>
   * <p>
   * Notarized rows are sorted by
   * </p>
   * <ol>
   * <li>Strictly increasing (no dups) row no.s.</li>
   * <li>Strictly increasing (no dups) UTC witness times.</li>
   * </ol>
   */
  public List<NotarizedRow> notarizedRows() {
    return rows;
  }
  
  
  
  
  
  
  /**
   * Returns the timechain's id.
   */
  public final LedgerId chainId() {
    return chainId;
  }
  
  
  
  /**
   * Returns the timechain info embedded in the {@linkplain #chainId()}.
   * 
   * @return {@code (TimechainInfo) chainId().info()}
   */
  public final TimechainInfo info() {
    return (TimechainInfo) chainId.info();
  }
  
  
  
  /**
   * Returns the timechain params.
   * 
   * @return {@code info().params()}
   */
  public final ChainParams chainParams() {
    return info().params();
  }



  @Override
  public int serialSize() {
    int tally = 8;
    for (int index = rows.size(); index-- > 0; )
      tally += rows.get(index).serialSize();
    return tally;
  }



  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    out.putInt(chainId.id());
    final int count = rows.size();
    out.putInt(count);
    for (int index = 0; index < count; ++index)
      rows.get(index).writeTo(out);
    return out;
  }
  
  
  
  public static NotaryPack load(ByteBuffer in, Function<Integer, LedgerId> idLookup)
      throws SerialFormatException {
    
    int intId = in.getInt();
    LedgerId id;
    try {
      id = idLookup.apply(intId);
    } catch (Exception x) {
      throw new SerialFormatException(
          "unknown ledger id %d read from %s".formatted(intId, in));
    }
    
    final int count = in.getInt();
    
    if (count <= 0) {
      throw new SerialFormatException(
          "read count %d at offset %d in %s"
          .formatted(count, in.position() - 4, in));
    }
    
    if (count * NotarizedRow.MIN_BYTE_SIZE > in.remaining())
      throw new SerialFormatException(
          "read count %d at offset %d which will cause an underflow from %s"
          .formatted(count, in.position() - 4, in));
    
    NotarizedRow[] array = new NotarizedRow[count];
    for (int index = 0; index < count; ++index)
      array[index] = NotarizedRow.load(in);
    
    try {
      return new NotaryPack(id, Lists.asReadOnlyList(array));
      
    } catch (IllegalArgumentException iax) {
      throw new SerialFormatException(
          "on loading from " + in + " -- detail: " + iax.getMessage(), iax);
    }
  }
  

}

























