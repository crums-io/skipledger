/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Function;

import io.crums.io.Serial;
import io.crums.io.SerialFormatException;
import io.crums.util.Lists;

/**
 * Sorted listed of references to a foreign ledger.
 * 
 * 
 * @see ForeignRefs.Builder
 */
public class ForeignRefs implements Serial {
  
  
  
  /**
   * Builds {@linkplain ForeignRefs}. <em>Not thread safe.</em>
   * 
   * @see #add(Reference)
   */
  public static class Builder {
    
    private final LedgerId foreignId;
    private final boolean noCells;
    
    private final TreeSet<Reference> refs = new TreeSet<>();
    
    
    public Builder(LedgerId foreignId) {
      this.foreignId = foreignId;
      this.noCells = foreignId.info().type().commitsOnly();
    }
    
    public Builder(LedgerId foreignId, Reference ref) {
      this(foreignId);
      add(ref);
    }
    
    /** Returns the foreign id, fixed on construction. */
    public final LedgerId foreignId() {
      return foreignId;
    }
    
    /**
     * Adds the given ref.
     * 
     * @return {@code true} iff {@code ref} was not already in the collection.
     */
    public boolean add(Reference ref) {
      if (noCells && ref.toCol() != -1)
        throw new IllegalArgumentException(
            "attempt to reference cell (%d) in commit-only ledger %s: %s"
            .formatted(ref.toCol(), foreignId, ref));
      return refs.add(ref);
    }
    
    
    /** Returns the number of refs in the collection. */
    public int count() {
      return refs.size();
    }
    
    
    /** Builds and returns a {@linkplain Reference} instance.*/
    public ForeignRefs build() {
      int count = refs.size();
      if (count == 0)
        throw new IllegalStateException(
            "instance is empty: at least one ref must be present");
      
      var array = refs.toArray(new Reference[count]);
      return new ForeignRefs(foreignId, array, true);
    }
    
  }

  
  
  private final LedgerId foreignId;
  
  private final Reference[] refs;

  /**
   * Checked constructor.
   * 
   * @param refs        sorted array; cloned
   * @throws NullPointerException
   *            if {@code refs} or any of its elements is {@code null}
   * @throws IllegalArgumentException
   *            if {@code refs} is empty or not sorted
   *            
   * @see Reference#compareTo(Reference)
   */
  public ForeignRefs(LedgerId foreignId, Reference[] refs)
      throws NullPointerException, IllegalArgumentException {
    this.foreignId = foreignId;
    this.refs = refs.clone();
    if (refs.length == 0)
      throw new IllegalArgumentException("empty refs array");
    int index = 0;
    try {
      if (refs[0] == null)
        throw new NullPointerException("refs[0]");
      for (index = 1; index < refs.length; ++index) {
        int comp = refs[index - 1].compareTo(refs[index]);
        if (comp >= 0)
          throw new IllegalArgumentException(failAtIndexMsg(index, comp));
      }
    } catch (NullPointerException npx) {
      throw new NullPointerException("refs[" + index + "]");
    }
  }
  
  
  private ForeignRefs(LedgerId foreignId, Reference[] refs, boolean ignored) {
    this.foreignId = foreignId;
    this.refs = refs;
  }
  
  
  
  /**
   * Returns the <em>destination</em> ledger ID of this instance's references
   * ({@linkplain #refs()}); the <em>source</em> ledger ID is just the ID of the
   * nugget this reference came from ({@linkplain Nugget#id()}).
   */
  public LedgerId foreignId() {
    return foreignId;
  }
  
  
  /**
   * Returns an instance with ID mapped by the given function.
   * 
   * @param idMapper ledger ID mapping function
   * 
   * @return {@linkplain #editForeignId(LedgerId) editForeignId}{@code (idMapper.apply(foreignId()))}
   */
  public ForeignRefs editForeignId(Function<LedgerId, LedgerId> idMapper) {
    return editForeignId(idMapper.apply(foreignId));
  }
  
  
  /**
   * Returns an instance with the given new ID.
   * 
   * @param newId
   * 
   * @return {@code this}, if {@code newId.equals(foreignId())}; a new
   *         instance, o.w.
   *         
   * @see LedgerId#verifyEdit(LedgerId)
   */
  public ForeignRefs editForeignId(LedgerId newId) {
    if (newId.equals(foreignId))
      return this;
    foreignId.verifyEdit(newId);
    return new ForeignRefs(newId, refs, false);
  }
  
  
  /**
   * Returns the sorted listed of {@linkplain Reference}s.
   * 
   * @see Reference#compareTo(Reference)
   * @see #indexOf(long)
   */
  public List<Reference> refs() {
    return Lists.asReadOnlyList(refs);
  }
  
  
  
  /**
   * Returns the list of row numbers referenced by source value (the
   * usual case). If a reference is to a foreign row's commitment hash,
   * then it does not contribute to the values in the returned list.
   * 
   * @return positive, ascending row no.s with no duplicates
   */
  public List<Long> foreignSourceNos() {
    return
        refs().stream()
        .filter(Reference::toSource)
        .map(Reference::toRow)
        .sorted()
        .distinct().toList();
  }
  
  
  
  
  
  
  
  
  /**
   * Returns the index of the first {@linkplain Reference} in
   * {@linkplain #refs()} with the given <em>from</em>-row-no.,
   * if found; the <em>insertion</em>-index encoded as a negative
   * number ({@code -index - 1}), otherwise.
   * 
   * @param fromRn  the {@linkplain Reference#fromRow()} value searched for
   * 
   * @return encoded insertion-index, if negative; found index, if positive
   */
  public int indexOf(long fromRn) {
    return Arrays.binarySearch(refs, Reference.newSearchKey(fromRn));
  }



  @Override
  public int serialSize() {
    return 8 + Reference.SERIAL_SIZE * refs.length;
  }



  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    out.putInt(foreignId.id());
    out.putInt(refs.length);
    for (int index = 0; index < refs.length; ++index)
      refs[index].writeTo(out);
    return out;
  }
  
  
  
  /**
   * Loads and returns the given instance using the given ID-lookup function.
   * 
   * @param in        position advanced by {@linkplain #serialSize()} on return
   * @param idLookup  provided by the morsel container
   * 
   * @return a well-formed instance
   */
  public static ForeignRefs load(ByteBuffer in, Function<Integer, LedgerId> idLookup)
      throws BufferUnderflowException, SerialFormatException {
    
    int id = in.getInt();
    LedgerId foreignId;
    try {
      foreignId = idLookup.apply(id);
    } catch (Exception x) {
      throw new SerialFormatException(
          "unknown ledger id %d read from %s".formatted(id, in));
    }
    int count = in.getInt();
    if (count <= 0)
      throw new SerialFormatException(
          "read illegal count (%d): %s".formatted(count, in));
    if (in.remaining() < count * Reference.SERIAL_SIZE)
      throw new SerialFormatException(
          "insufficient remaining bytes (count=%d): %s".formatted(count, in));
    
    Reference[] refs = new Reference[count];
    int index = 0;
    try {
      refs[0] = Reference.load(in);
      for (index = 1; index < count; ++index) {
        refs[index] = Reference.load(in);
        int comp = refs[index - 1].compareTo(refs[index]);
        if (comp >= 0)
          throw new SerialFormatException(failAtIndexMsg(index, comp));
          
      }
    } catch (IllegalArgumentException iax) {
      throw new SerialFormatException(
          "at index %d of %d ForeignRef instances: %s -- in: "
          .formatted(index, count, iax.getMessage(), in));
    }
    
    return new ForeignRefs(foreignId, refs, true);
  }
  
  
  
  private static String failAtIndexMsg(int index, int comp) {
    return (comp == 0 ? "duplicate" : "out-of-sequence") +
        " ref at index " + index;
  }
  
  
  

}




















