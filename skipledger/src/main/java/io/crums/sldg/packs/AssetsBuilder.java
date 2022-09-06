/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.packs;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.TreeMap;

import io.crums.io.Serial;
import io.crums.io.buffer.NamedParts;
import io.crums.sldg.src.SourceInfo;

/**
 * A more scalable specification for assets in a morsel file (<em>{@code .mrsl}</em>).
 * These include report templates, image resources used by those templates and meta-info
 * ({@linkplain MetaPack}/{@linkplain SourceInfo}. These are uniformly modeled as simple
 * name-to-bytes mappings. Certain names and prefixes are reserved for defined types.
 * This base class only knows about one of these: the {@code MetaPack} above.
 * 
 * <h2>Motivation</h2>
 * <p>
 * Version {@code 0.2} (of the file spec) introduced an optional meta pack describing
 * the columns in the ledger (names, descriptions, units and so on). Version {@code 0.3}, meanwhile,
 * introduces an optional JSON spec for generating PDF reports from source rows present in
 * morsel files. Each of these new capabilities has necessitated a revision to the file
 * spec for morsels. I aim to make the format less brittle this way for future features and
 * capabilities.
 * </p><p>
 * A second motivation for this design is to insulate the serialization and loading of morsels
 * from application/library specific knowledge/logic. For example, apart from this documentation,
 * this core module (library) knows nothing about the {@code reports} module.
 * </p>
 */
public class AssetsBuilder implements Serial {
  
  
  public final static String CRUMS = "/crums";

  public final static String META = CRUMS + "/meta";

  /**
   * Reserved prefix for library-defined stuff.
   */
  public final static String SYS_PREFIX = CRUMS.substring(0, CRUMS.length() - 1);

  
  
  
  
  
  
  
  
  private final TreeMap<String, ByteBuffer> namedBytes;

  // not worth making volatile w/o synchronizing access to namedBytes
  // ..and then if you do synchronize access, no point making this volatile :/
  private NamedParts snapshot;
  
  
  
  public AssetsBuilder() {
    namedBytes = new TreeMap<>();
  }
  
  
  /** Promotion (copy) constructor. Not a deep copy. */
  protected AssetsBuilder(AssetsBuilder promote) {
    this.namedBytes = promote.namedBytes;
  }
  
  
  
  /**
   * Sets the mapping for the meta pack.
   * 
   * @param meta  null means empty
   * @return the previous setting (not null, but may be empty)
   */
  public MetaPack setMeta(MetaPack meta) {
    var value = meta == null || meta.isEmpty() ? null : meta.serialize();
    var prev = setOrRemove(META, value);
    return prev == null ? MetaPack.EMPTY : MetaPack.load(prev);
  }
  
  
  /**
   * Returns the meta pack.
   * 
   * @return not null, but may be empty.
   */
  public MetaPack getMetaPack() {
    var bytes = namedBytes.get(META);
    return bytes == null ? MetaPack.EMPTY : MetaPack.load(bytes);
  }
  
  
  
  /**
   * Sets or removes the value associated with the given key.
   * 
   * @param key     not null, does not start with {@linkplain #SYS_PREFIX}
   * @param value   do not modify sliced contents (!) ({@code null} or no remaining mean <em>remove</em>)
   * @return        the previous value, if any; {@code null} o.w.
   */
  public ByteBuffer setValue(String key, ByteBuffer value) {
    Objects.requireNonNull(key, "null key");
    if (key.startsWith(SYS_PREFIX))
      throw new IllegalArgumentException(
          "'" + SYS_PREFIX + "' is a reserved key prefix: " + key);
    return setOrRemove(key, value);
  }
  
  
  /** No arg check implementation of {@linkplain #setValue(String, ByteBuffer)}. */
  protected final ByteBuffer setOrRemove(String key, ByteBuffer value) {
    snapshot = null;
    return value == null || !value.hasRemaining() ?
        namedBytes.remove(key) :
          namedBytes.put(key, value.slice());
  }
  
  
  
  /** Returns an immutable snapshot of this instance's state (the point of this class). */
  public NamedParts toNamedParts() {
    var snapshot = this.snapshot;
    if (snapshot == null)
      this.snapshot = snapshot = NamedParts.createInstance(namedBytes);
    return snapshot;
  }



  @Override
  public int serialSize() {
    return toNamedParts().serialSize();
  }
  
  @Override
  public int estimateSize() {
    return toNamedParts().estimateSize();
  }



  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    return toNamedParts().writeTo(out);
  }
  
  

}




