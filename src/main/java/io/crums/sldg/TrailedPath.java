/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;

import io.crums.io.Serial;
import io.crums.model.CrumTrail;

/**
 * 
 */
public class TrailedPath implements Serial {
  

  public static TrailedPath load(ReadableByteChannel in) throws IOException {
    return load(in, ByteBuffer.allocate(8));
  }
  
  
  public static TrailedPath load(ReadableByteChannel in, ByteBuffer work) throws IOException {
    Path path = Path.load(in);
    CrumTrail trail = CrumTrail.load(in, work);
    return new TrailedPath(path, trail);
  }
  
  
  public static TrailedPath load(ByteBuffer in) throws BufferUnderflowException {
    Path path = Path.load(in);
    CrumTrail trail = CrumTrail.load(in);
    return new TrailedPath(path, trail);
  }
  
  
  
  
  
  private final Path path;
  private final CrumTrail trail;

  /**
   * 
   */
  public TrailedPath(Path path, CrumTrail trail) {
    this.path = Objects.requireNonNull(path, "null path");
    this.trail = Objects.requireNonNull(trail, "null trail");
    
    if (!trail.crum().hash().equals(path.last().hash()))
      throw new IllegalArgumentException(
          "trail.item()/path.last() hash mismatch (row number " + path.hiRowNumber() + ")");
  }
  
  
  
  
  
  /**
   * Returns the path from the row witnessed to the {@linkplain #target() target},
   * in reverse order.
   */
  public Path path() {
    return path;
  }
  
  
  /**
   * Returns the target of this instance, the first row in the {@linkplain #path() path}.
   */
  public Row target() {
    return path.first();
  }
  
  
  /**
   * Return the crumtrail, the witness time proof.
   * @return
   */
  public CrumTrail trail() {
    return trail;
  }
  
  
  /**
   * Returns the witness time.
   */
  public final long utc() {
    return trail.crum().utc();
  }
  
  
  /**
   * Two instances are equal <b>iff</b> they prove the same {@linkplain #utc() utc}
   * witness time for the same {@linkplain #target() target}.
   */
  public final boolean equals(Object o) {
    if (o == this)
      return true;
    else if (o instanceof TrailedPath) {
      TrailedPath other = (TrailedPath) o;
      return utc() == other.utc() && target().equals(other.target());
    } else
      return false;
  }
  
  
  /**
   * Consistent with equals.
   */
  public final int hashCode() {
    return Long.hashCode(utc());
  }
  
  
  /**
   * Returns a serial (binary) representation of this instance.
   * 
   * @see #writeTo(ByteBuffer)
   */
  public ByteBuffer serialize() {
    ByteBuffer buffer = ByteBuffer.allocate(serialSize());
    writeTo(buffer);
    assert !buffer.hasRemaining();
    return buffer.flip();
  }
  
  

  @Override
  public int serialSize() {
    return path.serialSize() + trail.serialSize();
  }






  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    path.writeTo(out);
    return trail.writeTo(out);
  }

}







