/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import io.crums.io.SerialFormatException;
import io.crums.io.buffer.Partitioning;
import io.crums.io.channels.ChannelUtils;

/**
 * Lazy-loading, unverified, serialized {@linkplain Bindle}.
 * 
 * 
 * @see #load(ByteBuffer)
 */
public class LazyBun extends LazyBindle implements Bundle {

  private LazyBun(LedgerId[] ids, Partitioning nuggets) {
    super(ids, nuggets);
  }
  

  @Override
  public int serialSize() {
    return Bundle.serialIdsSize(ids()) + ((Partitioning) nuggets).serialSize();
  }

  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    Bundle.writeIdsTo(ids(), out);
    ((Partitioning) nuggets).writeTo(out);
    return out;
  }


  @Override
  public void writeTo(WritableByteChannel out) throws IOException {
    
    ChannelUtils.writeRemaining(out, Bundle.serializeIds(ids()));
    ((Partitioning) nuggets).writeTo(out);
  }
  
  

  
  /**
   * Loads and returns an instance from serial form.
   * 
   * @see ObjectBundle#load(ByteBuffer)
   */
  public static LazyBun load(ByteBuffer in)
      throws SerialFormatException, MalformedBindleException {
    
    LedgerId[] ids = Bundle.loadIds(in);
    Partitioning partition = Partitioning.load(in);
    
    if (partition.getParts() < ids.length)
      throw new SerialFormatException(
          "partition has fewer parts ($d) than IDs (%d) %s"
          .formatted(partition.getParts(), ids.length, in));
    
    return new LazyBun(ids, partition);
  }
  

}
