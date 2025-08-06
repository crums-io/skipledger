/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;


import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import io.crums.io.Serial;
import io.crums.io.SerialFormatException;
import io.crums.io.buffer.Partitioning;
import io.crums.io.channels.ChannelUtils;
import io.crums.util.Lists;

/**
 * Bindles with a common serial format.
 * 
 * @see Bindle
 */
public interface Bundle extends Bindle, Serial {
  
  /**
   * Returns the given {@code Bindle} as a {@code Bundle}.
   * 
   * @return  {@code bindle}, if it implements this interface;
   *          a view, otherwise
   */
  public static Bundle asBundle(Bindle bindle) {
    if (bindle instanceof Bundle bun)
      return bun;
    
    List<LedgerId> sortedIds;
    {
      var ids = bindle.ids();
      
      LedgerId[] array = ids.toArray(new LedgerId[ids.size()]);
      Arrays.sort(array, ID_COMP);
      
      sortedIds = Lists.asReadOnlyList(array);
    }
    
    
    return
        new Bundle() {
          @Override
          public Nugget getNugget(LedgerId id) {
            return bindle.getNugget(id);
          }
          @Override
          public List<LedgerId> ids() {
            return sortedIds;
          }
        };
  }
  

  
  
  /**
   * Order {@linkplain #ids()} are sorted by. Compares by
   * {@linkplain LedgerId#id()}.
   */
  public final static Comparator<LedgerId> ID_COMP =
      (a, b) -> Integer.compare(a.id(), b.id());
  
  
  static void writeIdsTo(List<LedgerId> ids, ByteBuffer out) {
    out.putInt(ids.size());
    for (var id : ids) {
      out.putInt(id.id());
      id.info().writeTo(out);
    }
  }
  
  static void writeIdsTo(List<LedgerId> ids, WritableByteChannel out)
      throws IOException {
    
    var intBuffer = ByteBuffer.allocate(4).putInt(ids.size()).flip();
    ChannelUtils.writeRemaining(out, intBuffer);
    for (var id: ids) {
      intBuffer.clear().putInt(id.id()).flip();
      ChannelUtils.writeRemaining(out, intBuffer);
      id.info().writeTo(out);
    }
  }
  
  static ByteBuffer serializeIds(List<LedgerId> ids) {
    var out = ByteBuffer.allocate(serialIdsSize(ids));
    writeIdsTo(ids, out);
    assert !out.hasRemaining();
    return out.flip();
  }
  
  static int serialIdsSize(List<LedgerId> ids) {
    int size = 4 * (1 + ids.size());
    for (var id : ids)
      size += id.info().serialSize();
    return size;
  }
  
  
  static LedgerId[] loadIds(ByteBuffer in)
      throws BufferUnderflowException, SerialFormatException {
    
    final int count = in.getInt();
    if (count <= 0)
      throw new SerialFormatException("read illegal id-count: " + count);
    if (count * 10 > in.remaining()) {
      in.position(in.limit());
      throw new BufferUnderflowException();
    }
    var ids = new LedgerId[count];
    int index = 0;
    try {
      
      for (; index < count; ++index) {
        int id = in.getInt();
        var info = LedgerInfo.load(in);
        ids[index] = new LedgerId(id, info);
      }
      
    } catch (BufferUnderflowException | SerialFormatException bsx) {
      throw bsx;
    } catch (Exception x) {
      throw new SerialFormatException(
          "failed to load LedgerId[%d]: %s".formatted(index, x.getMessage()),
          x);
    }
    
    return ids;
  }
  
  /**
   * The returned list is sorted by integral ID {@linkplain LedgerId#id()}.
   * 
   * @see #ID_COMP
   */
  @Override
  List<LedgerId> ids();
  
  

  @Override
  default int serialSize() {
    var ids = ids();
    int size = 8;       // 2 counts; one for ids, one for nugget partition
    for (var id : ids) {
      size += id.info().serialSize() + 4;                 // +4 for integral id
      size += Nug.asNug(getNugget(id)).serialSize() + 4;  // +4 for part-size
    }
    return size;
  }

  @Override
  default ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    writeIdsTo(ids(), out);
    Partitioning.writePartition(
        out,
        Lists.map(ids(), id -> Nug.asNug(getNugget(id))));
    return out;
  }

  @Override
  default void writeTo(WritableByteChannel out) throws IOException {
    var ids = ids();
    writeIdsTo(ids, out);
    Partitioning.writeSerialPartition(
        out, Lists.map(ids, id -> Nug.asNug(getNugget(id))));
  }
  
  
  

}













