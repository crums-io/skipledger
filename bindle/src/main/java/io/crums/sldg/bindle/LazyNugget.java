/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;


import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import io.crums.io.SerialFormatException;
import io.crums.io.buffer.BufferUtils;
import io.crums.io.buffer.Partitioning;
import io.crums.io.channels.ChannelUtils;
import io.crums.sldg.bindle.tc.NotaryPack;
import io.crums.sldg.src.SourcePack;
import io.crums.util.CachingList;
import io.crums.util.Lists;

/**
 * A lazy-loading, serialized {@linkplain Nugget}. Since it's lazy-loading,
 * it offers little validation.
 * 
 * @see #load(Function, ByteBuffer)
 * @see ObjectNug
 */
public class LazyNugget implements Nug {
  
  
  

  private final LedgerId id;
  private final Function<Integer, LedgerId> idLookup;
  
  private final MultiPath paths;
  private final ByteBuffer sourceBuffer;
  private final Partitioning notaryBuffers;
  private final Partitioning refBuffers;
  
  private boolean useCachingList = true;
  
  /**
   * Full constructor.
   * 
   * @param id                  not {@code null}
   * @param idLookup            not {@code null}
   * @param pathBuffers         not empty
   * @param sourceBuffer        optional: may be {@code null}
   * @param notaryBuffers       not {@code null}, but may be empty
   * @param refBuffers          not {@code null}, but may be empty
   */
  private LazyNugget(
      LedgerId id,
      Function<Integer, LedgerId> idLookup,
      MultiPath paths,
      ByteBuffer sourceBuffer,
      Partitioning notaryBuffers,
      Partitioning refBuffers) {
    
    this.id = id;
    this.idLookup = Objects.requireNonNull(idLookup, "idLookup");
    this.paths = Objects.requireNonNull(paths, "paths");
    this.sourceBuffer =
        sourceBuffer == null || !sourceBuffer.hasRemaining() ?
            BufferUtils.NULL_BUFFER : sourceBuffer.slice();
    this.notaryBuffers = Objects.requireNonNull(notaryBuffers, "notaryBuffers");
    this.refBuffers = refBuffers;

    
    
    if (id.info().type().commitsOnly() && this.sourceBuffer.hasRemaining())
      throw new IllegalArgumentException(
          "Nugget type %s does not take a sourcePack: %s"
          .formatted(id.type(), sourceBuffer));
  }
  
  
  
  
  
  protected LazyNugget(LazyNugget copy) {
    this.id = copy.id;
    this.idLookup = copy.idLookup;
    this.paths = copy.paths;
    this.sourceBuffer = copy.sourceBuffer;
    this.notaryBuffers = copy.notaryBuffers;
    this.refBuffers = copy.refBuffers;
  }
  
  
  
  
  
  @Override
  public LedgerId id() {
    return id;
  }
  
  
  @Override
  public MultiPath paths() {
    return paths;
  }
  

  @Override
  public Optional<SourcePack> sourcePack() {
    if (!sourceBuffer.hasRemaining())
      return Optional.empty();
    return Optional.of(SourcePack.load(sourceBuffer.duplicate()));
  }
  
  
  public boolean useCachingList() {
    return useCachingList;
  }
  
  
  public LazyNugget useCachingList(boolean on) {
    useCachingList = on;
    return this;
  }
  

  @Override
  public List<NotaryPack> notaryPacks() {
    if (notaryBuffers.isEmpty())
      return List.of();
    
    List<NotaryPack> out =
          Lists.functorList(
            notaryBuffers.getParts(),
            index -> NotaryPack.load(notaryBuffers.getPart(index), idLookup));
    
    return useCachingList ?  CachingList.cache(out) : out;
  }
  
  

  @Override
  public List<ForeignRefs> refPacks() {
    if (refBuffers.isEmpty())
      return List.of();
    
    List<ForeignRefs> out =
          Lists.functorList(
            refBuffers.getParts(),
            index -> ForeignRefs.load(refBuffers.getPart(index), idLookup));

    return useCachingList ?  CachingList.cache(out) : out;
  }
  
  
  
  
  
  
  
  



  @Override
  public int serialSize() {
    
    return
        8 + // 4(id) + 4(source-buffer size)
        paths.serialSize() +
        notaryBuffers.serialSize() +
        refBuffers.serialSize() +
        sourceBuffer.remaining();
  }


  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    out.putInt(id.id());
    paths.writeTo(out);
    out.putInt(sourceBuffer.remaining());
    if (sourceBuffer.hasRemaining())
      out.put(sourceBuffer.duplicate());
    notaryBuffers.writeTo(out);
    refBuffers.writeTo(out);
    
    return out;
  }
  
  
  
  
  @Override
  public void writeTo(WritableByteChannel out) throws IOException {
    var intBuffer = ByteBuffer.allocate(4).putInt(id.id()).flip();
    ChannelUtils.writeRemaining(out, intBuffer);
    paths.writeTo(out);
    intBuffer.clear().putInt(sourceBuffer.remaining()).flip();
    ChannelUtils.writeRemaining(out, intBuffer);
    if (sourceBuffer.hasRemaining())
      ChannelUtils.writeRemaining(out, sourceBuffer.duplicate());
    notaryBuffers.writeTo(out);
    refBuffers.writeTo(out);
  }





  public static LazyNugget load(
      Function<Integer, LedgerId> idLookup,
      ByteBuffer in)
          throws SerialFormatException, BufferUnderflowException {
    
    LedgerId id;
    {
      int code = in.getInt();
      try {
        id = idLookup.apply(code);
      } catch (Exception x) {
        throw new SerialFormatException(
            "unknown ledger id %d: %s".formatted(code, in));
      }
    }

    MultiPath paths = MultiPath.load(in);
    
    ByteBuffer sourceBuffer;
    {
      int srcBufSize = in.getInt();
      if (srcBufSize < 0)
        throw new SerialFormatException(
            "%s: read negative source-buffer size (%d) from %s"
            .formatted(id, srcBufSize, in));
      if (srcBufSize > in.remaining())
        throw new SerialFormatException(
            "%s: read source-buffer size (%d) from %s (will underflow)"
            .formatted(id, srcBufSize, in));
      sourceBuffer = BufferUtils.slice(in, srcBufSize);
    }
    
      
    Partitioning notaryBuffers = Partitioning.load(in);
    Partitioning refBuffers = Partitioning.load(in);
    
    try {
      return new LazyNugget(
          id, idLookup, paths, sourceBuffer, notaryBuffers, refBuffers);
    
    } catch (Exception x) {
      throw new SerialFormatException(
          "%s: on validating data read from %s: %s"
          .formatted(id, in, x.getMessage()), x);
    }
  }
  

}














