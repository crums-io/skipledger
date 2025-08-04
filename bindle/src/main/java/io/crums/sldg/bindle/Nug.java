/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;


import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.crums.io.Serial;
import io.crums.io.buffer.Partitioning;
import io.crums.sldg.bindle.tc.NotaryPack;
import io.crums.sldg.src.SourcePack;
import io.crums.util.Lists;

/**
 * Nuggets with a common serial format.
 * 
 * @see Nugget
 */
public interface Nug extends Nugget, Serial {
  
  /**
   * Returns the given {@code nugget} as a {@code Nug}.
   * 
   * @return  {@code nugget}, if it implements this interface;
   *          a view, otherwise
   */
  public static Nug asNug(Nugget nugget) {
    Objects.requireNonNull(nugget);
    
    return nugget instanceof Nug nug ? nug :
        new Nug() {
          @Override
          public LedgerId id() {
            return nugget.id();
          }
          @Override
          public MultiPath paths() {
            return nugget.paths();
          }
          @Override
          public Optional<SourcePack> sourcePack() {
            return nugget.sourcePack();
          }
          @Override
          public List<NotaryPack> notaryPacks() {
            return nugget.notaryPacks();
          }
          @Override
          public List<ForeignRefs> refPacks() {
            return nugget.refPacks();
          }
        };
  }
  
  
  @Override
  default ByteBuffer writeTo(ByteBuffer out) {
    out.putInt(id().id());
    paths().writeTo(out);
    var sources = sourcePack();
    if (sources.isEmpty())
      out.putInt(0);
    else {
      final int sizePosition = out.position();
      out.position(sizePosition + 4);
      sources.get().writeTo(out);
      int byteSize = out.position() - sizePosition - 4;
      out.putInt(sizePosition, byteSize);
    }
    Partitioning.writePartition(out, notaryPacks());
    Partitioning.writePartition(out, refPacks());
    return out;
  }
  

  @Override
  default int serialSize() {
    
    int tally = 4; // id
    tally += paths().serialSize();
    tally += 4;
    tally += sourcePack().map(SourcePack::serialSize).orElse(0);
    
    tally += Partitioning.serialSize(Lists.map(notaryPacks(), NotaryPack::serialSize));
    tally += Partitioning.serialSize(Lists.map(refPacks(), ForeignRefs::serialSize));
    
    return tally;
  }

}
