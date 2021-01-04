/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.crums.io.Serial;
import io.crums.io.channels.ChannelUtils;
import io.crums.util.Lists;
import io.crums.util.Tuple;

/**
 * Packages compact evidence of a row in a ledger, together with evidence
 * about how old it is.
 */
public class Nugget implements Serial {
  
  
  
  

  /**
   * Loads and returns a new instance from its serial form.
   * 
   * {@link #serialize()}
   */
  public static Nugget load(InputStream in) throws IOException {
    return load(ChannelUtils.asChannel(in));
  }

  /**
   * Unchecked {@linkplain #load(InputStream) load} more suitable for functional idioms.
   */
  public static Nugget loadUnchecked(InputStream in) throws UncheckedIOException {
    try {
      return load(in);
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  

  /**
   * Loads and returns a new instance from its serial form.
   * 
   * {@link #serialize()}
   */
  public static Nugget load(ReadableByteChannel in) throws IOException {
    return load(in, ByteBuffer.allocate(16));
  }
  
  
  public static Nugget load(ReadableByteChannel in, ByteBuffer work) throws IOException {
    Path path = Path.load(in);
    TrailedPath firstWitness = TrailedPath.load(in, work);
    return new Nugget(path, firstWitness);
  }
  
  
  
  public static Nugget load(ByteBuffer in) throws BufferUnderflowException {
    Path path = Path.load(in);
    TrailedPath firstWitness = TrailedPath.load(in);
    return new Nugget(path, firstWitness);
  }
  
  
  
  private final Path path;
  private final TrailedPath firstWitness;

  /**
   * 
   */
  public Nugget(Path path, TrailedPath firstWitness) {
    this.path = Objects.requireNonNull(path, "null path");
    this.firstWitness = Objects.requireNonNull(firstWitness, "null firstWitness");
    
    verify();
  }

  private void verify() {
    if (!path.target().equals(firstWitness.target())) {
      throw new IllegalArgumentException(
          "path.target() not equal to firstWitness.target(): " + path + " <> " + firstWitness);
    }
  }
  
  
  /**
   * Returns the target row.
   * 
   * @return {@linkplain #ledgerPath()}.{@linkplain Path#first() first()}
   */
  public final Row target() {
    return path.target();
  }
  
  
  /**
   * Returns the optional date <em>after</em> which the target row
   * was created.
   */
  public Optional<Long> afterUtc() {
    if (path.hasBeacon()) {
      // search for a beacon row at or before the target
      long targetRn = target().rowNumber();
      List<Tuple<Long, Long>> beacons = path.beacons();
      List<Long> bcRowNumbers = Lists.map(beacons, t -> t.a);
      int index = Collections.binarySearch(bcRowNumbers, targetRn);
      if (index < 0) {
        // the usual case (why build a nugget to a beacon row?)
        index = -1 - index;
        if (index != 0)
          return Optional.of(beacons.get(index - 1).b);
      } else
        return Optional.of(beacons.get(index).b);
    }
    return Optional.empty();
  }
  
  
  
  
  public Optional<Tuple<Row,Long>> timestampRow() {
    if (path.hasBeacon()) {
      // search for a beacon row at or before the target
      long targetRn = target().rowNumber();
      List<Long> bcRowNumbers = Lists.map(path.beacons(), t -> t.a);
      int index = Collections.binarySearch(bcRowNumbers, targetRn);
      if (index < 0) {
        // the usual case 
        index = -1 - index;
        if (index != 0) {
          return Optional.of(path.beaconRows().get(index - 1));
        }
      } else {
        // (why build a nugget to a beacon row?)
        // dunno, but we allow it
        return Optional.of(path.beaconRows().get(index));
      }
    }
    return Optional.empty();
    
  }
  
  
  
  /**
   * Returns the UTC time of the {@linkplain #firstWitness first recorded witness}.
   * 
   * @return {@linkplain #firstWitness()}.utc()
   */
  public final long utc() {
    return firstWitness.utc();
  }
  
  
  public final Path ledgerPath() {
    return path;
  }
  
  
  /**
   * Returns the evidence for the time it was first witnessed.
   */
  public final TrailedPath firstWitness() {
    return firstWitness;
  }
  
  
  @Override
  public int serialSize() {
    return path.serialSize() + firstWitness.serialSize();
  }
  
  

  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    path.writeTo(out);
    return firstWitness.writeTo(out);
  }

}




