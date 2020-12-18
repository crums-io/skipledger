/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;

import io.crums.io.Serial;
import io.crums.io.channels.ChannelUtils;

/**
 * Packages compact evidence of a row in a ledger, together with evidence
 * about how old it is.
 */
public class Nugget implements Serial {
  
  
  
  public static Nugget load(File file) throws IOException {
    try (FileInputStream in = new FileInputStream(file)) {
      return load(in.getChannel());
    }
  }
  
  
  public static Nugget load(InputStream in) throws IOException {
    return load(ChannelUtils.asChannel(in));
  }
  
  
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
    if (!path.first().equals(firstWitness.target())) {
      throw new IllegalArgumentException(
          "path.first() not equal to first.target(): " + path + " <> " + firstWitness);
    }
  }
  
  
  public final Row target() {
    return path.first();
  }
  
  
  public final long utc() {
    return firstWitness.utc();
  }
  
  
  public final Path ledgerPath() {
    return path;
  }
  
  
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




