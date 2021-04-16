/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.db;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.logging.Logger;

import io.crums.io.FileUtils;
import io.crums.io.Opening;
import io.crums.io.buffer.BufferUtils;
import io.crums.io.channels.ChannelUtils;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.cli.FilenamingConvention;
import io.crums.sldg.entry.Entry;
import io.crums.sldg.packs.MorselPack;
import io.crums.sldg.packs.MorselPackBuilder;
import io.crums.util.Strings;

/**
 * A {@linkplain MorselPack} serialized to a file.
 * <p>
 * Morsel files are assumed to fit in memory. In most cases, the majority of the space
 * is taken not by the hash evidence of the ledger's rows, but by the <em>contents</em> that
 * generated the input hashes for select rows (our {@linkplain Entry} abstraction). 
 * </p>
 * 
 * <h1>Limits</h2>
 * <p>
 * The maximum theoretical size of a morsel file is 2,147,483,647 bytes, just under 2GB.
 * Anything approaching these sizes is likely to exhaust memory. Adding memory-mapped
 * buffer capabilities would make sense then--and straight-forward right here.
 * </p>
 */
public class MorselFile {

  public final static String HEADER_STRING = "MRSL 10";
  
  private final static String HEADER_SANS_VER =
      HEADER_STRING.substring(0, HEADER_STRING.length() - 2);
  
  
  private final static ByteBuffer HEADER;
  private final static int HEADER_SIZE;
  
  static {
    String header = HEADER_STRING;
    HEADER = ByteBuffer.wrap(header.getBytes(Strings.UTF_8)).asReadOnlyBuffer();
    HEADER_SIZE = HEADER.remaining();
  }
  
  /**
   * Returns a read-only view of the expected file header. It reads <em>MRSL 10</em> in ASCII.
   */
  public static ByteBuffer header() {
    return HEADER.asReadOnlyBuffer();
  }
  
  
  
  /**
   * Creates a new morsel file at the {@code target} file path using the given {@code builder}.
   * If the target is a path to a non-existent file, then the file is written to the destination
   * as given; if however the target is a path to a directory, then a file with a name generated
   * from the characteristics of the given {@linkplain builder} is returned.
   * 
   * @return the file the morsel was written to
   */
  public static File createMorselFile(File target, MorselPackBuilder builder) throws IOException {
    Objects.requireNonNull(target, "null target");
    Objects.requireNonNull(builder, "null builder");
    
    target = prepareTarget(target, builder);
    
    try (var ch = Opening.CREATE.openChannel(target)) {
      ChannelUtils.writeRemaining(ch, header());
      ChannelUtils.writeRemaining(ch, builder.serialize());
    }
    
    return target;
  }
  
  
  
  private static File prepareTarget(File target, MorselPackBuilder builder) {
    if (!target.isDirectory())
      return target;
    
    
    String name;
    if (builder instanceof DbMorselBuilder) {
      name = ((DbMorselBuilder) builder).getName();
    } else {
      name = target.getAbsoluteFile().getName();
    }
    
    String filename = FilenamingConvention.INSTANCE.morselFilename(name, builder);
    
    target = new File(target, filename);
    if (target.exists())
      throw new IllegalStateException("file already exists: " + target);
    
    return target;
  }
  
  
  
  
  
  
  
  
  private final File file;
  private final MorselPack pack;
  
  
  
  /**
   * Creates a new instance. Does not use a direct buffer.
   * 
   * @param file    the morsel file (suggest <em>.mrsl</em> extension)
   * 
   * @throws ByteFormatException    if the file format is malformed
   * @throws HashConflictException  if the file is somehow corrupted, then the hashes won't match
   */
  public MorselFile(File file) throws IOException, ByteFormatException, HashConflictException {
    this(file, false);
  }
  

  /**
   * Creates a new instance. Does not use a direct buffer.
   * 
   * @param file    the morsel file (suggest <em>.mrsl</em> extension)
   * @param direct  if {@code true} then instance will be backed by a direct buffer
   *                (not a memory-mapped file buffer in either case)
   * 
   * @throws ByteFormatException    if the file format is malformed
   * @throws HashConflictException  if the file is somehow corrupted, then the hashes won't match
   */
  public MorselFile(File file, boolean direct) throws IOException, ByteFormatException, HashConflictException {
    int size;
    {
      long bytes =  Objects.requireNonNull(file, "null file").length();
      if (bytes <= 0)
        throw new IllegalArgumentException(file + " is empty or not a file");
      else if (bytes > Integer.MAX_VALUE)
        throw new IllegalArgumentException(file + " is too big (" + bytes + " bytes)");
      
      size = (int) bytes;
    }

    if (size < HEADER_SIZE + 8)
      throw new ByteFormatException(file + " is not a morsel file");
    
    ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    FileUtils.loadFileToMemory(file, buffer);
    
    checkHeader(buffer, file);
    
    this.file = file;
    this.pack = MorselPack.load(buffer);
  }
  
  
  /**
   * On return {@code buffer}'s position is advanced by {@linkplain #HEADER_SIZE}
   */
  private void checkHeader(ByteBuffer buffer, File file) {
    ByteBuffer expHeader = header();
    ByteBuffer header = BufferUtils.slice(buffer, expHeader.remaining());
    if (!header.equals(expHeader)) {
      byte[] b = new byte[expHeader.remaining()];
      header.get(b);
      String headerString = new String(b, Strings.UTF_8);
      if (headerString.startsWith(HEADER_SANS_VER)) {
        // ok, but nag
        Logger log = Logger.getLogger(MorselFile.class.getSimpleName());
        log.warning(
            "Loading " + file + " with header '" + headerString + "'; " +
            "expected header is '" + HEADER_STRING + "'");
      } else
        throw new ByteFormatException(
            file + " header not recognized. Expected '" + HEADER_STRING +
            "'; actual was '" + headerString);
    }
  }
  
  
  /**
   * Returns the file the instance was loaded from.
   */
  public final File getFile() {
    return file;
  }
  
  
  /**
   * Returns the goodies.
   */
  public final MorselPack getMorselPack() {
    return pack;
  }
  

}
