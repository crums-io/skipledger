/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import java.io.File;
import java.io.IOException;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Comparator;
import java.util.Objects;

import io.crums.io.FileUtils;
import io.crums.io.Opening;
import io.crums.io.buffer.BufferUtils;
import io.crums.io.channels.ChannelUtils;
import io.crums.sldg.fs.Filenaming;
import io.crums.sldg.packs.MorselPack;
import io.crums.sldg.packs.MorselPackBuilder;
import io.crums.sldg.src.SourceRow;
import io.crums.util.Strings;

/**
 * A {@linkplain MorselPack} serialized to a file.
 * <p>
 * Morsel files are assumed to fit in memory. In most cases, the majority of the space
 * is taken not by the hash evidence of the ledger's rows, but by the <em>contents</em> that
 * generated the input hashes for select rows (our {@linkplain SourceRow} abstraction). 
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

  public final static int HEADER_SIZE = 10;
  
  /**
   * File header magic + version string with room for a leading or trailing
   * digit. AKA version string.
   */
  public final static String HEADER_STRING = "MRSL  0.3 ";
  
  
  
  private final static String PREAMBLE =
      HEADER_STRING.substring(0, 5);
  
  private final static int PREAMBLE_SIZE = PREAMBLE.length();
  

  /**
   * Don't use me.
   * 
   * @see #header()
   */
  private final static ByteBuffer HEADER;
  
  static {
    String header = HEADER_STRING;
    HEADER = ByteBuffer.wrap(header.getBytes(Strings.UTF_8)).asReadOnlyBuffer();
    
    assert HEADER.remaining() == HEADER_SIZE;
  }
  
  /**
   * Returns a read-only view of the expected file header. It reads <em>MRSL  0.3</em> in ASCII.
   * (That's 10 bytes includng a trailing space.)
   */
  public static ByteBuffer header() {
    return HEADER.asReadOnlyBuffer();
  }
  
  /**
   * Header version comparison function.
   */
  public final static Comparator<String> HEADER_COMP =
      new Comparator<>() {
        @Override
        public int compare(String left, String right) {
          var leftVer = left.substring(PREAMBLE_SIZE, left.length()).trim();
          var rightVer = right.substring(PREAMBLE_SIZE, right.length()).trim();
          return leftVer.compareTo(rightVer);
        }
      };
  
  
  
  
  /**
   * Creates a new morsel file at the {@code target} file path using the given {@code builder}.
   * If the target is a path to a non-existent file, then the file is written to the destination
   * as given; if however the target is a path to a directory, then a file with a name generated
   * from the characteristics of the given {@linkplain builder} is returned.
   * 
   * @param target target file or directory; {@code null} is interpreted as the current directory
   * 
   * @return the file the morsel was written to
   * 
   * @see #writeMorselFile(WritableByteChannel, MorselPackBuilder)
   */
  public static File createMorselFile(File target, MorselPackBuilder builder) throws IOException {
    Objects.requireNonNull(builder, "null builder");
    target = prepareTarget(target, builder);
    
    try (var ch = Opening.CREATE.openChannel(target)) {
      writeMorselFile(ch, builder);
    }
    
    return target;
  }
  
  
  /**
   * Writes a morsel file to the given channel.
   * 
   * @param ch the channel, to a file or maybe bound to a socket
   * @param builder the morsel
   * 
   * @return the number of bytes written
   */
  public static int writeMorselFile(WritableByteChannel ch, MorselPackBuilder builder) throws IOException {
    ByteBuffer packBytes = builder.serialize();
    int size = HEADER_SIZE + packBytes.remaining();
    ChannelUtils.writeRemaining(ch, header());
    ChannelUtils.writeRemaining(ch, packBytes);
    return size;
  }
  
  
  
  private static File prepareTarget(File target, MorselPackBuilder builder) {
    
    if (target == null)
      target = new File(".");
    else if (!target.isDirectory())
      return target;
    
    return Filenaming.INSTANCE.newMorselFile(target, builder);
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
  public MorselFile(File file) throws ByteFormatException, HashConflictException {
    this(file, false);
  }
  

  /**
   * Creates a new instance.
   * 
   * @param file    the morsel file (suggest <em>.mrsl</em> extension)
   * @param direct  if {@code true} then instance will be backed by a direct buffer
   *                (not a memory-mapped file buffer in either case)
   * 
   * @throws ByteFormatException    if the file format is malformed
   * @throws HashConflictException  if the file is somehow corrupted, then the hashes won't match
   */
  public MorselFile(File file, boolean direct) throws ByteFormatException, HashConflictException {
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
    
    this.file = file;
    this.pack = MorselPack.loadVersioned(buffer, file);
  }
  
  
  
  
  /**
   * Checks the header.
   * On return the {@code buffer}'s position is advanced by {@linkplain #HEADER_SIZE}
   * 
   * @param buffer  buffer containing the file's contents
   * @param file    used for adding context to error messages
   * @return 0 if the header matches this version; &lt; 0, if the header is from a previous
   *           version; &gt; 0 if the header is from a future version
   */
  public static int advanceHeader(ByteBuffer buffer, Object file) {
    ByteBuffer expHeader = header();
    ByteBuffer header = BufferUtils.slice(buffer, HEADER_SIZE);
    
    if (!header.equals(expHeader)) {
      byte[] b = new byte[header.remaining()];
      header.get(b);
      String headerString = new String(b, Strings.UTF_8);
      if (headerString.startsWith(PREAMBLE)) {
        // ok, but nag
        int comp = HEADER_COMP.compare(headerString, HEADER_STRING);
        Level logLevel;
        var log = System.getLogger(MorselFile.class.getSimpleName());
        String msg = "Loading morsel from " + file;
        if (comp < 0) {
          msg +=  " with older version string '" + headerString +
                  "'; current version string is '" + HEADER_STRING + "'";
          logLevel = Level.DEBUG;
        } else if (comp > 0) {
          msg +=  " with version string '" + headerString +
                  "'; ahead of current version string is '" + HEADER_STRING +
                  "'. If there's legit a new version, consider upgrading the software.";
          logLevel = Level.INFO;
        } else {
          msg +=  " with well formed, but anamolous version string '"+ headerString +
                  "': expected version string is '" + HEADER_STRING + "'";
          logLevel = Level.WARNING;
        }
                
        log.log(logLevel, msg);
        return comp;
      
      } else
        throw new ByteFormatException(
            file + " header not recognized. Expected '" + HEADER_STRING +
            "'; actual was '" + headerString);
    }
    return 0;
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
