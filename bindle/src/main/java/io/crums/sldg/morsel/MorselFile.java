/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import static io.crums.sldg.morsel.MorselConstants.LOG_NAME;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger.Level;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;

import io.crums.io.FileUtils;
import io.crums.io.Opening;
import io.crums.io.SerialFormatException;
import io.crums.io.buffer.FilePartition;
import io.crums.io.channels.ChannelUtils;
import io.crums.util.TaskStack;

/**
 * A lazy-loading {@linkplain Morsel}, backed by a file.
 * 
 * <h2>No Caching</h2>
 * <p>
 * Excepting the {@linkplain #ids()}, this class always retrieves data
 * directly from the morsel file. This is by design, because it allows
 * one to
 * </p>
 * <ol>
 * <li>Inspect large morsel file with little overhead. </li>
 * <li>Load a large morsel file that may not fit into memory.</li>
 * <li><em>Reduce</em> a large morsel file into smaller one with less
 * overhead.</li>
 * </ol>
 * 
 * 
 * @see #load(File)
 * @see #loadInMemory(File)
 */
public class MorselFile extends LazyMorsel implements Channel {
  
  
  public static void create(Morsel morsel, File file) throws UncheckedIOException {
    if (morsel.ids().isEmpty())
      throw new IllegalArgumentException("empty morsel: " + morsel);
    
    if (file.exists())
      throw new IllegalArgumentException(
          "attempt to overwrite existing file " + file);
    
    if (file.getParentFile() != null)
      FileUtils.ensureDir(file.getParentFile());
    
    Mug mug = Mug.asMug(morsel);
    
    try (var ch = Opening.CREATE.openChannel(file)) {
      assert ch.position() == 0L;
      
      ChannelUtils.writeRemaining(ch, magicHeader());
      mug.writeTo(ch);
      
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on creating morsel file " + file + " -- Caused by " + iox, iox);
    }
  }
  
  
  /**
   * Loads the given {@code file} into memory and returns
   * a lazy-loading morsel backed by the loaded bytes.
   * 
   * @param file        morsel file
   * 
   * @return a not-empty, in-memory morsel
   * 
   * @throws SerialFormatException
   *         if structurally malformed
   */
  public static LazyMug loadInMemory(File file)
      throws UncheckedIOException, SerialFormatException {
    
    try (var ch = Opening.READ_ONLY.openChannel(file)) {
      verifyHeader(ch, file);
      
      var mem = ByteBuffer.allocate((int) (ch.size() - HEADER_SIZE));
      ChannelUtils.readRemaining(ch, HEADER_SIZE, mem);
      
      return LazyMug.load(mem.flip());
      
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on loadInMemory(" + file + ") -- Caused by " + iox, iox);
    }
  }
  
  /**
   * Loads ands returns an instance backed by the given {@code file}.
   * 
   * @param file        morsel file
   * 
   * @return a not-empty, file-backed, empty morsel
   * 
   * @throws SerialFormatException
   *         if structurally malformed
   */
  public static MorselFile load(File file)
      throws UncheckedIOException, SerialFormatException {
    
    LedgerId[] ids = null;
    FilePartition partition = null;
    
    try (var onFail = new TaskStack()) {
      
      FileChannel ch = Opening.READ_ONLY.openChannel(file);
      onFail.pushClose(ch);
      
      verifyHeader(ch, file);
      
      ids = loadIds(ch, file);
      partition = new FilePartition(ch, ch.position());
      var morsel = new MorselFile(ids, partition, file, ch);
      onFail.clear();
      
      return morsel;
      
    } catch (IOException iox) {
      String msg = "Failed to load morsel from " + file;
      if (ids == null)
        msg += " (LedgerIds not loaded)";
      else if (partition == null)
        msg += " (Nugget partition not loaded)";
      
      msg += " -- Caused by: " + iox;
      
      throw new UncheckedIOException(msg, iox);
    }
  }
  
  private final static int MAX_IDS_SIZE = 64 * 1024 * 1024;
  

  /**
   * Loads and returns the serialized IDs. On return, the file channel
   * {@code ch}, is positioned at the next byte beyond the serialized
   * {@linkplain LedgerId}s.
   * 
   * @param ch            open channel on {@code file}
   * @param file          used only informationally in any exceptions thrown
   * 
   * @throws SerialFormatException
   *         if malformed data is read
   */
  static LedgerId[] loadIds(FileChannel ch, File file)
      throws IOException, SerialFormatException {
    
    return loadIds(ch, ch.size() < 1024 * 1024 ? 16 * 1024 : 64 * 1024, file);
  }
  
  /**
   * Loads and returns the serialized IDs. On return, the file channel
   * {@code ch}, is positioned at the next byte beyond the serialized
   * {@linkplain LedgerId}s.
   * 
   * @param ch            open channel on {@code file}
   * @param initAllocSize initial buffer allocation size
   * @param file          used only informationally in any exceptions thrown
   * 
   * @throws SerialFormatException
   *         if malformed data is read
   */
  static LedgerId[] loadIds(FileChannel ch, int initAllocSize, File file)
      throws IOException, SerialFormatException {
    
    if (initAllocSize <= 0)
      throw new IllegalArgumentException("initAllocSize: " + initAllocSize);
    
    final long fileSizeSansHeader = ch.size() - HEADER_SIZE;
    
    var buffer = ByteBuffer.allocate((int) Math.min(fileSizeSansHeader, initAllocSize));
    
    while (true) {
      
      ChannelUtils.readRemaining(ch, HEADER_SIZE + buffer.position(), buffer);
      
      assert !buffer.hasRemaining();
      
      try {
        
        var ids = Mug.loadIds(buffer.flip());
        ch.position(HEADER_SIZE + buffer.position());
        return ids;
      
      } catch (BufferUnderflowException bux) {
        
        if (buffer.capacity() >= MAX_IDS_SIZE)
          throw new SerialFormatException(
              "Ledger ID buffer exceeds 64MB; no. of IDs: " +
              buffer.clear().getInt());
        
        if (buffer.capacity() >= fileSizeSansHeader)
          throw new SerialFormatException(
              "Malformed morsel (may be truncated): " + file);
        
        var copy = ByteBuffer.allocate(
            (int) Math.min(fileSizeSansHeader, 2 * buffer.capacity()));
        
        copy.put(buffer.clear());
        buffer = copy;
        
      }  // catch
      
    } // while
  }
  
  private static void verifyHeader(FileChannel ch, File file)
      throws SerialFormatException, IOException {
    
    if (ch.size() < HEADER_SIZE)
      throw new SerialFormatException(
          "empty (or nearly empty) morsel file: " + file);
    
    ch.position(0L);
    var header = ByteBuffer.allocate(HEADER_SIZE);
    ChannelUtils.readRemaining(ch, header).flip();
    var expectedMagic = ByteBuffer.wrap(MAGIC);
    header.limit(expectedMagic.limit());
    if (!expectedMagic.equals(header))
      throw new SerialFormatException(
          "malformed header (magic bytes preamble) in morsel file: " + file);
    
    header.clear().position(MAGIC.length);
    int version = 0xff & header.getShort();
    
    if (version == 0)
      throw new SerialFormatException(
          "malformed header (unknown version zero) in morsel file: + file");
    
    if (version > VERSION) {
      System.getLogger(LOG_NAME).log(Level.WARNING,
          "Morsel file version (%d) ahead of software file version (%d): %s"
          .formatted(version, VERSION, file));
    }
  }
  
  private final static byte[] MAGIC = { 'M', 'O', 'R', 'S', 'E', 'L' };
  
  /** File format version. */
  public final static short VERSION = 1;
  
  private final static int HEADER_SIZE = MAGIC.length + 2;
  
  /**
   * DO NOT TOUCH!
   * 
   * @see #magicHeader()
   */
  private final static ByteBuffer MGM =
      ByteBuffer.allocate(HEADER_SIZE)
      .put(MAGIC).putShort(VERSION)
      .flip().asReadOnlyBuffer();
    
  
  
  private static ByteBuffer magicHeader() {
    return MGM.asReadOnlyBuffer();
  }
  
  
  
  
  
  
  
  //  - - - I N S T A N C E   M E M B E R S - - -
  
  private final File file;
  private final FileChannel ch;
  
  

  /**
   * 
   */
  private MorselFile(
      LedgerId[] ids, FilePartition partition, File file, FileChannel ch) {
    super(ids, partition);
    this.file = file;
    this.ch = ch;
  }
  
  
  /**
   * Returns the morsel file version.
   * 
   * @return
   * @throws UncheckedIOException
   */
  public int getVersion() throws UncheckedIOException {
    var shortBuffer = ByteBuffer.allocate(2);
    try {
      return
          0xffff &
          ChannelUtils.readRemaining(ch, MAGIC.length, shortBuffer).flip()
          .getShort();
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on reading file version -- Caused by: " + iox, iox);
    }
  }
  

  /**
   * Returns the backing file.
   */
  public File getFile() {
    return file;
  }


  /**
   * Closes the instance. On return {@linkplain #getNugget(LedgerId)}, no longer
   * works.
   */
  @Override
  public void close() throws IOException {
    ch.close();
  }

  
  @Override
  public boolean isOpen() {
    return ch.isOpen();
  }

}




