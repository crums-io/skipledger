/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.Optional;

import io.crums.io.Opening;
import io.crums.io.channels.ChannelUtils;
import io.crums.sldg.SldgConstants;
import io.crums.util.TaskStack;

/**
 * Direct file-based implementation of {@code FrontierTable}. This is a fixed-width
 * (32-byte) table.
 */
public class FrontierTableFile implements FrontierTable {
  
  /** Creates and returns a new (read/write) instance. */
  public static FrontierTableFile create(File path, ByteBuffer header) {
    return openInstance(path, Opening.CREATE, header, false);
  }
  
  /** Returns a read/write an instance; either loaded or newly created on the file system. */
  public static FrontierTableFile createOnDemand(File path, ByteBuffer header) {
    return openInstance(path, Opening.CREATE_ON_DEMAND, header, true);
  }
  
  /** Loads and returns a read/write instance already existing on the file system. */
  public static FrontierTableFile loadReadWrite(File path, ByteBuffer header) {
    return openInstance(path, Opening.READ_WRITE_IF_EXISTS, header, true);
  }
  
  /** Loads a read-only instance from the file system. */
  public static FrontierTableFile loadReadOnly(File path, ByteBuffer header) {
    return openInstance(path, Opening.READ_ONLY, header, true);
  }
  
  /**
   * Returns a new instance.
   * 
   * @param path    the file path
   * @param mode    opening mode
   * @param header  the file header (determines the zero-offset)
   * @param check   if {@code true} and loading a previously persisted instance, then
   *                the {@code header} is checked
   */
  public static FrontierTableFile openInstance(
      File path, Opening mode, ByteBuffer header, boolean check) {

    Objects.requireNonNull(header, "null header");
    
    header = header.duplicate();  // just to be nice
    
    try (var onFail = new TaskStack()) {
      boolean create = !path.exists();
      FileChannel ch = mode.openChannel(path);
      onFail.pushClose(ch);
      int zeroOffset = header.remaining();
      
      if (create) {
        // write header if creating..
        if (zeroOffset > 0)
          ChannelUtils.writeRemaining(ch, 0, header.slice());
      
      } else if (check && zeroOffset > 0) {
        // verify header if existing..
        var headerRead = ChannelUtils.readRemaining(
            ch, 0, ByteBuffer.allocate(zeroOffset)).flip();
        if (!headerRead.equals(header))
          throw new IllegalArgumentException("header mismatch: " + path);
      }
      
      var table = new FrontierTableFile(ch, zeroOffset, path);
      onFail.clear();
      
      return table;
      
    } catch (IOException iox) {
      throw new UncheckedIOException("on opening " + path + "; mode=" + mode, iox);
    }
    
  }
  
  
  
  
  
  
  
  
  
  

  
  private final static int RW = SldgConstants.DIGEST.hashWidth();
  
  private final FileChannel file;
  private final long zeroOffset;
  private final File path;
  
  
  /**
   * Constructs an instance with no file path information.
   * 
   * @param file          seekable channel
   * @param zeroOffset    offset at which the rows begin (&ge; 0)
   */
  public FrontierTableFile(FileChannel file, long zeroOffset) {
    this(file, zeroOffset, null);
  }

  /**
   * Full constructor.
   * 
   * @param file          seekable channel
   * @param zeroOffset    offset at which the rows begin (&ge; 0)
   * @param path          the file path from which the channel was constructed.
   *                      Optional, not checked; informational only
   */
  public FrontierTableFile(FileChannel file, long zeroOffset, File path) {
    this.file = Objects.requireNonNull(file, "null file channel");
    this.zeroOffset = zeroOffset;
    this.path = path;
    
    if (zeroOffset < 0)
      throw new IllegalArgumentException("zeroOffset: " + zeroOffset);
    
    try {
      long fsize = file.size();
      if (fsize < zeroOffset)
        throw new IllegalArgumentException(
            "zero offset %d is beyond file size %d, filepath=%s"
            .formatted(zeroOffset, pathMsg()));
      
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
  private String pathMsg() {
    return path == null ? "[UNKNOWN]" : path.getPath();
  }
  
  
  public Optional<File> filepath() {
    return Optional.ofNullable(path);
  }

  /**
   * Returns the number of rows in the table.
   */
  @Override
  public final long size() {
    try {
      return checkedSize();
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }

  /**
   * Returns the number of rows in the table.
   * <p>
   * If overriden, then it's <em>controlling</em>: every method that needs to know the
   * size accesses this method.
   * </p>
   */
  protected long checkedSize() throws IOException {
    long fsize = file.size();
    assert fsize >= zeroOffset;
    // notice fractional rows are ignored
    return (fsize - zeroOffset) / RW;
  }

  @Override
  public ByteBuffer get(long index, ByteBuffer out) {
    try {
      Objects.checkIndex(index, checkedSize());
      out.clear().limit(RW);
      ChannelUtils.readRemaining(file, positionForIndex(index), out);
      return out.flip();
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on index [" + index + "]: " + iox.getMessage(), iox);
    }
  }

  private long positionForIndex(long index) {
    return index * RW + zeroOffset;
  }


  @Override
  public void append(ByteBuffer hash) {
    if (hash.remaining() != RW)
      throw new IllegalArgumentException("illegal hash width: " + hash);
    long index = size();
    long pos = positionForIndex(index);
    try {
      ChannelUtils.writeRemaining(file, pos, hash);
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on append (index=%d): %s".formatted(index, iox.getMessage()), iox);
    }
  }

  @Override
  public void trimSize(long newSize) {
    long size = size();
    if (newSize < 0)
      throw new IllegalArgumentException("newSize " + newSize);

    if (newSize < size) {
      long eof = positionForIndex(newSize);
      try {
        file.truncate(eof);
      } catch (IOException iox) {
        throw new UncheckedIOException(
            "on newSize %d; current size %d".formatted(newSize, size), iox);
      }
    }
  }


  @Override
  public void close() {
    if (file.isOpen()) {
      try {
        file.close();
      } catch (IOException iox) {
        throw new UncheckedIOException(iox);
      }
    }
  }
  
  
  @Override
  public String toString() {
    return
        "%s[path=%s,size=%d]".formatted(getClass().getSimpleName(), pathMsg(), size());
  }

}

