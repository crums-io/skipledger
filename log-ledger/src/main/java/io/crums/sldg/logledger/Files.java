/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger.Level;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

import io.crums.io.FileUtils;
import io.crums.io.Opening;
import io.crums.io.Serial;
import io.crums.io.SerialFormatException;
import io.crums.io.buffer.BufferUtils;
import io.crums.io.channels.ChannelUtils;
import io.crums.sldg.salt.TableSalt;
import io.crums.util.Strings;
import io.crums.util.TaskStack;

/**
 * Filenaming conventions and binary serialization of {@linkplain LogState},
 * {@linkplain Checkpoint}, {@linkplain TableSalt}, and {@linkplain Grammar}
 * to files. Nearly all files begin with a 4-byte header encoding the format version
 * no., and most are followed with the {@linkplain Serial serial} representation
 * of the object. Skipledger chain files, however, have no header.
 */
public class Files {
  
  private Files() {  }  // never
  
  /** File format version no. */
  public final static byte VERSION = 1;
  
  /** 3-byte magic {@code 'lgl'} and one byte version no.*/
  private final static byte[] HEADER = { 'l', 'g', 'l', VERSION };
  
  /** 4-byte header length. */
  public final static int HEADER_LENGTH = HEADER.length;
  
  /** Base file extension. Dot-prefixed */
  public final static String EXT_ROOT= ".lgl";
  
  /** Rules file extension. */
  public final static String RULES_FILE_EXT = ".rules" + EXT_ROOT;
  
  /** Salt file extension. */
  public final static String SALT_FILE_EXT = ".salt" + EXT_ROOT;
  
  /** Checkpoint file extension. */
  public final static String CHECKPOINT_FILE_EXT = ".ckpt" + EXT_ROOT;
  
//  public final static String LOG_STATE_FILE_EXT = ".ls" + EXT_ROOT;
  
//  public final static String GRAMMAR_FILE_EXT = ".gram" + EXT_ROOT;
  
  /** Alf (Ascending Long File) base extension. */
  public final static String ALF_EXT = ".alf" + EXT_ROOT;
  
  /** Offsets file extension. */
  public final static String OFFSETS_FILE_EXT = ".off" + ALF_EXT;
  
  /** Skipledger file extension. */
  public final static String SLDG_FILE_EXT = ".sldg" + EXT_ROOT;
  
  
  
  
  /** Returns the standard file header for this libaray. */
  public static ByteBuffer fileHeader() {
    return ByteBuffer.wrap(HEADER).asReadOnlyBuffer();
  }
  
  
  
  
  /**
   * Numbered filename comparator.
   */
  public final static Comparator<String> FILE_NO_COMP =
      new Comparator<String>() {
        
        @Override
        public int compare(String file1, String file2) {
          long no1 = filenameToNo(file1);
          long no2 = filenameToNo(file2);
          return Long.compare(no1, no2);
        }
      };
      
  
  private static class NumberedFilenameFilter implements FilenameFilter {
    private final String prefix;
    private final String ext;
    
    
    NumberedFilenameFilter(String prefix, String ext) {
      this.prefix = prefix;
      this.ext = ext;
    }

    @Override
    public boolean accept(File dir, String name) {
      if (!name.startsWith(prefix))
        return false;
      if (!name.endsWith(ext))
        return false;
      
      
      if (name.length() < prefix.length() + ext.length() + 2)
        return false;
      if (name.charAt(prefix.length()) != '-')
        return false;
      
      
      try {
        int noStart = prefix.length() + 1;
        int noEnd = name.length() - ext.length();
        if (Long.parseLong(name.substring(noStart, noEnd)) <= 0L)
          return false;
      } catch (Exception x) {
        return false;
      }
      
      return true;
    }
    
  }
  
  
  /**
   * Returns the default log-ledger storage directory for the given log-file
   * It is a sibling directory of the log-file named
   * {@code .lgl} (the {@linkplain #EXT_ROOT}). I.e. a hidden directory.
   * 
   * @return {@code new File(logFile.getParentFile(), EXT_ROOT)}
   */
  public static File defaultLglDir(File logFile) {
    return new File(logFile.getParentFile(), EXT_ROOT);
  }
  
  
  /** Returns the rules filename for the given log file. */
  public static String rulesFilename(File log) {
    return log.getName() + RULES_FILE_EXT;
  }
  

  /** Returns the offsets filename for the given log file. */
  public static String offsetsFilename(File log) {
    return log.getName() + OFFSETS_FILE_EXT;
  }
  
  

  /** Returns the skipledger chain filename for the given log file. */
  public static String skipledgerFilename(File log) {
    return log.getName() + SLDG_FILE_EXT;
  }
  
  
  /**
   * Returns the number encoded in the filename (under this library's
   * convention).
   * 
   * @throws IllegalArgumentException
   *         if the number embedded in the filename is not found
   */
  public static long filenameToNo(String filename) {
    int dash = filename.lastIndexOf('-');
    if (dash >= 0) {
      int startNo = dash + 1;
      int endNo = filename.indexOf('.', startNo);
      if (endNo >= 0)
        try {
          return Long.parseLong(filename.substring(startNo, endNo));
        } catch (Exception x) {  }
    }
    throw new IllegalArgumentException(
        "filename does not parse to no.: " + filename);
  }
  
  

  /**
   * Returns the filename for the given {@code ParseState} and
   * {@code log} file.
   */
  public static String checkpointFilename(File log, Checkpoint parseState) {
    return checkpointFilename(log, parseState.rowNo());
  }
  
  
  /**
   * Returns the filename for the {@code checkpoint} with the given row no.
   * 
   * @see #checkpointFilename(File, Checkpoint)
   */
  public static String checkpointFilename(File log, long rowNo) {
    return log.getName() + "-" + rowNo + CHECKPOINT_FILE_EXT;
  }
  
  
  
  /**
   * Lists the names of the checkpoint files, if any.
   * 
   * @param dir         the directory searched
   * @param logFile     the log file
   * 
   * @return filenames, sorted by row no.
   */
  public static String[] listCheckpointFiles(File dir, File logFile) {
    return listNumberedFiles(dir, logFile, CHECKPOINT_FILE_EXT);
  }
  
  
  /**
   * Lists the row numbers encoded in the parse-state filenames.
   * 
   * @param dir         the directory searched
   * @param logFile     the log file
   * 
   * @return sorted by row no.s
   */
  public static List<Long> listCheckpointNos(File dir, File logFile) {
    return listNos(dir, logFile, CHECKPOINT_FILE_EXT);
  }
  
  
  
  private static String[] listNumberedFiles(File dir, File logFile, String ext) {
    var filenames = dir.list(
        new NumberedFilenameFilter(logFile.getName(), ext));
    
    Arrays.sort(filenames, FILE_NO_COMP);
    return filenames;
  }
  
  
  private static List<Long> listNos(File dir, File logFile, String ext) {
    var filenames = dir.list(
        new NumberedFilenameFilter(logFile.getName(), ext));
    return
        Arrays.asList(filenames)
        .stream().map(Files::filenameToNo).sorted().toList();
  }

  
  
//  /**
//   * Lists the names of the log-state files, if any.
//   * 
//   * @param dir         the directory searched
//   * @param logFile     the log file
//   * 
//   * @return filenames, sorted by row no.
//   */
//  public static String[] listLogStateFiles(File dir, File logFile) {
//    return listNumberedFiles(dir, logFile, LOG_STATE_FILE_EXT);
//  }
  
  
  
  
  
  /**
   * Returns the filename containing the seed salt for the given
   * {@code log} file.
   */
  public static String saltFilename(File log) {
    return log.getName() + SALT_FILE_EXT;
  }
  
  
  
  
//  /**
//   * Loads and returns the {@code LogState} recorded in the given file
//   * 
//   * @see #writeToFile(LogState, File)
//   */
//  public static LogState loadLogState(File file) {
//    return LogState.load( loadSansHeader(file) );
//  }
//  
//  
//  /**
//   * Creates a new file and writes the given {@linkplain LogState}.
//   * 
//   * @see #loadLogState(File)
//   */
//  public static void writeToFile(LogState logState, File file) {
//    toFile(logState, file);
//  }
  
  
  
  /**
   * Creates a new file and writes pseudo-random bytes as salting seed.
   * 
   * @param file        usually named with {@linkplain #SALT_FILE_EXT}
   * @see #initTableSalt(byte[], File)
   * @see #loadTableSalt(File)
   */
  public static void initTableSalt(File file) {
    byte[] seed = new byte[LglConstants.SEED_SIZE];
    new SecureRandom().nextBytes(seed);
    initTableSalt(seed, file);
    for (int index = seed.length; index-- > 0;)
      seed[index] = 0;
  }
  
  
  /**
   * Creates a new file and writes the given bytes as salting seed.
   * 
   * @param seed        32-bytes
   * @param file        usually named with {@linkplain #SALT_FILE_EXT}
   * 
   * @see #initTableSalt(File)
   * @see #loadTableSalt(File)
   */
  public static void initTableSalt(byte[] seed, File file) {
    toFile(new SaltSeed(seed), file);
  }
  
  
  
  private static class SaltSeed implements Serial {
    
    private final byte[] seed;
    
    
    SaltSeed(byte[] seed) {
      this.seed = seed;
      if (seed.length != LglConstants.SEED_SIZE)
        throw new IllegalArgumentException(
            "expected %d bytes for salt seed; actual given was %d"
            .formatted(LglConstants.SEED_SIZE, seed.length));
    }

    @Override
    public int serialSize() {
      return LglConstants.SEED_SIZE;
    }

    @Override
    public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
      out.put(seed);
      return out;
    }

    @Override
    public ByteBuffer serialize() {
      return ByteBuffer.wrap(seed).asReadOnlyBuffer();
    }
    
  }
  
  
  /**
   * Loads and returns a {@linkplain TableSalt} from the given file.
   * 
   * @param file        usually named with {@linkplain #SALT_FILE_EXT}
   * @see #initTableSalt(File)
   */
  public static TableSalt loadTableSalt(File file) {
    var buffer = loadSansHeader(file);
    if (buffer.remaining() != LglConstants.SEED_SIZE)
      throw new SerialFormatException(
          "expected %d bytes following header; actual was %d"
          .formatted(LglConstants.SEED_SIZE, buffer.remaining()));
    
    return new TableSalt(buffer);
  }
  
  
  
  /**
   * Loads and returns the checkpont.
   * 
   * @param lglDir      lgl directory
   * @param rowNo       the checkpoint's row no.
   * @param logFile     the log file
   */
  public static Checkpoint loadCheckpoint(File lglDir, long rowNo, File logFile) {
    return Files.loadCheckpoint(
        new File(lglDir, checkpointFilename(logFile, rowNo)));
  }
  
  
  
  /**
   * Loads and returns a {@linkplain Checkpoint} from the given file.
   * 
   * @param file        usually named with {@linkplain #CHECKPOINT_FILE_EXT}
   * @see #writeToFile(Checkpoint, File)
   */
  public static Checkpoint loadCheckpoint(File file) {
    return Checkpoint.load( loadSansHeader(file) );
  }
  
  /**
   * Creates a new file and writes the given {@linkplain Checkpoint}.
   * 
   * @param file        usually named with {@linkplain #CHECKPOINT_FILE_EXT}
   * @see #loadCheckpoint(File)
   */
  public static void writeToFile(Checkpoint parseState, File file) {
    toFile(parseState, file);
  }
  
  
  
  /**
   * Creates a new file and writes a simple grammar using the given arguments.
   * 
   * @param skipBlankLines      {@code true} if blank lines are don't count
   *                            as ledger rows
   * @param tokenDelimiters     token delimiters, {@code null} means whitespace
   * @param commentPrefix       optional comment prefix
   * 
   * @see #loadSimpleGrammar(File)
   */
  public static void writeSimpleGrammar(
      boolean skipBlankLines,
      String tokenDelimiters,
      String commentPrefix,
      File file) {
    
    toFile(
        new SimpleGrammar(skipBlankLines, tokenDelimiters, commentPrefix),
        file);
  }
  
  
  /**
   * Loads and returns a {@linkplain Grammar} from the given file.
   * Note, presently "simple-grammar"s are optionally written to the "rules"
   * file. But since grammars and log-files can have a one-to-many
   * relationship, a grammar-only file may have use.
   * 
   * @param file        the file stored in.
   * @see #writeSimpleGrammar(boolean, String, String, File)
   */
  public static Grammar loadSimpleGrammar(File file) {
    return SimpleGrammar.load(loadSansHeader(file)).toGrammar();
  }
  
  
  
  record SimpleGrammar(
      boolean skipBlankLines,
      String tokenDelimiters,
      String commentPrefix) implements Serial {
    
    
    SimpleGrammar {
      if (tokenDelimiters == null)
        tokenDelimiters = "";
      else if (tokenDelimiters.length() > 32)
        throw new IllegalArgumentException(""
            + "tokenDelimiters too long: \"" + tokenDelimiters + "\"");
      if (commentPrefix == null)
        commentPrefix = "";
      else if (commentPrefix.length() > 32)
        throw new IllegalArgumentException(""
            + "commentPrefix too long: \"" + commentPrefix + "\"");
    }
    
    Grammar toGrammar() {
      Predicate<ByteBuffer> commentMatcher =
          commentPrefix.isEmpty() ? null : Grammar.prefixMatcher(commentPrefix);
      return new Grammar(
          tokenDelimiters.isEmpty() ? null : tokenDelimiters, commentMatcher, skipBlankLines);
    }
    
    @Override
    public int serialSize() {
      return writeTo(ByteBuffer.allocate(estimateSize())).flip().remaining();
    }

    @Override
    public int estimateSize() {
      return 3 + (tokenDelimiters.length() + commentPrefix.length()) * 2;
    }

    @Override
    public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
      out.put(skipBlankLines ? (byte) 1 : 0);
      out.put((byte) tokenDelimiters.length());
      out.put(Strings.utf8Bytes(tokenDelimiters));
      out.put((byte) commentPrefix.length());
      out.put(Strings.utf8Bytes(commentPrefix));
      return out;
    }
    
    
    static SimpleGrammar load(ByteBuffer in) {
      boolean skipBlankLines;
      {
        int flag = 0xff & in.get();
        if (flag == 0)
          skipBlankLines = false;
        else if (flag == 1)
          skipBlankLines = true;
        else
          throw new SerialFormatException(
              "skipBlankLines flag " + flag + ": " + in);
      }
      String tokenDelimiters = loadShortString(in);
      String commentPrefix = loadShortString(in);
      
      return new SimpleGrammar(skipBlankLines, tokenDelimiters, commentPrefix);
    }
    
    
    
    
    private static String loadShortString(ByteBuffer in) {
      int byteCount = 0xff & in.get();
      return byteCount == 0 ? "" : Strings.utf8String(BufferUtils.slice(in, byteCount));
    }
    
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  private static void toFile(Serial obj, File file) throws UncheckedIOException {
    toFile(obj.serialize(), file);
  }
  
  
  /**
   * Writes the given bytes to the specified file, prepending the contents
   * with the version header.
   * 
   * <h4>TODO Improvements</h4>
   * <ol>
   * <li>Stage to temp file, then move.</li>
   * <li>Support overwrite existing file (force).</li>
   * </ol>
   * 
   * @param out         remaining bytes are the file's contents (after file header)
   * @param file        target file
   */
  static void toFile(ByteBuffer out, File file) throws UncheckedIOException {
    try (var ch = Opening.CREATE.openChannel(file)) {
      ch.write(ByteBuffer.wrap(HEADER));
      ChannelUtils.writeRemaining(ch, out);
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on writing to " + file + " -- out buffer: " + out, iox);
    }
  }
  
  
  
  static ByteBuffer loadSansHeader(File file) throws UncheckedIOException {
    var buffer = FileUtils.loadFileToMemory(file);
    readVersion(buffer, file);
    return buffer;
  }
  
  
  private static int readVersion(ByteBuffer buffer, File path) {
    for (int index = 0; index < HEADER.length - 1; ++index) {
      if (HEADER[index] != buffer.get())
        throw new BadHeaderException(
            "bad or missing header: offset " + index + " in file " + path);
    }
    byte version = buffer.get();
    final int fileVersion = 0xff & version;
    if (version != VERSION) {
      if (version == 0)
        throw new BadHeaderException(
            "invalid header version (0) in file " + path);
      
      int codeVersion = 0xff & VERSION;
      if (fileVersion > codeVersion)
        System.getLogger(LglConstants.LOG_NAME)
        .log(
            Level.WARNING,
            "file version %d ahead of software version %d: %s",
            fileVersion, codeVersion, path);
    }
    return fileVersion;
  }
  
  
  
  public static FileChannel openVersioned(File file, Opening mode) {
    try (var onFail = new TaskStack()) {
      
      boolean exists = file.exists();
      var ch = mode.openChannel(file);
      onFail.pushClose(ch);
      if (exists)
        readVersion(ch, file);
      else 
        ch.write(fileHeader());
      
      onFail.clear();
      return ch;
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
  static int readVersion(FileChannel file, File path) {
    try {
      if (file.position() != 0L)
        file.position(0L);
      ByteBuffer buffer = ByteBuffer.allocate(HEADER.length);
      file.read(buffer);
      return readVersion(buffer.flip(), path);
      
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }

}






























