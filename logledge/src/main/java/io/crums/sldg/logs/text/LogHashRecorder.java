/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.Comparator;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.function.Predicate;

import io.crums.io.BackedupFile;
import io.crums.io.FileUtils;
import io.crums.io.Opening;
import io.crums.io.channels.ChannelUtils;
import io.crums.io.sef.Alf;
import io.crums.sldg.ByteFormatException;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.src.TableSalt;
import io.crums.util.Lists;
import io.crums.util.Strings;
import io.crums.util.TaskStack;

/**
 * <p>
 * Convention for the tracking files for a log file. Given a file, say
 * {@code acme-2023.log}, its tracking files are found in a sibling directory
 * {@code .sldg.acme-2023.log} containing the files {@code fhash} and {@code eor}.
 * </p>
 * <h2>Frontiers File</h2>
 * <p>
 * The file {@code fhash} contains the (skip ledger) row hashes for row numbers at some fixed
 * multiple of a power of 2, the so called delta exponent (dex).
 * </p>
 * <h3>Header</h3>
 * <p>
 * The header contains info about the dex, the table salt (secret!),
 * and optionally the token delimiters. It is formed from the byte
 * sequence {@code <magic unused dex salt cpLen delLen cpchars delchars>} where
 * </p>
 * <pre>
 * magic := 'fhash'
 * unused := BYTE^2
 * dex := BYTE
 * salt := BYTE^32
 * cpLen := BYTE
 * delLen := BYTE
 * cpChars := BYTE^cpLen
 * delChars := BYTE^delLen
 * </pre>
 * <p>Only the last 2 items {@code cpChars delChars} are variable length.
 * </p>
 * <h2>Offsets File</h2>
 * <p>
 * The file {@code eor} contains the end-of-row (EOL) file offsets for row numbers
 * at some fixed multiple of a power of 2, the so called delta exponent (dex).
 * Presently, the dex for this file must match that written in the frontiers file:
 * specified here, so that in the future, it may conceivably be more fine-grained.
 * </p>
 * <h3>Header</h3>
 * <p>
 * The header contains info about the dex. It is formed from the fixed length byte
 * sequence {@code <magic unused dex>} where
 * </p>
 * <pre>
 * magic := 'eor'
 * unused := BYTE^2
 * dex := BYTE
 * </pre>
 */
public class LogHashRecorder implements AutoCloseable {
  
  public final static String LOGNAME = "sldg.lhash";
  
  public final static String DIR_PREFIX = ".sldg.";

  public final static String FRONTIERS_FILE = "fhash";
  public final static String OFFSETS_FILE = "eor";
  
  public final static String STATE_PREFIX = "_";
  public final static String STATE_POSTFIX = ".state";

  private final static String FRONTIERS_MAGIC = FRONTIERS_FILE;
  private final static String OFFSETS_MAGIC = OFFSETS_FILE;
  private final static String STATE_MAGIC = "fstate";
  
  
  
  

  private final static int UNUSED_PAD = 2;
  
  private final static int FRONTIERS_FIXED_HDR_LEN =
      FRONTIERS_MAGIC.length() +
      UNUSED_PAD +
      1 +                         // dex
      SldgConstants.HASH_WIDTH +  // salt
      1 +                         // comment prefix len
      1;                          // delimiters len
  
  private final static int OFFSETS_HDR_LEN =
      OFFSETS_MAGIC.length() +
      UNUSED_PAD +
      1;                          // dex
  
  
  // records are implicitly static, btw
  private record FrontiersInfo(int dex, TableSalt salter, String cPrefix, String delimiters) {
    FrontiersInfo {
      checkDex(dex);
      if (!salter.isOpen())
        throw new IllegalArgumentException("salter is closed");
      if (cPrefix != null) {
        if (cPrefix.isEmpty())
          cPrefix = null;
      }
      if (delimiters != null) {
        if (delimiters.isEmpty())
          delimiters = null;
        else {
          checkDelimiters(delimiters);
        }
      }
    }
    
    
    Predicate<ByteBuffer> commentMatcher() {
      if (cPrefix == null)
        return null;
      byte[] pbytes = Strings.utf8Bytes(cPrefix);
      
      if (pbytes.length == 1) {
        final byte c = pbytes[0];
        return (b) -> b.get(b.position()) == c;
      }
      
      return new Predicate<ByteBuffer>() {
        
        @Override
        public boolean test(ByteBuffer t) {
          final int len = pbytes.length;
          if (t.remaining() < len)
            return false;
          int pos = t.position();
          for (int index = 0; index < len; ++index)
            if (t.get(pos + index) != pbytes[index])
              return false;
          return true;
        }
        
      };
    }
    
    long zeroOffset() {
      int commentPrefixLen = cPrefix == null ?
          0 : Strings.utf8Bytes(cPrefix).length;
      int delimitersLen = delimiters == null ?
          0 : Strings.utf8Bytes(delimiters).length;
      return FRONTIERS_FIXED_HDR_LEN + commentPrefixLen + delimitersLen;
    }
    
    
    static FrontiersInfo load(FileChannel file) throws IOException {
      ByteBuffer work = ByteBuffer.allocate(256);
      
      work.clear().limit(FRONTIERS_FIXED_HDR_LEN);
      
      ChannelUtils.readRemaining(file, 0, work).flip();
      
      assertMagic(FRONTIERS_MAGIC, work);
      advanceUnused(work);

      int dex = 0xff & work.get();
      int postSalt = work.position() + SldgConstants.HASH_WIDTH;
      TableSalt salter = new TableSalt(work.duplicate().limit(postSalt));
      
      work.position(postSalt);
      
      int commentPrefixLen = 0xff & work.get();
      int delimiterLen = 0xff & work.get();
      String commentPrefix;
      String delimiters;
      if (commentPrefixLen == 0)
        commentPrefix = null;
      else {
        work.clear().limit(commentPrefixLen);
        ChannelUtils.readRemaining(file, FRONTIERS_FIXED_HDR_LEN, work).flip();
        commentPrefix = Strings.utf8String(work);
      }
      if (delimiterLen == 0)
        delimiters = null;
      else {
        work.clear().limit(delimiterLen);
        long pos = FRONTIERS_FIXED_HDR_LEN + commentPrefixLen;
        ChannelUtils.readRemaining(file, pos, work).flip();
        delimiters = Strings.utf8String(work);
      }
      
      return new FrontiersInfo(dex, salter, commentPrefix, delimiters);
    }
    
    
    
    static FrontiersInfo create(FileChannel file, int dex, String cPrefix, String delimiters) throws IOException {
      
      byte[] salt = new SecureRandom().generateSeed(SldgConstants.HASH_WIDTH);
      
      var salter = new TableSalt(ByteBuffer.wrap(salt).asReadOnlyBuffer());
      
      var info = new FrontiersInfo(dex, salter, cPrefix, delimiters);
      
      // checks out.. write the file header
      
      ByteBuffer header = ByteBuffer.allocate(2048);
      writeHeader(header, FRONTIERS_MAGIC, dex);
      header.put(salt);
      byte[] cp, del;
      
      if (info.cPrefix() == null)
        cp = new byte[0];
      else
        cp = Strings.utf8Bytes(info.cPrefix());
      if (info.delimiters() == null)
        del = new byte[0];
      else
        del = Strings.utf8Bytes(delimiters);
      
      header.put((byte) cp.length).put((byte) del.length);
      header.put(cp).put(del);
      header.flip();
      
      ChannelUtils.writeRemaining(file, 0, header);
      
      return info;
    }
  } // FrontierInfo
  
  
  
  private record OffsetsInfo(int dex) {
    
    OffsetsInfo {
      checkDex(dex);
    }
    
    long zeroOffset() {
      return OFFSETS_HDR_LEN;
    }
    
    
    static OffsetsInfo load(FileChannel file) throws IOException {

      ByteBuffer work = ByteBuffer.allocate(OFFSETS_HDR_LEN);
      
      ChannelUtils.readRemaining(file, 0, work).flip();
      
      assertMagic(OFFSETS_MAGIC, work);
      advanceUnused(work);
      int dex = 0xff & work.get();
      return new OffsetsInfo(dex);
    }
    
    
    static OffsetsInfo create(FileChannel file, int dex) throws IOException {
      var info = new OffsetsInfo(dex);
      var buffer = ByteBuffer.allocate(OFFSETS_HDR_LEN);
      writeHeader(buffer, OFFSETS_MAGIC, dex).flip();
      ChannelUtils.writeRemaining(file, 0, buffer);
      return info;
    }
  } // OffsetsInfo
  
  
  
//  private static State loadState(File file) throws IOException {
//    try (var ch = Opening.READ_ONLY.openChannel(file)) {
//      return loadState(ch);
//    }
//  }
  
  private static State loadState(FileChannel file) throws IOException {
    int bufferSize = (int) Math.min(8 * 1024, file.size());
    var buffer = ByteBuffer.allocate(bufferSize);
    ChannelUtils.readRemaining(file, 0L, buffer).flip();
    
    assertMagic(STATE_MAGIC, buffer);
    advanceUnused(buffer);
    return State.loadSerial(buffer);
  }
  
  
  private static void saveState(File file, State state) throws IOException {
    BackedupFile backup = new BackedupFile(file);
    backup.moveToBackup();
    var ch = Opening.CREATE.openChannel(file);
    try (ch) {
      
      saveState(ch, state);
    
    } catch (Exception x) {
      ch.close();
      backup.rollback();
      if (x instanceof IOException iox)
        throw iox;
      if (x instanceof RuntimeException rx)
        throw rx;
      
      assert false; // not reachable
    }
  }
  
  private static void saveState(FileChannel file, State state) throws IOException {
    var buffer = ByteBuffer.allocate(64 * 128); // 8k
    buffer.put(Strings.utf8Bytes(STATE_MAGIC)).put((byte) 0).put((byte) 0);
    state.writeTo(buffer).flip();
    ChannelUtils.writeRemaining(file, 0L, buffer);
  }
  
  
  
  
  
  
  
  private static void checkDelimiters(String delimiters) {
    if (delimiters.length() > 256)
      throw new IllegalArgumentException(
          "too many delimiters: " + delimiters);
    try {
      new StringTokenizer(
          "fox jumping over lazy dog n such", delimiters);
    } catch (Exception error) {
      throw new IllegalArgumentException(
          "delimiters \"%s\" (quoted): %s"
          .formatted(delimiters, error.getMessage()));
    }
  }
  
  /**
   * Checks the magic bytes and advances the given buffer.
   * 
   * @param expected  expected bytes (as UTF-8 encoded)
   * @param buffer    positioned at beginning of magic bytes
   */
  private static void assertMagic(String expected, ByteBuffer buffer) {
    for (int index = 0; index < expected.length(); ++index) {
      if (buffer.get() != expected.charAt(index))
        throw new IllegalArgumentException(
            "magic bytes '%s' not matched".formatted(expected));
    }
  }
  
  private static ByteBuffer advanceUnused(ByteBuffer work) {
    return work.position(work.position() + UNUSED_PAD);
  }
  
  private static void checkDex(int dex) {
    if (dex < 0 || dex > 63)
      throw new IllegalArgumentException("dex out-of-bounds: " + dex);
  }
  
  
  private static ByteBuffer writeHeader(ByteBuffer header, String magic, int dex) {
    header.put(magic.getBytes(Strings.UTF_8));
    header.put((byte) 0).put((byte) 0);
    return header.put((byte) dex);
  }
  
  

  private static String stateFilename(State state) {
    return STATE_PREFIX + state.rowNumber() + STATE_POSTFIX;
  }
  
  
  private final static FileFilter STATE_FILE_FILTER = new FileFilter() {
    @Override
    public boolean accept(File pathname) {
      return inferStateRn(pathname) != -1L;
    }
  };
  
  
  private final static Comparator<File> STATE_FILE_COMP = new Comparator<File>() {
    @Override
    public int compare(File a, File b) {
      long ar = inferStateRn(a);
      long br = inferStateRn(b);
      return ar < br ? -1 : (ar == br ? 0 : 1);
    }
  };
  
  private static long inferStateRn(File stateFile) {
    var name = stateFile.getName();
    if (!name.startsWith(STATE_PREFIX) || !name.endsWith(STATE_POSTFIX))
        return -1L;
    var rn = name.substring(
        STATE_PREFIX.length(),
        name.length() - STATE_POSTFIX.length());
    try {
      return Long.parseLong(rn);
    
    } catch (NumberFormatException ignore) {
      return -1L;
    }
  }
  
  
  
  
  
  
  
  
  
  
  private final File log;
  
  private LogHasher hasher;
  
  

  /**
   * Creates a <em>new</em> peristence instance for the given {@code log} file
   * using the specified settings. Though bound to the {@code log}, the initial
   * recorded state is empty.
   * 
   * @param log         the log / journal
   * @param cPrefix     lines starting with this prefix are ignored.
   *                    May be {@code null}.
   * @param delimiters  token delimiters (used by {@code StringTokenizer}).
   *                    May be {@code null}.
   * @param dex         delta exponent. Row hashes and offsets are recorded for
   *                    row numbers that are a multiple of 2<sup>dex</sup>.
   * @throws IOException
   */
  public LogHashRecorder(File log, String cPrefix, String delimiters, int dex)
      throws IOException {
    this.log = log;
    if (!log.isFile())
      throw new IllegalArgumentException("not a file: " + log);
    FileUtils.ensureDir(getIndexDir());
    
    try (var onFail = new TaskStack()) {
      var ff = Opening.CREATE.openChannel(frontiersFile());
      onFail.pushClose(ff);
      
      var fInfo = FrontiersInfo.create(ff, dex, cPrefix, delimiters);
      
      var off = Opening.CREATE.openChannel(offsetsFile());
      onFail.pushClose(off);
      
      var oInfo = OffsetsInfo.create(off, dex);
      var fTable = new FrontierTableFile(ff, fInfo.zeroOffset());
      var rowOffsets = new Alf(off, oInfo.zeroOffset());
      
      this.hasher = new LogHasher(
          fInfo.salter(),
          fInfo.commentMatcher(),
          fInfo.delimiters(),
          rowOffsets,
          fTable,
          fInfo.dex());
      
      onFail.clear();
    }
    
  }
  

  /**
   * Loads an <em>existing</em> saved instance from the file system for the
   * given {@code log} file. The location of the backing files are
   * convention based.
   * 
   * @param log
   * @param readOnly
   * @throws IOException
   */
  @SuppressWarnings("resource")
  public LogHashRecorder(File log, boolean readOnly) throws IOException {
    this.log = log;
    if (!log.isFile())
      throw new IllegalArgumentException("not a file: " + log);

    Opening mode = readOnly ? Opening.READ_ONLY : Opening.READ_WRITE_IF_EXISTS;
    
    try (var onFail = new TaskStack()) {
      var fFile = mode.openChannel(frontiersFile());
      onFail.pushClose(fFile);
      
      var fInfo = FrontiersInfo.load(fFile);
      var fTable = new FrontierTableFile(fFile, fInfo.zeroOffset(), null);
      
      var oFile = mode.openChannel(offsetsFile());
      onFail.pushClose(oFile);
      
      var oInfo = OffsetsInfo.load(oFile);
      
      if (fInfo.dex() != oInfo.dex())
        throw new IllegalArgumentException(
            "dex mismatch: %d (frontiers) vs. %d (offests)"
            .formatted(fInfo.dex(), oInfo.dex()));
      
      var rowOffsets = new Alf(oFile, oInfo.zeroOffset());
      
      this.hasher = new LogHasher(
          fInfo.salter(),
          fInfo.commentMatcher(),
          fInfo.delimiters(),
          rowOffsets,
          fTable,
          fInfo.dex());
      
      onFail.clear();
    }
  }
  
  
  
  protected File getIndexDir() {
    return new File(log.getParentFile(), dirPrefix() + log.getName());
  }
  
  
  protected String dirPrefix() {
    return DIR_PREFIX;
  }
  
  
  public File offsetsFile() {
    return new File(getIndexDir(), OFFSETS_FILE);
  }
  
  
  public File frontiersFile() {
    return new File(getIndexDir(), FRONTIERS_FILE);
  }
  
  
  public State update() throws IOException {
    try (var logChannel = Opening.READ_ONLY.openChannel(log)) {
      State state = hasher.update(logChannel);
      saveState(state);
      return state;
    }
  }
  
  
  
  public StateHasher stateHasher() {
    return new StateHasher(hasher);
  }
  
  
  
  private void saveState(State state) throws IOException {
    var prev = getState();
    if (prev.isPresent() && prev.get().equals(state))
      return;
    
    File file = stateFile(state);
    saveState(file, state);
    
    // punting on what to do if prev.rowNumber() > state.rowNumber()
    // e.g. below..
//    
//    if (prevState.rowNumber() > state.rowNumber()) {
//      File[] hiStateFiles = getIndexDir().listFiles(new FileFilter() {
//        @Override
//        public boolean accept(File pathname) {
//          return inferStateRn(file) > state.rowNumber();
//        }
//      });
//    }
    
    
  }
  
  
//  private void warn(String msg) {
//    System.getLogger(LOGNAME).log(Level.WARNING, msg);
//  }
  
  
  private File stateFile(State state) {
    return new File(getIndexDir(), stateFilename(state));
  }
  
  
  
  public Optional<File> stateFile() {
    File[] stateFiles = getIndexDir().listFiles(STATE_FILE_FILTER);
    return
        stateFiles == null ?
          Optional.empty()
            : Lists.asReadOnlyList(stateFiles).stream().max(STATE_FILE_COMP);
  }
  
  
  
  public Optional<State> getState() {
    var stateFile = stateFile();
    if (stateFile.isEmpty())
      return Optional.empty();
    
    try (var ch = Opening.READ_ONLY.openChannel(stateFile.get())) {
      
      return Optional.of( loadState(ch) );
      
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on loading " + stateFile.get() + ": " + iox.getMessage(), iox);
      
    } catch (RuntimeException rx) {
      throw new ByteFormatException(
          "on loading " + stateFile.get() + ": " + rx.getMessage(), rx);
    }
  }
  
  
  
  
  
  
  @Override
  public void close() {
    hasher.close();
  }
  

}























