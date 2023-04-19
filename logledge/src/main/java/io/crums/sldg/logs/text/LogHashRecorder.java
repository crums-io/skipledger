/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import static io.crums.sldg.logs.text.LogledgeConstants.DIR_PREFIX;
import static io.crums.sldg.logs.text.LogledgeConstants.FRONTIERS_FILE;
import static io.crums.sldg.logs.text.LogledgeConstants.FRONTIERS_MAGIC;
import static io.crums.sldg.logs.text.LogledgeConstants.LOGNAME;
import static io.crums.sldg.logs.text.LogledgeConstants.OFFSETS_FILE;
import static io.crums.sldg.logs.text.LogledgeConstants.OFFSETS_MAGIC;
import static io.crums.sldg.logs.text.LogledgeConstants.STATE_EXT;
import static io.crums.sldg.logs.text.LogledgeConstants.UNUSED_PAD;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.function.Predicate;

import io.crums.client.Client;
import io.crums.io.BackedupFile;
import io.crums.io.Opening;
import io.crums.io.channels.ChannelUtils;
import io.crums.io.sef.Alf;
import io.crums.sldg.MorselFile;
import io.crums.sldg.Path;
import io.crums.sldg.Row;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.SldgException;
import io.crums.sldg.WitnessedRowRepo;
import io.crums.sldg.fs.WitRepo;
import io.crums.sldg.logs.text.ContextedHasher.Context;
import io.crums.sldg.packs.MorselPackBuilder;
import io.crums.sldg.src.TableSalt;
import io.crums.sldg.time.TrailedRow;
import io.crums.sldg.time.WitnessRecord;
import io.crums.util.Lists;
import io.crums.util.Strings;
import io.crums.util.TaskStack;

/**
 * A state hasher bound to a particular log file.
 * <p>
 * Convention for the tracking files associated with the target log file.
 * Given a target file, say {@code acme-2023.log}, its tracking files are
 * found in a sibling directory {@code .sldg.acme-2023.log} containing an
 * {@code fhash} file specifying hashing and parsing rules, optionally an
 * {@code eor} file recording end-of-line (row) offsets, and (if anything
 * has been recorded) one or more {@code .fstate} files.
 * </p>
 * <h2>Frontiers File</h2>
 * <p>
 * The file {@code fhash} begins with a header that defines the parsing rules for the
 * log (text) file (token char delimiters and comment-line prefixes)
 * as well as the seed salt used in hashing tokens. Following the header, the
 * file may optionally include (skip ledger) row hashes for row numbers at some fixed
 * multiple of a power of 2, the so called delta exponent (dex).
 * </p>
 * <h3>Header</h3>
 * <p>
 * The header contains info about the dex, the table salt (secret!),
 * and optionally, comment-line prefix and token delimiters. It is formed from the byte
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
 * <p>Only the last 2 items {@code cpChars delChars} are variable length. If only
 * <em>state</em> files are to be used to track the log's state, then set {@code dex}
 * to 63.
 * </p>
 * <h3>Data (Row Hashes)</h3>
 * <p>
 * Row hashes (if any) are written to a fixed-width (32-byte) table immediately following
 * the above header. Since the header is variable length, byte alignment may not be
 * ideal (but, so what).
 * </p>
 * <h2>Offsets File</h2>
 * <p>
 * If the {@code dex} value of the preceding file {@code fhash} is less than 63,
 * then the {@code eor} file containing the end-of-row (EOL) file offsets for row numbers
 * that are multiples of 2<sup>{@code dex}</sup> is also created.
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
 * <h3>Offsets</h3>
 * <p>
 * End-of-row (EOL) offsets are written in simplified Elias-Fano encoding. This
 * provides good compression while also supporting efficient random access.
 * </p>
 * 
 * <h2>State Files</h2>
 * <p>
 * State files record the hash of a log up to a certain line (row). If data written to
 * the target log file is never modified and is only ever appended to, then updating
 * the state only involves playing the log forward from the (ending) offset recorded
 * in the last state file. They also contain additional information that allow replaying
 * the last row in the log. This helps verify the previous content of the log being
 * appended to has not been inadvertantly modified.
 * </p>
 * <h3>Header</h3>
 * <p>
 * The header contains fixed length byte sequence {@code <magic unused>} where
 * </p>
 * <pre>
 * magic := 'fstate'
 * unused := BYTE^2
 * </pre>
 * <h3>Data</h3>
 * <p>
 * A serial representation of the {@linkplain Fro row} is written out following
 * the header. See {@linkplain Fro#writeTo(ByteBuffer)}. This has sufficient information
 * to allow re-playing the last row of the log (useful as a sanity check).
 * </p>
 */
public class LogHashRecorder implements WitnessedRowRepo {
  
  
  
  
  public static boolean trackDirExists(File log) {
    return getIndexDir(log).isDirectory();
  }

  
  
  public static File getIndexDir(File log) {
    return new File(log.getParentFile(), DIR_PREFIX + log.getName());
  }
  

  
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
    
    /** Returns a new configured hasher (salter, comment-rules, token delimiters). */
    StateHasher stateHasher() {
      return new StateHasher(salter, commentMatcher(), delimiters);
    }
    
    
    Predicate<ByteBuffer> commentMatcher() {
      if (cPrefix == null)
        return null;
      return StateHasher.commentPrefixMatcher(cPrefix);
    }
    
    long zeroOffset() {
      int commentPrefixLen = cPrefix == null ?
          0 : Strings.utf8Bytes(cPrefix).length;
      int delimitersLen = delimiters == null ?
          0 : Strings.utf8Bytes(delimiters).length;
      return FRONTIERS_FIXED_HDR_LEN + commentPrefixLen + delimitersLen;
    }
    
    
    boolean hasBlocks() {
      return dex != NO_DEX;
    }
    
    
    static FrontiersInfo load(FileChannel file) throws IOException {
      return loadFi(file).toFrontiersInfo();
    }
    
    
    private static Fi loadFi(FileChannel file) throws IOException {
      ByteBuffer work = ByteBuffer.allocate(256);
      
      work.clear().limit(FRONTIERS_FIXED_HDR_LEN);
      
      ChannelUtils.readRemaining(file, 0, work).flip();
      
      assertMagic(FRONTIERS_MAGIC, work);
      advanceUnused(work);

      int dex = 0xff & work.get();
      byte[] salt = new byte[SldgConstants.HASH_WIDTH];
      work.get(salt);
      
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
      return new Fi(dex, salt, commentPrefix, delimiters);
    }
    
    
    private record Fi(int dex, byte[] salt, String cPrefix, String delimiters) {
      Fi {
        checkDex(dex);
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
      /** Warning: clears salt on invocation. */
      FrontiersInfo toFrontiersInfo() {
        var salter = new TableSalt(salt);
        return new FrontiersInfo(dex, salter, cPrefix, delimiters);
      }
      boolean hasBlocks() {
        return dex != NO_DEX;
      }
      Fi cPrefix(String prefix) {
        return 
            Objects.equals(prefix, prefix) ? this :
              new Fi(dex, salt, prefix, delimiters);
      }
      Fi dex(int exp) {
        return dex == exp ? this : new Fi(exp, salt, cPrefix, delimiters);
      }
    }
    
    // TODO: You should be able to update these, particularly from the default
    // settings (no dex, no comment-lines)
    static FrontiersInfo updateCPrefix(File dir, String cPrefix) throws IOException {
      if (cPrefix == null)
        cPrefix = "";
      return updateConfig(dir, Optional.empty(), Optional.of(cPrefix));
    }
    
    
    static FrontiersInfo updateDex(File dir, int dex) throws IOException {
      return updateConfig(dir, Optional.of(dex), Optional.empty());
    }
    
    
    static FrontiersInfo updateConfig(
        File dir, Optional<Integer> dex, Optional<String> cPrefix)
            throws IOException {

      if (!dir.isDirectory())
        throw new IllegalArgumentException("directory does not exist: " + dir);
      
      // Note: filepath indirection breaks here
      File ff = new File(dir, LogledgeConstants.FRONTIERS_FILE);
      File eor = new File(dir, LogledgeConstants.OFFSETS_FILE);
      

      Fi rawInfo;
      try (var ch = Opening.READ_ONLY.openChannel(ff)) {
        rawInfo = loadFi(ch);
      }
      
      Fi modInfo = rawInfo;
      if (dex.isPresent())
        modInfo = modInfo.dex(dex.get());
      if (cPrefix.isPresent())
        modInfo = modInfo.cPrefix(cPrefix.get());
     
      if (modInfo.equals(rawInfo))
        return rawInfo.toFrontiersInfo();

      new BackedupFile(eor).moveToBackup();
      
      replace(ff, modInfo);
      if (modInfo.hasBlocks())
        try (var ch = Opening.CREATE.openChannel(eor)) {
          OffsetsInfo.create(ch, modInfo.dex());
        }
      
      return modInfo.toFrontiersInfo();
    }
    
    
    private static void replace(File file, Fi rawInfo) throws IOException {
      BackedupFile bf = new BackedupFile(file);
      bf.moveToBackup();
      var ch = Opening.CREATE.openChannel(file);
      try (ch) {
        writeFi(ch, rawInfo);
      } catch (IOException iox) {
        ch.close();
        bf.rollback();
        throw iox;
      }
    }
    
    static FrontiersInfo create(FileChannel file, int dex, String cPrefix, String delimiters) throws IOException {
      
      byte[] salt = new SecureRandom().generateSeed(SldgConstants.HASH_WIDTH);
      Fi rawInfo = new Fi(dex, salt, cPrefix, delimiters);
      writeFi(file, rawInfo);
      return rawInfo.toFrontiersInfo();
    }
    
    
    
    static void writeFi(FileChannel file, Fi info) throws IOException {

      ByteBuffer header = ByteBuffer.allocate(2048);
      writeHeader(header, FRONTIERS_MAGIC, info.dex());
      header.put(info.salt());
      byte[] cp, del;
      
      if (info.cPrefix() == null)
        cp = new byte[0];
      else
        cp = Strings.utf8Bytes(info.cPrefix());
      if (info.delimiters() == null)
        del = new byte[0];
      else
        del = Strings.utf8Bytes(info.delimiters());
      
      header.put((byte) cp.length).put((byte) del.length);
      header.put(cp).put(del);
      header.flip();
      
      ChannelUtils.writeRemaining(file, 0, header);
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
  
  
  public final static int NO_DEX = 63;
  
  private static void checkDex(int dex) {
    if (dex < 0 || dex > NO_DEX)
      throw new IllegalArgumentException("dex out-of-bounds: " + dex);
  }
  
  
  private static ByteBuffer writeHeader(ByteBuffer header, String magic, int dex) {
    header.put(magic.getBytes(Strings.UTF_8));
    header.put((byte) 0).put((byte) 0);
    return header.put((byte) dex);
  }
  
  
  
  private static Logger sysLogger() {
    return System.getLogger(LOGNAME);
  }
  
  
  
  
  
  
  
  
  
  
  private final File log;
  
  private final FrontiersInfo config;
  
  private final EndStateRecorder endStateRecorder;
  
  private final BlockRecorder blockRecorder;
  
  private SldgException error;
  
  private WitRepo witRepo;
  


  /**
   * Creates a <em>new</em> peristence instance for the given {@code log} file
   * using the given settings. Only state files are recorded.
   * 
   * @param log         text-based log / journal tracked
   * @param cPrefix     lines starting with this prefix are ignored.
   *                    May be {@code null}.
   * @param delimiters  token delimiters (used by {@code StringTokenizer}).
   *                    May be {@code null}.
   */
  public LogHashRecorder(File log, String cPrefix, String delimiters)
      throws IOException {
    this(log, cPrefix, delimiters, NO_DEX);
  }
  

  /**
   * Creates a <em>new</em> peristence instance for the given {@code log} file
   * using the specified settings. Though bound to the {@code log}, the initial
   * recorded state is empty.
   * 
   * @param log         text-based log / journal tracked
   * @param cPrefix     lines starting with this prefix are ignored.
   *                    May be {@code null}.
   * @param delimiters  token delimiters (used by {@code StringTokenizer}).
   *                    May be {@code null}.
   * @param dex         delta exponent. Row hashes and offsets are recorded for
   *                    row numbers that are a multiple of 2<sup>dex</sup>.
   */
  public LogHashRecorder(File log, String cPrefix, String delimiters, int dex)
      throws IOException {
    this.log = log;
    if (!log.isFile())
      throw new IllegalArgumentException("not a file: " + log);
    
    try (var onFail = new TaskStack()) {
      var ff = Opening.CREATE.openChannel(frontiersFile());
      onFail.pushClose(ff);
      
      this.config = FrontiersInfo.create(ff, dex, cPrefix, delimiters);
      
      this.endStateRecorder = new EndStateRecorder(getIndexDir(), false);
      
      if (config.hasBlocks()) {

        var off = Opening.CREATE.openChannel(offsetsFile());
        onFail.pushClose(off);
        
        var oInfo = OffsetsInfo.create(off, dex);
        var fTable = new FrontierTableFile(ff, config.zeroOffset());
        var rowOffsets = new Alf(off, oInfo.zeroOffset());
        
        this.blockRecorder = new BlockRecorder(fTable, rowOffsets, dex);
      
      } else {
        
        ff.close();
        this.blockRecorder = null;
      }
      
      
      onFail.clear();
    }
    
  }
  

  /**
   * Loads an <em>existing</em> saved instance from the file system for the
   * given {@code log} file. The location of the backing files are
   * convention based.
   * 
   * @param log       the log file tracked
   */
  public LogHashRecorder(File log) throws IOException {
    this(log, false);
  }
  

  /**
   * Loads an <em>existing</em> saved instance from the file system for the
   * given {@code log} file. The location of the backing files are
   * convention based.
   * 
   * @param log       the log file tracked
   * @param readOnly  if {@code true} then no data can be modified since the
   *                  backing files are opened in read-only mode
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
      
      this.config = FrontiersInfo.load(fFile);
      
      this.endStateRecorder = new EndStateRecorder(getIndexDir(), false);
      
      if (config.hasBlocks()) {

        var fTable = new FrontierTableFile(fFile, config.zeroOffset(), null);
        
        var oFile = mode.openChannel(offsetsFile());
        onFail.pushClose(oFile);
        
        var oInfo = OffsetsInfo.load(oFile);
        
        if (config.dex() != oInfo.dex())
          throw new IllegalArgumentException(
              "dex mismatch: %d (frontiers) vs. %d (offests)"
              .formatted(config.dex(), oInfo.dex()));
        
        var rowOffsets = new Alf(oFile, oInfo.zeroOffset());
        
        this.blockRecorder = new BlockRecorder(fTable, rowOffsets, config.dex);
        
      } else {
        
        fFile.close();
        this.blockRecorder = null;
      }
      
      onFail.clear();
    }
  }
  
  
  public File getJournal() {
    return log;
  }
  
  /** Returns the comment-line prefix. (<em>Leading whitespace is not stripped</em>.) */
  public Optional<String> commentLinePrefix() {
    return Optional.ofNullable(config.cPrefix());
  }
  

  /** Returns any special token delimiters, if set. Defaults to ASCII whitespace chars. */
  public Optional<String> tokenDelimiters() {
    return Optional.ofNullable(config.delimiters());
  }
  

  /**
   * Returns the delta exponent.
   * 
   * @return &ge; 0 and &le; {@link #NO_DEX}
   */
  public int dex() {
    return config.dex();
  }
  
  
  /**
   * The directory path tracking files are located in.
   */
  public File getIndexDir() {
    return new File(log.getParentFile(), dirPrefix() + log.getName());
  }
  
  
  protected String dirPrefix() {
    return DIR_PREFIX;
  }
  
  
  /**
   * EOL row offsets are recorded in this file. Does not exist in the
   */
  public File offsetsFile() {
    return new File(getIndexDir(), OFFSETS_FILE);
  }
  
  
  /**
   * Row hashes for rows at some multiple of 2<sup>k</sup> are
   * recorded in this file. It's also header contains the secret seed for
   * salting values. Always exists.
   */
  public File frontiersFile() {
    return new File(getIndexDir(), FRONTIERS_FILE);
  }
  
  
  public State update() throws IOException, OffsetConflictException, RowHashConflictException {
    return updateImpl(hasher());
  }
  
  
  private State updateImpl(ContextedHasher hasher) throws IOException, SldgException {
    try (var logChannel = Opening.READ_ONLY.openChannel(log)) {
      this.error = null;
      var fro = endStateRecorder.lastFro();
      return
          fro.isPresent() ?
              hasher.play(logChannel, fro.get().preState()) :
                hasher.play(logChannel);
    
    } catch (SldgException error) {
      this.error = error;
      throw error;
    }
  }
  
  
  public State update(int maxRows) throws IOException, OffsetConflictException, RowHashConflictException {
    if (maxRows < 1)
      throw new IllegalArgumentException("maxRows " + maxRows + " < 1");
    
    final long maxRn = maxRows + getState().map(State::rowNumber).orElse(0L);
    
    Context limiter = newLimitContext(maxRn);
    
    var hasher = hasher().appendContext(limiter);
    
    return updateImpl(hasher);
  }
  
  
  
  private Context newLimitContext(long maxRn) {
    return new Context() {
      long rn = 0;
      @Override
      public void observeLedgeredLine(Fro frontier, long offset) {
        rn = frontier.rowNumber();
      }
      @Override
      public boolean stopPlay() {
        return rn >= maxRn;
      }
    };
  }
  
  
  
  public State verify()
      throws IOException, OffsetConflictException, RowHashConflictException {

    var fro = endStateRecorder.lastFro();
    if (fro.isEmpty())
      return State.EMPTY; // nothing to verify
    
    var frontier = fro.get();
    var hasher = hasher().appendContext( newLimitContext(frontier.rowNumber()) );
    
    try {
      this.error = null;
      
      return hasher.play(log);
      
    } catch (SldgException sx) {
      this.error = sx;
      throw sx;
    }
  }
  
  
  
  public State fixOffsets(long minRn)
      throws IOException, OffsetConflictException, RowHashConflictException {
    if (minRn < 1)
      throw new IllegalArgumentException("minRn " + minRn);
    
    
    trimBlockOffsets(minRn);
    endStateRecorder.repairsOffsets(true);
    
    
    try {
      return verify();
    } finally {
      endStateRecorder.repairsOffsets(false);
    }
  }
  
  
  
  private void trimBlockOffsets(long maxRn) throws IOException {
    if (blockRecorder == null)
      return;
    long newCount = maxRn / blockRecorder.rnDelta();
    if (newCount < blockRecorder.endOffsetsRecorded())
      blockRecorder.trimOffsetsRecorded(newCount);
  }
  
  private void trimBlocks(long maxRn) throws IOException {
    if (blockRecorder == null)
      return;
    long newCount = maxRn / blockRecorder.rnDelta();
    if (newCount < blockRecorder.frontiersRecorded())
      blockRecorder.trimRecorded(newCount);
  }
  
  
  
  
  public State rollback(long maxRn)
      throws IOException, OffsetConflictException, RowHashConflictException {
    
    if (maxRn < 1)
      throw new IllegalArgumentException("maxRn " + maxRn);
    
    var repoOpt = getWitRepo();
    if (repoOpt.isPresent()) {
      var repo = repoOpt.get();
      int index = Collections.binarySearch(repo.getTrailedRowNumbers(), maxRn + 1);
      int newCount = index < 0 ? -1 - index : index;
      if (newCount < repo.getTrailCount())
        repo.trimTrails(newCount);
    }
    
    trimBlocks(maxRn);
    endStateRecorder.deleteFiles(maxRn + 1);
    stateMorselFiler().deleteFiles(maxRn + 1);

    var hasher = hasher().appendContext( newLimitContext(maxRn) );
    return updateImpl(hasher);
  }
  
  
  
  protected ContextedHasher hasher() {
    var stateMorselFiler = new StateMorselFiler(getIndexDir());
    boolean hasMorsels = stateMorselFiler.lastFile().isPresent();
    var contexts = new ArrayList<Context>(3);
    if (config.hasBlocks())
      contexts.add(blockRecorder);
    contexts.add(endStateRecorder);
    if (hasMorsels)
      contexts.add(stateMorselFiler);
    Context[] ctxArray = contexts.toArray(new Context[contexts.size()]);
    return new ContextedHasher(config.stateHasher(), ctxArray);
  }
  
  
  
  public boolean hasBlocks() {
    return config.hasBlocks();
  }
  
  
  
  
  
  
  /**
   * Returns the last recorded state of the ledger, if any.
   */
  public Optional<State> getState() {
    return endStateRecorder.lastFro().map(Fro::state);
  }
  
  
  
  
  /**
   * Returns a no-side-effects state hasher. This represents the parsing and
   * hashing rules in their most basic form.
   */
  public StateHasher stateHasher() {
    return config.stateHasher();
  }
  
  
  
  
  
  @Override
  public void close() {
    
    if (witRepo != null)
      witRepo.close();
    if (blockRecorder != null)
      blockRecorder.close();
  }
  
  
  
  public boolean hasError() {
    return error != null;
  }
  
  
  
  public Optional<OffsetConflictException> getOffsetConflict() {
    return error instanceof OffsetConflictException ocx ?
        Optional.of(ocx) : Optional.empty();
  }
  
  
  public Optional<RowHashConflictException> getHashConflict() {
    return error instanceof RowHashConflictException hcx ?
        Optional.of(hcx) : Optional.empty();
  }
  
  
  
  /**
   * Removes the backup files (filenames prefixed with {@code ~}).
   * 
   * @return the number of backup files deleted, possibly zero
   */
  public int clean() {
    File[] backups = getIndexDir().listFiles((f) -> f.getName().startsWith("~"));
    if (backups == null || backups.length == 0)
      return 0;
    
    for (var f : backups)
      if (!f.delete())
        sysLogger().log(Level.WARNING, "Failed to remove " + f);
    return backups.length;
  }
  
  
  
  private StateMorselFiler stateMorselFiler() {
    return new StateMorselFiler(getIndexDir());
  }
  
  
  /**
   * 
   */
  public Optional<MorselFile> getStateMorsel() {
    return stateMorselFiler().getStateMorsel();
  }
  
  
  
  public boolean updateStateMorsel()
      throws IOException, OffsetConflictException, RowHashConflictException {
    var filer = stateMorselFiler();
    Optional<MorselFile> oldSm = filer.getStateMorsel();
    
    var fro = endStateRecorder.lastFro().orElseThrow(
        () -> new IllegalStateException("no '" + STATE_EXT + "' file found"));
    
    long hi = fro.rowNumber();
    BackedupFile target = new BackedupFile(filer.rnFile(hi));
    
    try (var logChannel = Opening.READ_ONLY.openChannel(log)) {
      Path path = hasher().getPath(1, hi, logChannel);
      
      if (oldSm.isPresent()) {
        var oldMorsel = oldSm.get().getMorselPack();
        if (oldMorsel.hi() == hi && oldMorsel.getRow(hi).equals(path.last()))
          return false;
      }
      
      target.moveToBackup();
      var builder = new MorselPackBuilder();
      builder.initPath(path, log.getName());
      
      MorselFile.createMorselFile(target.getFile(), builder);
      return true;
    
    } catch (OffsetConflictException | RowHashConflictException | IOException x) {
      if (x instanceof SldgException error)
        this.error = error;
      target.rollback();
      throw x;
    }
  }
  
  
  
  
  
  public Row getRow(long rn)
      throws IOException, NoSuchElementException {
    
    var row = stateMorselFiler().getRow(rn);
    if (row.isPresent())
      return row.get();

    try (var lc = Opening.READ_ONLY.openChannel(log)) {
      return hasher().getFullRow(rn, lc).row();
    }
  }
  
  
  
  public ByteBuffer getRowHash(long rn)
      throws IOException, NoSuchElementException {
    
    if (blockRecorder != null) {
      var bh = blockRecorder.getRowHash(rn);
      if (bh.isPresent())
        return bh.get();
    }
    
    var fm = stateMorselFiler().getRowHash(rn);
    if (fm.isPresent())
      return fm.get();
    
    try (var lc = Opening.READ_ONLY.openChannel(log)) {
      return hasher().getRowHash(rn, lc);
    }
  }
  
  
  
  public List<ByteBuffer> getRowHashes(List<Long> rns)
      throws IOException, NoSuchElementException {
    
    if (rns.isEmpty())
      return List.of();

//    if (rns.get(0) < 1)
//      throw new IllegalArgumentException("rns [0]: " + rns.get(0));
    
    var hashes = new ArrayList<ByteBuffer>(rns.size());
    long lastRn = 0;
    for (long rn : rns) {
      if (rn <= lastRn)
        throw new IllegalArgumentException(
            "out-of-bounds/sequence %d; predecessor %d"
            .formatted(rn, lastRn));
      lastRn = rn;
      hashes.add(getRowHash(rn));
    }
    return Collections.unmodifiableList(hashes);
  }
  
  
  
  public List<Row> getRows(List<Long> rns)
      throws IOException, NoSuchElementException {
    
    if (rns.isEmpty())
      return List.of();

    if (rns.get(0) < 1)
      throw new IllegalArgumentException("rns [0]: " + rns.get(0));

    
    Row[] rows = new Row[rns.size()];
    
    State state = State.EMPTY;
    
    var smFiler = stateMorselFiler();
    final boolean checkSm = smFiler.lastFile().isPresent();
    
    var hasher = hasher();
    
    try (var lc = Opening.READ_ONLY.openChannel(log)) {
      for (int index = 0; index < rows.length; ++index) {
        long rn = rns.get(index);
        if (rn <= state.rowNumber())
          throw new IllegalArgumentException(
              "out-of-sequence/illegal row number %d <= last %d (at index %d)"
              .formatted(rn, state.rowNumber(), index));
        if (checkSm) {
          var row = smFiler.getRow(rn);
          if (row.isPresent()) {
            rows[index] = row.get();
            continue;
          }
        }
        var fr = hasher.getFullRow(rn, lc);
        rows[index] = fr.row();
        state = fr.toState();
      }
      return Lists.asReadOnlyList(rows);
    }
  }
  
  
  public List<FullRow> getFullRows(List<Long> rns)
      throws IOException, NoSuchElementException {

    try (var lc = Opening.READ_ONLY.openChannel(log)) {
      return hasher().getFullRows(rns, lc);
    }
  }
  
  
  
  
  public Path getPath(long loRn, long hiRn)
      throws IOException, NoSuchElementException {

    var rows = getRows(SkipLedger.skipPathNumbers(loRn, hiRn));
    return new Path(rows);
  }
  
  
  
  @Override
  public List<Long> getTrailedRowNumbers() {
    return getWitRepo().map(WitRepo::getTrailedRowNumbers).orElse(List.of());
  }
  
  
  
  /**
   * Lists the row numbers that may have pending (not-yet-recorded) witness
   * records. These are just the row numbers of the end-state {@code .fstate}
   * files that are greater than the {@linkplain #lastWitnessedRowNumber()
   * last witnessed row number}.
   */
  public List<Long> pendingTrailedRows() {
    long lastTrailedRn = lastWitnessedRowNumber();
    return endStateRecorder.listFileRns(lastTrailedRn + 1);
  }
  

  
  /**
   * Submits the hash of the witness-able rows and returns the result.
   * A subset of these may have been saved.
   * 
   * @see #toStored(List)
   */
  public List<WitnessRecord> witness() throws IOException {
    var pendingRns = pendingTrailedRows();
    if (pendingRns.isEmpty())
      return List.of();

    var rowsToWitness = getRows(pendingRns);
    
    Client remote = new Client();
    
    for (var row : rowsToWitness) {
      remote.addHash(row.hash());
    }
    var cRecords = remote.getCrumRecords();
    var witRecords = Lists.zip(rowsToWitness, cRecords, WitnessRecord::new);
    
    var toStore = toStored(witRecords);
    if (!toStore.isEmpty()) {
      var witRepo = ensureWitRepo();
      for (var wRecord : toStore)
        witRepo.addTrail(wRecord);
    }
    return witRecords;
  }
  
  
  
  /**
   * Returns the savable records in the given list. If 2 or more witness records
   * share the same time, then the one with the higher row number wins (is saved)
   * and the others are discarded.
   * 
   * @param witRecords with ascending row numbers
   * @return possibly empty subset of the given list
   */
  public List<WitnessRecord> toStored(List<WitnessRecord> witRecords) {
    if (witRecords.isEmpty())
      return List.of();
    long lastUtc = Long.MAX_VALUE;
    long lastRn = lastUtc;
    final int size = witRecords.size();
    var toStore = new ArrayList<WitnessRecord>(size);
    for (int index = size; index-- > 0; ) {
      var record = witRecords.get(index);
      if (lastRn <= record.rowNum())
        throw new IllegalArgumentException(
            "out-of-sequence row number at index " + (index + 1) + ": " + witRecords);
      lastRn = record.rowNum();
      if (!record.isTrailed())
        continue;
      
      if (lastUtc > record.utc()) {
        toStore.add(record);
        lastUtc = record.utc();
      }
    }
    return Lists.reverse(toStore);
  }
  
  
  
  
  
  protected Optional<WitRepo> getWitRepo() {
    if (witRepo == null) {
      File dir = getIndexDir();
      if (!WitRepo.isPresent(dir))
        return Optional.empty();
      try {
        witRepo = new WitRepo(dir, Opening.READ_WRITE_IF_EXISTS);
      } catch (IOException iox) {
        throw new UncheckedIOException(iox);
      }
    }
    return Optional.of(witRepo);
  }
  
  protected WitRepo ensureWitRepo() {
    if (witRepo == null) {
      try {
        File dir = getIndexDir();
        // half-cooked states are not supported..
        // (i.e. no CREATE_ON_DEMAND)
        Opening mode = WitRepo.isPresent(dir) ?
            Opening.READ_WRITE_IF_EXISTS : Opening.CREATE;
        witRepo = new WitRepo(dir, mode);
      } catch (IOException iox) {
        throw new UncheckedIOException(iox);
      }
    }
    return witRepo;
  }

  @Override
  public int getTrailCount() {
    return getWitRepo().map(WitRepo::getTrailCount).orElse(0);
  }

  @Override
  public TrailedRow getTrailByIndex(int index) {
    return getWitRepo().map(repo -> repo.getTrailByIndex(index)).orElseThrow();
  }

  @Override
  public boolean addTrail(WitnessRecord trailedRecord) {
    throw new UnsupportedOperationException(
        "direct trail record addition not supported");
  }
  

}























