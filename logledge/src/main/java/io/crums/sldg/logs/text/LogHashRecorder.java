/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import static io.crums.sldg.logs.text.ContextedHasher.newLimitContext;
import static io.crums.sldg.logs.text.LogledgeConstants.FRONTIERS_FILE;
import static io.crums.sldg.logs.text.LogledgeConstants.FRONTIERS_MAGIC;
import static io.crums.sldg.logs.text.LogledgeConstants.LOGNAME;
import static io.crums.sldg.logs.text.LogledgeConstants.LINE_NOS_FILE;
import static io.crums.sldg.logs.text.LogledgeConstants.LINE_NOS_MAGIC;
import static io.crums.sldg.logs.text.LogledgeConstants.OFFSETS_FILE;
import static io.crums.sldg.logs.text.LogledgeConstants.OFFSETS_MAGIC;
import static io.crums.sldg.logs.text.LogledgeConstants.PREFIX;
import static io.crums.sldg.logs.text.LogledgeConstants.STATE_EXT;
import static io.crums.sldg.logs.text.LogledgeConstants.assertMagicHeader;
import static io.crums.sldg.logs.text.LogledgeConstants.magicHeaderLen;
import static io.crums.sldg.logs.text.LogledgeConstants.writeMagicHeader;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import io.crums.client.Client;
import io.crums.io.BackedupFile;
import io.crums.io.Opening;
import io.crums.io.channels.ChannelUtils;
import io.crums.io.sef.Alf;
import io.crums.sldg.MorselFile;
import io.crums.sldg.Path;
import io.crums.sldg.Row;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SldgException;
import io.crums.sldg.WitnessedRowRepo;
import io.crums.sldg.fs.WitRepo;
import io.crums.sldg.logs.text.ContextedHasher.Context;
import io.crums.sldg.packs.MorselPackBuilder;
import io.crums.sldg.time.TrailedRow;
import io.crums.sldg.time.WitnessRecord;
import io.crums.util.Lists;
import io.crums.util.TaskStack;

/**
 * Incremental state-recording hasher bound to a particular text-based log file.
 * <p>
 * Maintains convention-based tracking files associated with the target log file.
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
    return new File(log.getParentFile(), PREFIX + log.getName());
  }
  

  private final static int FRONTIERS_HDR_LEN = magicHeaderLen(FRONTIERS_MAGIC)
      + 1; // dex
  
  
  private final static int OFFSETS_HDR_LEN = magicHeaderLen(OFFSETS_MAGIC)
      + 1; // dex
  

  private final static int LINE_NOS_HDR_LEN = magicHeaderLen(LINE_NOS_MAGIC)
      + 1; // dex
  
  
  
  
//  // records are implicitly static, btw
  private record FrontiersInfo(int dex, HashingGrammar rules) {
    
    FrontiersInfo {
      checkDex(dex);
      Objects.requireNonNull(rules, "null hash grammar rules");
    }
    
    /** Returns a new configured hasher (salter, comment-rules, token delimiters). */
    StateHasher stateHasher() {
      return rules.stateHasher();
    }
    
    long zeroOffset() {
      return FRONTIERS_HDR_LEN + rules.serialSize();
    }
    
    
    boolean hasBlocks() {
      return dex != NO_DEX;
    }
    
    
    static FrontiersInfo load(FileChannel file) throws IOException {
      ByteBuffer work = ByteBuffer.allocate(2048);
      work.limit(
          (int) Math.min(work.capacity(), file.size()));
      
      
      
      ChannelUtils.readRemaining(file, 0, work).flip();
      
      assertMagicHeader(FRONTIERS_MAGIC, work);

      int dex = 0xff & work.get();
      
      var rules = HashingGrammar.load(work);
      
      return new FrontiersInfo(dex, rules);
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
      
      final FrontiersInfo info;

      try (var ch = Opening.READ_ONLY.openChannel(ff)) {
        info = FrontiersInfo.load(ch);
      }
      
      FrontiersInfo mod = info;
      if (dex.isPresent()) {
        int d = dex.get();
        if (d != mod.dex)
          mod = new FrontiersInfo(d, mod.rules);
      }
      
      if (cPrefix.isPresent()) {
        var prefix = cPrefix.get();
        if (!prefix.equals(mod.rules.grammar().cPrefix())) {
          var modGrammar = mod.rules.grammar().cPrefix(prefix);
          var modRules = new HashingGrammar(mod.rules.salt(), modGrammar);
          mod = new FrontiersInfo(mod.dex, modRules);
        }
      }
      
      if (mod.equals(info))
        return info;

      boolean dexChange = mod.dex() != info.dex();
      if (dexChange)
        new BackedupFile(eor).moveToBackup();
      
      replace(ff, mod);
      if (dexChange && mod.hasBlocks())
        try (var ch = Opening.CREATE.openChannel(eor)) {
          OffsetsInfo.create(ch, mod.dex());
        }
      
      return mod;
    }
    
    private static void replace(File file, FrontiersInfo mod) throws IOException {
      BackedupFile bf = new BackedupFile(file);
      bf.moveToBackup();
      var ch = Opening.CREATE.openChannel(file);
      try (ch) {
        write(ch, mod);
      } catch (IOException iox) {
        ch.close();
        bf.rollback();
        throw iox;
      }
    }
    
    
    static FrontiersInfo create(FileChannel file, HashingGrammar grammar, int dex) throws IOException {
      var info = new FrontiersInfo(dex, grammar);
      write(file, info);
      return info;
    }
    
    
    static void write(FileChannel file, FrontiersInfo info) throws IOException {

      ByteBuffer header = ByteBuffer.allocate(2048);
      writeHeader(header, FRONTIERS_MAGIC, info.dex());
      info.rules.writeTo(header).flip();
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
      
      assertMagicHeader(OFFSETS_MAGIC, work);
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
  
  
  
  
private record LineNosInfo(int dex) {
    
    LineNosInfo {
      checkDex(dex);
    }
    
    long zeroOffset() {
      return LINE_NOS_HDR_LEN;
    }
    
    
    static LineNosInfo load(FileChannel file) throws IOException {

      ByteBuffer work = ByteBuffer.allocate(LINE_NOS_HDR_LEN);
      
      ChannelUtils.readRemaining(file, 0, work).flip();
      
      assertMagicHeader(LINE_NOS_MAGIC, work);
      int dex = 0xff & work.get();
      return new LineNosInfo(dex);
    }
    
    
    static LineNosInfo create(FileChannel file, int dex) throws IOException {
      var info = new LineNosInfo(dex);
      var buffer = ByteBuffer.allocate(LINE_NOS_HDR_LEN);
      writeHeader(buffer, LINE_NOS_MAGIC, dex).flip();
      ChannelUtils.writeRemaining(file, 0, buffer);
      return info;
    }
  } // LineNosInfo
  
  

  
  
  public final static int NO_DEX = 63;
  
  private static void checkDex(int dex) {
    if (dex < 0 || dex > NO_DEX)
      throw new IllegalArgumentException("dex out-of-bounds: " + dex);
  }
  
  
  private static ByteBuffer writeHeader(ByteBuffer header, String magic, int dex) {
    writeMagicHeader(magic, header);
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
    this(log, new Grammar(delimiters, cPrefix), dex);
    
  }
  
  /**
   * Creates a <em>new</em> peristence instance for the given {@code log} file
   * using the specified settings. Though bound to the {@code log}, the initial
   * recorded state is empty.
   * 
   * @param log         text-based log / journal tracked
   * @param grammar     defines tokenizaton and comment-lines
   * @param dex         delta exponent. Row hashes and offsets are recorded for
   *                    row numbers that are a multiple of 2<sup>dex</sup>.
   */
  public LogHashRecorder(File log, Grammar grammar, int dex) throws IOException {
    this(log, new HashingGrammar(grammar), dex);
  }
  
  
  /**
   * Creates a <em>new</em> peristence instance for the given {@code log} file
   * using the specified settings. Though bound to the {@code log}, the initial
   * recorded state is empty.
   * 
   * @param log         text-based log / journal tracked
   * @param grammar     defines tokenizaton and comment-lines and secret salt
   * @param dex         delta exponent. Row hashes and offsets are recorded for
   *                    row numbers that are a multiple of 2<sup>dex</sup>.
   */
  public LogHashRecorder(File log, HashingGrammar grammar, int dex) throws IOException {
    this.log = log;
    if (!log.isFile())
      throw new IllegalArgumentException("not a file: " + log);
    checkDex(dex);
    Objects.requireNonNull(grammar, "null grammar");
    
    try (var onFail = new TaskStack()) {
      var ff = Opening.CREATE.openChannel(frontiersFile());
      onFail.pushClose(ff);
      
      this.config = FrontiersInfo.create(ff, grammar, dex);
      
      this.endStateRecorder = new EndStateRecorder(getIndexDir(), false);
      
      if (config.hasBlocks()) {

        var off = Opening.CREATE.openChannel(offsetsFile());
        onFail.pushClose(off);
        
        var lnos = Opening.CREATE.openChannel(lineNosFile());
        onFail.pushClose(lnos);
        
        var oInfo = OffsetsInfo.create(off, dex);
        var lnInfo = LineNosInfo.create(lnos, dex);
        var fTable = new FrontierTableFile(ff, config.zeroOffset());
        var rowOffsets = new Alf(off, oInfo.zeroOffset());
        var lineNos = new Alf(lnos, lnInfo.zeroOffset());
        
        this.blockRecorder = new BlockRecorder(fTable, rowOffsets, lineNos, dex);
      
      } else {
        
        ff.close();
        this.blockRecorder = null;
      }
      
      onFail.clear();
    } // try (
  }
  
  
  
  
  
  
  
  

  /**
   * Loads an <em>existing</em> saved instance from the file system for the
   * given {@code log} file. The location of the backing files are
   * convention based. The instance is in read/write mode.
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
        
        var lnFile = mode.openChannel(lineNosFile());
        onFail.pushClose(lnFile);
        
        var oInfo = OffsetsInfo.load(oFile);
        var lnInfo = LineNosInfo.load(lnFile);
        
        if (config.dex() != oInfo.dex())
          throw new IllegalArgumentException(
              "dex mismatch: %d (frontiers) vs. %d (offests)"
              .formatted(config.dex(), oInfo.dex()));
        
        var rowOffsets = new Alf(oFile, oInfo.zeroOffset());
        var lineNos = new Alf(lnFile, lnInfo.zeroOffset());
        
        this.blockRecorder = new BlockRecorder(fTable, rowOffsets, lineNos, config.dex);
        
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
    return config.rules().grammar().commentPrefix();
  }
  

  /** Returns any special token delimiters, if set. Defaults to ASCII whitespace chars. */
  public Optional<String> tokenDelimiters() {
    return config.rules().grammar().tokenDelimiters();
  }
  
  
  public Grammar grammar() {
    return config.rules().grammar();
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
    return PREFIX;
  }
  
  
  /**
   * EOL row offsets are recorded in this file. Does not exist in the
   */
  public File offsetsFile() {
    return new File(getIndexDir(), OFFSETS_FILE);
  }
  
  
  
  public File lineNosFile() {
    return new File(getIndexDir(), LINE_NOS_FILE);
    
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
  
  
  public State update(long maxRows) throws IOException, OffsetConflictException, RowHashConflictException {
    if (maxRows < 1L)
      throw new IllegalArgumentException("maxRows " + maxRows + " < 1");
    
    final long maxRn = maxRows + getState().map(State::rowNumber).orElse(0L);
    
    Context limiter = newLimitContext(maxRn);
    
    var hasher = hasher().appendContext(limiter);
    
    return updateImpl(hasher);
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
  

  
  
  
  public State savedStateAhead(State fallback, long rn) throws IOException {
    return hasher().savedStateAhead(fallback, rn);
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
    return updateStateMorsel(true);
  }
  
  
  public boolean updateStateMorsel(boolean includeTrail)
      throws IOException, OffsetConflictException, RowHashConflictException {
    
    var fro = endStateRecorder.lastFro().orElseThrow(
        () -> new IllegalStateException("no '" + STATE_EXT + "' file found"));
    
    long hi = fro.rowNumber();
    
    Path path;
    try (var logChannel = Opening.READ_ONLY.openChannel(log)) {
      path = hasher().getPath(1, hi, logChannel);
    }

    var filer = stateMorselFiler();
    Optional<MorselFile> oldSm = filer.getStateMorsel();
    
    final Optional<TrailedRow> trail;
    
    if (oldSm.isPresent()) {
      var oldMorsel = oldSm.get().getMorselPack();
      boolean sameState =
          oldMorsel.hi() == hi && oldMorsel.getRow(hi).equals(path.last());
      
      if (sameState) {
        if (!includeTrail || !oldMorsel.trailedRowNumbers().isEmpty())
          return false;
        trail = lastTrailedRow();
        if (trail.isEmpty())
          return false;
        
      } else
        trail = includeTrail ? lastTrailedRow() : Optional.empty();
    } else
      trail = includeTrail ? lastTrailedRow() : Optional.empty();
    
    
    var builder = new MorselPackBuilder();
    builder.initPath(path, log.getName());

    if (trail.isPresent()) {
      long trn = trail.get().rowNumber();
      if (trn == hi)
        builder.addTrail(trail.get());
      
      else if (trn > hi)  // sanity check state
        throw new IllegalStateException(
            "repo is in inconsistent state (trail repo ahead of skip leddger): hi row [%d] < last trail [%d]"
            .formatted(hi, trn));
    }
    
    BackedupFile target = new BackedupFile(filer.rnFile(hi));
    target.moveToBackup();
    
    try {
      MorselFile.createMorselFile(target.getFile(), builder);
      return true;
    
    } catch (RuntimeException | IOException x) {
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
    
    
    var smFiler = stateMorselFiler();
    final boolean checkSm = smFiler.lastFile().isPresent();
    
    var hasher = hasher();
    
    try (var lc = Opening.READ_ONLY.openChannel(log)) {
      
      State state = State.EMPTY;
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
        var fr = hasher.getFullRow(rn, state, lc);
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

    // this seems needlessly inefficient
    // (we really just need row hashes)..
    // but it isn't, and the list isn't that big besides
    
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

      System.out.println("last UTC: " + lastUtc);
      System.out.println("record UTC: " + record.utc());
      
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
  public boolean addTrail(WitnessRecord record) {
    
    var last = lastTrailedRow();
    if (last.isPresent()) {
      var lastTrailedRow = last.get();
      long lastTrn = lastTrailedRow.rowNumber();
      
      if (lastTrn == record.rowNum() &&
          !lastTrailedRow.hash().equals(record.row().hash())) {
        throw new RowHashConflictException(
            lastTrn,
            "attempt to add trail with conflicting hash for row [%d]"
            .formatted(lastTrn));
      }

      if (lastTrn >= record.rowNum())
        return false;
    }

    State state = getState().orElseThrow(
        () -> new IllegalArgumentException("no state file; no rows in ledger"));
    
    long trn = record.rowNum();
    
    if (trn > state.rowNumber())
      throw new IllegalArgumentException(
          "trailed row [%d] > last row [%d]"
          .formatted(trn, state.rowNumber()));
    
    try {
      
      if (!getRowHash(trn).equals(record.row().hash()))
          throw new RowHashConflictException(trn,
              "attempt to add trail with conflicting hash for row [%d]"
              .formatted(trn));
    
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
    
    return ensureWitRepo().addTrail(record);
  }
  
  
  
  

}























