/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;

import io.crums.io.FileUtils;
import io.crums.io.Opening;
import io.crums.io.sef.Alf;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.Path;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SkipLedgerFile;
import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.logledger.Hasher.RowHashListener;
import io.crums.sldg.src.SourceRow;
import io.crums.util.TaskStack;

/**
 * 
 * Low-level utility class for generating the skipledger hash of a log file and compact
 * proofs of line entries. For the most part, this uses {@code .lgl} files to
 * store results, but is hopefully flexible enough for storage in more
 * centralized data stores.
 * 
 * <h2>Storage Directory</h2>
 * <p>
 * Results (log hash, no-of-rows, and other generated artifacts), are saved in
 * the {@linkplain #lglDir() lglDir} directory. It defaults to a hidden {@code .lgl}
 * sibling directory of the {@linkplain #getLogFile() log file}. Typically (when the
 * {@linkplain HashingRules hashing rules} use salt), a {@code .rules.lgl} is
 * also stored there.
 * </p><p>
 * Note, per the convention of the {@code java.io.File} class, a {@code null}
 * reference as the {@code lglDir} paramater in any of this class's constructors
 * is interpreted as the <em>current directory</em>.
 * </p>
 * 
 * <h2>Construction</h2>
 * <p>
 * Nothing is written on instantiation; the storage directory {@code lglDir}
 * must already exist. For setup, use one of the "init" pseudo-constructors.
 * </p>
 * <h2>API</h2>
 * <p>
 * Most of the tasks this class performs involve parsing the log file;
 * <em>non-</em>parse-related tasks involve reading previously recorded data
 * (from previous parsing runs). Since parsing a log file is (relatively) time
 * consuming, when multiple tasks are to be performed, it is advantageous to
 * "schedule" and execute them in the same parsing run.
 * </p>
 * <p>
 * To set <em>what tasks to perform</em> on a parse-run, invoke the
 * {@linkplain #job()} method. The returned {@linkplain Job Job} object allows
 * the user to specify a number of tasks, such as
 * </p>
 * <ul>
 * <li>Compute (and optionally record) the skipledger hash of the file.</li>
 * <li>Extract tokenized values of individual lines (specified by row no.).</li>
 * <li>Generate proofs that the contents of individual lines are used to derive
 * the skipledger hash of the log file.</li>
 * </ul>
 * <h3>Random (Indexed) Access</h3>
 * <p>
 * Random access (by row no.) to {@linkplain SourceRow source row}s in the log
 * file and proofs of their positions in the log (encoded in skipledger
 * {@linkplain Path path}s) is available after building (see {@linkplain
 * #buildSkipLedger()}.
 * </p><p>
 * When built, the instance's {@linkplain #isRandomAccess()} property returns
 * {@code true}. As hinted above, random access actually comes in two parts:
 * </p>
 * <ol>
 * <li>{@linkplain #loadSourceIndex()} provides indexed access to source rows by
 * row no. It also provides a raw or text view of the row (ledgered line).</li>
 * <li>{@linkplain #loadSkipLedger()} supports building {@linkplain Path path}s
 * connecting rows at arbitray row no.s.</li>
 * </ol>
 * @see #job()
 * @see #executeJob()
 * 
 */
public class LogLedger {
  
  
  
  /**
   * Initializes (writes) and returns a salted instance in the same directory
   * as the {@code logFile}.
   * 
   * @param logFile     not touched; log's simple filename governs other files
   * @return {@code initSalt(logFile, logFile.getParentFile(), grammar)}
   * @see #initSalt(File, File, Grammar)
   */
  public static LogLedger initSalt(File logFile, Grammar grammar) {
    return initSalt(logFile, Files.defaultLglDir(logFile), grammar);
  }
  
  
  /**
   * Initializes (writes) and returns a salted instance. The grammar (assumed
   * shared across multiple log files) is not saved.
   * 
   * @param logFile     not touched; log's simple filename governs other files
   * @param lglDir      directory log-ledger arificats are saved
   *                    (created on demand)
   * @param grammar     {@code null} means {@linkplain Grammar#DEFAULT}
   * 
   * @return a newly created instance
   * @see #initSalt(File, Grammar)
   * @see #init(File, File, boolean, String, String, boolean)
   */
  public static LogLedger initSalt(File logFile, File lglDir, Grammar grammar) {
    checkFilepaths(logFile, lglDir);
    byte[] salt = newRandomSalt();
    File rulesFile = new File(lglDir, Files.rulesFilename(logFile));
    var rules = HashingRules.initSalt(rulesFile, grammar, salt);
    clear(salt);
    return new LogLedger(logFile, lglDir, rules, false);
  }
  
  
  /**
   * Initializes (writes) and returns a salted instance with the given
   * "grammar".
   * 
   * @param logFile     not touched; log's simple filename governs other files
   * @param skipBlankLines      if {@code true} blank lines are skipped
   * @param tokenDelimiters     {@code null} or empty string means
   *                            whitespace delimited
   * @param commentPrefix       {@code null} or empty string means
   *                            no comments
   */
  public static LogLedger initSalted(
      File logFile,
      boolean skipBlankLines,
      String tokenDelimiters,
      String commentPrefix) {
    
    return init(
        logFile,
        Files.defaultLglDir(logFile),
        skipBlankLines,
        tokenDelimiters,
        commentPrefix,
        true);
  }
  
  
  
  /**
   * Initializes (writes) and returns an instance with the given "grammar".
   * 
   * @param logFile     not touched; log's simple filename governs other files
   * @param lglDir      directory log-ledger arificats are saved
   *                    (created on demand)
   * @param skipBlankLines      if {@code true} blank lines are skipped
   * @param tokenDelimiters     {@code null} or empty string means
   *                            whitespace delimited
   * @param commentPrefix       {@code null} or empty string means
   *                            no comments
   * @param salt        if {@code true}, then a 32-byte (secret) salt is
   *                    also generated
   *                    
   * @return
   */
  public static LogLedger init(
      File logFile,
      File lglDir,
      boolean skipBlankLines,
      String tokenDelimiters,
      String commentPrefix,
      boolean salt) {
    
    checkFilepaths(logFile, lglDir);
    byte[] seed = salt ? newRandomSalt() : null;
    var rules = 
        HashingRules.init(
            new File(lglDir, Files.rulesFilename(logFile)),
            skipBlankLines, tokenDelimiters, commentPrefix, seed);
    clear(seed);
    return new LogLedger(logFile, lglDir, rules, false);
  }
  
  
  private static void checkFilepaths(File logFile, File lglDir) {
    checkLogFile(logFile);
    if (lglDir != null)
      FileUtils.ensureDir(lglDir);
  }
  
  private static byte[] newRandomSalt() {
    byte[] salt = new byte[LglConstants.SEED_SIZE];
    new SecureRandom().nextBytes(salt);
    return salt;
  }
  
  private static void clear(byte[] array) {
    if (array == null)
      return;
    for (int index = array.length; index-- > 0; )
      array[index] = 0;
  }
  
  private static void checkLogFile(File logFile) {
    if (!logFile.isFile())
      throw new IllegalArgumentException(
          (logFile.exists() ? "no such file: " : "not a file: ") + logFile);
  }
  
  
  
  
  
  
  
  

  private volatile Job job = new Job();
  private final File logFile;
  private final File lglDir;
  private final HashingRules rules;
  
  
  private LogLedger(
      File logFile, File lglDir, HashingRules rules, boolean dummy) {
    this.logFile = logFile;
    this.lglDir = lglDir;
    this.rules = rules;
  }

  
  /**
   * Creates a new instance backed by an <em>existing</em>
   * {@code .lgl} directory that is a sibling of the log file,
   * with no grammar-override / setting.
   * 
   * @param logFile
   */
  public LogLedger(File logFile) {
    this(logFile, Files.defaultLglDir(logFile), null);
  }
  
  
  /**
   * Creates a new instance backed by an <em>existing</em>
   * {@code .lgl} directory that is a sibling of the log file.
   * 
   * @param logFile     the log file
   * @param grammar     if {@code null}, then loaded from the {@code lglDir} directory,
   *                    if any; o.w. defaults to {@linkplain Grammar#DEFAULT}
   */
  public LogLedger(File logFile, Grammar grammar) {
    this(logFile, Files.defaultLglDir(logFile), grammar);
  }
  
  
  /**
   * Full constructor. 
   * 
   * @param logFile     the log file
   * @param lglDir      directory where log-ledger files are written,
   *                    {@code null} means current directory.
   * @param grammar     if {@code null}, then loaded from the {@code lglDir} directory,
   *                    if any; o.w. defaults to {@linkplain Grammar#DEFAULT}
   *                    
   */
  public LogLedger(File logFile, File lglDir, Grammar grammar) {
    this.logFile = logFile;
    checkLogFile(logFile);
    
    this.lglDir = lglDir;
    if (lglDir != null && !lglDir.isDirectory())
      throw new IllegalArgumentException("not a directory: " + lglDir);
    
    File rulesFile = new File(lglDir, Files.rulesFilename(logFile));
    this.rules =
        rulesFile.exists() ? HashingRules.load(rulesFile, grammar) :
          new HashingRules(grammar, null);
    
  }
  
  
  /**
   * Returns {@code true} if salt is used to determine the row hash.
   */
  public final boolean isSalted() {
    return rules.saltScheme().hasSalt();
  }
  
  
  /** Returns the log file. */
  public final File getLogFile() {
    return logFile;
  }
  
  
  /**
   * Returns the directory where parse artifacts are stored.
   */
  public final File lglDir() {
    return lglDir == null ? new File("") : lglDir;
  }
  
  /**
   * Returns the hashing rules (salt and grammar) for the log file.
   * Loaded on instantiation, from the file system.
   */
  public final HashingRules rules() {
    return rules;
  }
  
  
  /**
   * Returns the row numbers of the saved checkpoints in ascending order.
   * 
   * @see #loadCheckpoint(long)
   */
  public List<Long> checkpointNos() {
    return Files.listCheckpointNos(lglDir, logFile);
  }
  
  
  /** Returns the last checkpoint, any. (Convenience method.) */
  public Optional<Checkpoint> lastCheckpoint() {
    var checkNos = checkpointNos();
    return checkNos.isEmpty() ?
        Optional.empty() :
          Optional.of(loadCheckpoint(checkNos.getLast()));
  }
  
  
  /**
   * Loads and returns the saved checkpoint with the specified row no.
   * 
   * @see #checkpointNos()
   */
  public Checkpoint loadCheckpoint(long rowNo) {
    if (rowNo <= 0L)
      throw new IllegalArgumentException("rowNo " + rowNo);
    return Files.loadCheckpoint(lglDir, rowNo, logFile);
  }
  
  
  /**
   * Loads and returns the nearest saved checkpoint <em>at or before</em>
   * the specified row no., if any.
   */
  public Optional<Checkpoint> nearestCheckpoint(long rowNo) {
    if (rowNo <= 1L)
      return Optional.empty();
    
    return nearestCheckpoint(checkpointNos(), rowNo);
  }
  
  
  private Optional<Checkpoint> nearestCheckpoint(
      List<Long> parseStateNos, long rowNo) {
    
    return nearestCheckpointNo(parseStateNos, rowNo).map(this::loadCheckpoint);
  }
  
  
  private Optional<Long> nearestCheckpointNo(
      List<Long> parseStateNos, long rowNo) {
    
    int index = Collections.binarySearch(parseStateNos, rowNo);
    if (index < 0) {
      // set to one less than the insert-index
      // i.e. to the index preceding the insert index
      index = -2 - index;
      if (index < 0)
        return Optional.empty();
    }
    return Optional.of(parseStateNos.get(index));
  }
  
  
  /**
   * Returns {@code true} if there's an offsets-index.
   * When present, certain operations (such as gathering
   * source rows) may be significantly faster.
   */
  public boolean hasSourceIndex() {
    return offsetsFile().exists();
  }
  
  
  /**
   * Loads and returns the log's source index, if any. If present,
   * the user must eventually {@linkplain SourceIndex#close() close}
   * the returned instance.
   * 
   * @see #hasSourceIndex()
   */
  public Optional<SourceIndex> loadSourceIndex() {
    File offsetsFile = offsetsFile();
    
    return
        offsetsFile.exists() ?
            Optional.of(SourceIndex.newInstance(logFile, offsetsFile, rules)) :
              Optional.empty();
  }
  
  /**
   * Returns {@code true} if there's a skipledger commitment chain.
   * When present, proofs of entry (commitment {@linkplain Path}s)
   * can constructed on demand (without parsing the log).
   */
  public boolean hasSkipledger() {
    return skipledgerFile().exists();
  }
  

  /**
   * Loads and returns the log's skipledger, if any. If present,
   * the user must eventually {@linkplain SkipLedger#close() close}
   * the returned instance.
   * 
   * @see #hasSkipledger()
   */
  public Optional<SkipLedger> loadSkipLedger() {
    File sldgFile = skipledgerFile();
    if (!sldgFile.exists())
      return Optional.empty();
    try {
      return Optional.of(
          new SkipLedgerFile(sldgFile, Opening.READ_ONLY));
    
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on loading skipledger-file %s for log-file %s: %s"
          .formatted(sldgFile, logFile, iox),
          iox);
    }
  }
  
  
  
  public long buildSkipLedger() {
    return buildSkipLedger(true, false);
  }
  
  public long buildSkipLedger(boolean indexSource, boolean overwrite) {
    
    try (TaskStack closer = new TaskStack()) {
      
      var ch = Opening.READ_ONLY.openChannel(logFile);
      closer.pushClose(ch);
      LogParser parser = new LogParser(rules.grammar(), ch);
      closer.pushClose(parser);
      
      File chainFile = skipledgerFile();
      if (chainFile.exists() && overwrite)
        FileUtils.delete(chainFile);
      
      var chainCh = Opening.CREATE_ON_DEMAND.openChannel(chainFile);
      closer.pushClose(chainCh);
      
      
      var chainWriter = new SkipLedgerWriter(chainCh);
      
      final long rowCount = chainWriter.rowCount();
      
      Optional<Alf> offsets = indexSource ?
          Optional.of(openAlf(closer, Opening.CREATE_ON_DEMAND)) :
            Optional.empty();
        
      final long firstRowToIndex = indexSource ?
          offsets.get().size() : Long.MAX_VALUE;
      
      final long minStartRow =
          Math.min(rowCount, firstRowToIndex);
      
      var checkpoint = nearestCheckpoint(minStartRow);
      initParser(checkpoint, parser, ch);

      parser.pushListener(
          Hasher.initInstance(rules, checkpoint)
            .setRowHashListeners(chainWriter) );
      
      if (offsets.isPresent()) {
        parser.pushListener(new SourceIndexer(offsets.get(), true));
      }
      
      parser.parse();
      
      return chainWriter.rowCount() - rowCount;
      
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
  private void initParser(
      Optional<Checkpoint> checkpoint, LogParser parser, FileChannel log)
          throws IOException {
    
    if (checkpoint.isEmpty())
      return;
    
    Checkpoint state = checkpoint.get();
    long pos = state.prevEol();
    // set position
    log.position(pos);
    parser.rowNo(state.rowNo() -1L).lineEndOffset(pos);
  }
  
  
  /**
   * Returns {@code true} if log row contents and their proofs are
   * available on demand (in sublinear time).
   * 
   * @return {@code hasSkipledger() && hasOffsetsIndex()}
   */
  public boolean isRandomAccess() {
    return hasSkipledger() && hasSourceIndex();
  }
  
  
  /**
   * Returns the greatest row number whose starting offset 
   * in the log file is indexed; zero, if there's no offsets
   * index.
   * 
   * @return &ge; 0
   * @see #hasSourceIndex()
   */
  public long maxRowInOffsetsIndex() {
    if (!hasSourceIndex())
      return 0L;
    try (var closer = new TaskStack()) {
      return openAlf(closer, Opening.READ_ONLY).size();
    }
  }
  
  
  
  /**
   * Returns the current (parse) job. The returned object encapsulates
   * both what to do, and the result.
   * 
   * @see #executeJob()
   * @see #newJob()
   */
  public Job job() {
    return job;
  }
  
  
  /**
   * Resets (replaces) the current job with a new, empty instance and returns
   * it.
   * <h4>Thread-safety</h4>
   * <p>
   * If the current job is {@linkplain #executeJob() executing} (in another
   * thread), it's execution continues. It's alway okay to invoke this method
   * while the current job is running.
   * </p>
   */
  public Job newJob() {
    return this.job = new Job();
  }
  
  
  /**
   * Executes the instance's current {@linkplain #job()} and returns
   * the result. The returned result is also available via
   * {@code job().result()}.
   * 
   * @throws IllegalStateException if the job is empty or alreay executed
   * @see #job()
   * @see Job#result()
   */
  public JobResult executeJob() throws IllegalStateException {
    Job job = job();
    synchronized (job) {
      return executeJob(job);
    }
  }
  
  
  private Alf openAlf(TaskStack closer, Opening mode) {
    try {
      
      FileChannel offIdxFile =
          Files.openVersioned(offsetsFile(), mode);
      closer.pushClose(offIdxFile);
      
      Alf alf = new Alf(offIdxFile, Files.HEADER_LENGTH);
      closer.pushClose(alf);
      return alf;
      
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
  private long rowOffset(Alf alf, long rowNo) {
    try {
      return alf.get(rowNo - 1L);
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
  /**
   * Executes the given {@code job} and returns the result (also
   * stored in the job itself). Invoked while synchronized on the
   * argument.
   * <p>
   * The state of the instance (this {@code LogLeder}) may only ever change
   * indirectly: thru write operations to the file system. Therefore,
   * it is safe to execute multiple <em>read-only</em> jobs concurrently.
   * Future versions may guard against executing concurrent jobs with writes:
   * for now, it seems not worth the effort.
   * </p>
   */
  protected JobResult executeJob(Job job) {
    
    
    if (job.isEmpty())
      throw new IllegalStateException("attempt to execute empty job");
    
    if (job.result != null)
      throw new IllegalStateException("job already executed");
    
    job.result = Optional.empty();
    
    try (TaskStack closer = new TaskStack()) {
      
      var ch = Opening.READ_ONLY.openChannel(logFile);
      closer.pushClose(ch);
      LogParser parser = new LogParser(rules.grammar(), ch);
      closer.pushClose(parser);
      
      final long startNo = job.startRow();

      
      final var checkpointNos = checkpointNos();

      Optional<Alf> offIdx;
      if (job.indexOffsets()) {
        offIdx = Optional.of(openAlf(closer, Opening.CREATE_ON_DEMAND));
      } else if (job.useOffsetsIndex() && offsetsFile().exists()) {
        offIdx = Optional.of(openAlf(closer, Opening.READ_ONLY));
      } else
        offIdx = Optional.empty();
      
      final long lastIndexedRn =  offIdx.map(Alf::size).orElse(0L);
      
      // 
      final long firstRnToIndex =
          job.indexOffsets() ?
              lastIndexedRn + 1L :
                Long.MAX_VALUE;
      
      final long minParserStartNo = Math.min(startNo, firstRnToIndex);
      
      // true, if minParseStartNo is indexed AND > minRowHashed
      final boolean initWithIndex =
          lastIndexedRn >= minParserStartNo &&
          job.minRowHashed() > minParserStartNo;
      
      
      final Optional<Checkpoint> checkpoint;
      
      if (job.computeHash()) {
        
        checkpoint =
            nearestCheckpoint(
                checkpointNos,
                initWithIndex ? job.minRowHashed() : minParserStartNo);
        
      // else not computing hash..
      } else {
        
        checkpoint = initWithIndex ? Optional.empty() :
            nearestCheckpoint(checkpointNos, minParserStartNo);
      }
      
      final long checkpointRn =
          checkpoint.map(Checkpoint::rowNo).orElse(0L);
      
      // set parser row no. and lineEndOffset; set log stream position
      if (checkpointRn > minParserStartNo) {
        
        long pos =
            offIdx.map(alf -> rowOffset(alf, minParserStartNo))
            .orElse(0L);
        
        parser.rowNo(minParserStartNo - 1L).lineEndOffset(pos);
        ch.position(pos);
        
      } else
        
        initParser(checkpoint, parser, ch);
      
      final Optional<SourceRowGatherer> srcGatherer;
      
      if (job.sourcesRequested()) {
        var gatherer = rules.saltScheme().hasSalt() ?
            new SourceRowGatherer(
                job.srcNos::contains,
                rules.salter().get()) :
              new SourceRowGatherer(job.srcNos::contains);
        parser.pushListener(gatherer);
        srcGatherer = Optional.of(gatherer);
      } else
        srcGatherer = Optional.empty();
      
      
      
      final Optional<Hasher> hasher;
      final Optional<PathGatherer> pathGatherer;
      
      if (job.computeHash()) {
        
        Hasher h = checkpoint.isPresent() ?
            new Hasher(rules, checkpoint.get()) :
              new Hasher(rules);
        
        parser.pushListener(h);
        hasher = Optional.of(h);
        
        Optional<RowHashListener> checkpointVerifier =
            checkPointVerifier(job, checkpointNos);
        
        
        if (job.pathRequested()) {
          
          var pg = new PathGatherer(job.pathNos);
          if (checkpointVerifier.isPresent())
            h.setRowHashListeners(checkpointVerifier.get(), pg);
          else
            h.setRowHashListeners(pg);
          
          pathGatherer = Optional.of(pg);
          
        } else {
          pathGatherer = Optional.empty();
          checkpointVerifier.ifPresent(h::setRowHashListeners);
        }
      
      } else {
        hasher = Optional.empty();
        pathGatherer = Optional.empty();
      }
      
      final Optional<SourceIndexer> offsetIndexer;
      if (job.indexOffsets()) {
        var indexer = new SourceIndexer(offIdx.get(), job.verifyOffsetsIndex());
        parser.pushListener(indexer);
        offsetIndexer = Optional.of(indexer);
      } else {
        offsetIndexer = Optional.empty();
      }
      
      long stopRow = job.stopRow();
      if (stopRow != 0L)
        parser.maxRowNo(stopRow);
      
      parser.parse();
      
      Optional<Checkpoint> parseState = hasher.map(Hasher::parseState);
      Optional<Hasher.Stats> hasherStats = hasher.map(Hasher::getStats);
      Optional<Path> path = pathGatherer.map(PathGatherer::getPath);
      List<SourceRow> sources =
          srcGatherer.map(SourceRowGatherer::gatheredRows).orElse(List.of());
      
      if (job.saveParseState())
        saveCheckpoint(parseState.get(), job.overwriteCheckpoints());
      
      offsetIndexer.ifPresent(SourceIndexer::commit);
      
      job.result = Optional.of(
          new JobResult(parseState, hasherStats, path, sources));
      
      return job.result.get();
    
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on executing job %s on log %s: %s"
          .formatted(job, logFile, iox.getMessage()),
          iox);
    }
  }
  
  
  
  
  
  private File offsetsFile() {
    return new File(lglDir, Files.offsetsFilename(logFile));
  }
  
  private File skipledgerFile() {
    return new File(lglDir, Files.skipledgerFilename(logFile));
  }
  
  
  
  private Optional<RowHashListener> checkPointVerifier(
      Job job, List<Long> savedParseStateNos) {
    
    if (!job.validateCheckpoints())
      return Optional.empty();

    List<Long> nosToCheck =
        savedParseStateNos.stream()
        .filter(n -> n > job.minRowHashed() && n <= job.maxRowHashed())
        .toList();
    
    return
        nosToCheck.isEmpty() ?
            Optional.empty() :
              Optional.of(checkPointVerifier(nosToCheck));
  }
  
  private RowHashListener checkPointVerifier(List<Long> parseStateNos) {
    
    final HashSet<Long> psNos = new HashSet<>(parseStateNos);
    return new RowHashListener() {
      @Override
      public void rowHashParsed(
          ByteBuffer inputHash,
          HashFrontier frontier, HashFrontier prevFrontier) {

        final long rowNo = frontier.rowNo();
        if (!psNos.contains(rowNo))
          return;
        
        Checkpoint checkpoint = loadCheckpoint(rowNo);
        if (!checkpoint.frontier().equals(frontier)) {
          var msg = checkpoint.inputHash().equals(inputHash) ?
              "log file changed or corrupted before row [" + rowNo + "] (line-count appears unchanged)" :
              "log file changed or corrupted on row [" + rowNo + "] or before";
          throw new HashConflictException(msg);
        }
      }
    };
  }
  
  
//  private boolean saveParseState(ParseState state) {
//    return saveParseState(state, false);
//  }
  
  private boolean saveCheckpoint(Checkpoint state, boolean force) {
    File file = new File(lglDir, Files.checkpointFilename(logFile, state));
    final boolean exists = file.exists();
    if (exists && verifyCheckpointFile(file, state, !force))
      return false;
    
    if (exists) {
      System.getLogger(LglConstants.LOG_NAME).log(
          Level.WARNING,
          "Overwriting (force=true) conflicting parse-state " + file);
      FileUtils.delete(file);
    }
    
    Files.writeToFile(state, file);
    return true;
  }
  
  
  private boolean verifyCheckpointFile(File file, Checkpoint state, boolean fail) {
    Checkpoint savedState = Files.loadCheckpoint(file);
    if (savedState.equals(state))
      return true;
    if (fail)
      throw new HashConflictException(
          "parse-state %s conflicts with recorded state %s for log file %s"
          .formatted(state, savedState, logFile));
    return false;
  }
  
  
  
  
  
  
  
  
  
  
  


  /**
   * Result returned on {@linkplain LogLedger#executeJob() executing} a
   * parse {@linkplain Job job}.
   * 
   * @param parseState  the state at the <em>end</em> of the job
   * @param path        the requested (commitment) path (not compressed)
   * @param sources     the requested source rows
   */
  public record JobResult(
      Optional<Checkpoint> parseState,
      Optional<Hasher.Stats> hasherStats,
      Optional<Path> path,
      List<SourceRow> sources) {
    
    public JobResult {
      Objects.requireNonNull(parseState, "parseState");
      Objects.requireNonNull(hasherStats, "hasherStats");
      Objects.requireNonNull(path, "path");
      Objects.requireNonNull(sources, "sources");
    }
    
    /**
     * Returns {@code true}, if the result has no data. 
     */
    public boolean isEmpty() {
      return
          parseState.isEmpty() &&
          path.isEmpty() &&
          sources.isEmpty();
    }
  }
  
  
  /**
   * A collection of tasks executed in a single parse of the log file and
   * their results.
   * 
   * <h2>API</h2>
   * <p>
   * This is a low-level API.
   * The convention for single value properties is that they're accessed by name
   * (for example {@linkplain #computeHash()} with setters bearing the same
   * name taking the property argument and returning the instance (for example
   * {@linkplain #computeHash(boolean)}. Properties that denote a bag of values
   * are built using a corresponding "add" method (for example,
   * {@linkplain #addSourceRow(Long)} and {@linkplain #sourceNos()}).
   * </p>
   * <h3>Workflow</h3>
   * <p>
   * Instances may only be {@linkplain LogLedger#executeJob() executed} once.
   * A lock is held on execution (via the object's monitor in a {@code synchronized}
   * block), guaranteeing the request properties are not modified while parsing
   * is underway.
   * </p>
   * <h4>Dividing Work Across Jobs</h4>
   * <p>
   * Since jobs are I/O-bound and the tasks they perform are sequential, it pays
   * to group tasks that at neighboring row no.s together. Jobs with no
   * side-effects (i.e. when nothing is written to the file system) may be
   * processed concurrently.
   * </p>
   * <h3>Future Design Options</h3>
   * <p>
   * It's tempting to introduce an {@code execute()} method in this class itself.
   * It would be appropriate, if executing multiple {@code Job} instances were
   * thread-safe. For read-only operations (operations not involving writing 
   * computed results to the file system), {@code Job} instances are indeed
   * thread-safe; the challenge is with concurrent write ops (mostly worked out,
   * but punting for now).
   * </p>
   * 
   * @see JobResult
   * @see LogLedger#job()
   * @see LogLedger#executeJob()
   */
  public static class Job {
    
    private record HashRange(
        long startNo, long endNo,
        boolean saveParseState, boolean overwrite,
        boolean verifyCheckpoints) {
      
      HashRange {
        if (startNo <= 0L || startNo == Long.MAX_VALUE)
          throw new IllegalArgumentException("startRowNo out-of-bounds: " + startNo);
        if (endNo < startNo)
          throw new IllegalArgumentException(
              "endRowNo %d < startRowNo %d".formatted(startNo, endNo));
        if (verifyCheckpoints && overwrite)
          throw new IllegalArgumentException(
              "validate and overwrite cannot both be true");
      }
      
      HashRange() {
        this(1L, Long.MAX_VALUE, false, false, true);
      }
      
      
      HashRange(long startNo) {
        this(startNo, startNo, false, false, true);
      }
      
      
      // - - - Mutator methods - - -
      
      HashRange saveParseState(boolean save) {
        return save == saveParseState ?
            this :
              new HashRange(
                  startNo, endNo, save, overwrite, verifyCheckpoints);
      }
      HashRange overwrite(boolean force) {
        return force == overwrite ?
            this :
              new HashRange(
                  startNo, endNo, saveParseState,
                  force,
                  force ? false : verifyCheckpoints);
      }
      HashRange verifyCheckpoints(boolean validate) {
        if (validate == verifyCheckpoints)
          return this;
        
        return 
            new HashRange(
                startNo, endNo, saveParseState,
                validate ? false : overwrite,
                validate);
      }
      
      HashRange startNo(long rowNo) {
        return rowNo == startNo ?
            this :
              new HashRange(
                  rowNo, endNo, saveParseState, overwrite, verifyCheckpoints);
      }
      HashRange endNo(long rowNo) {
        return rowNo == endNo ?
            this :
              new HashRange(
                  startNo, rowNo,
                  saveParseState, overwrite, verifyCheckpoints);
      }
      
      
      HashRange ensureInRange(long rowNo) {
        if (rowNo < startNo)
          return startNo(rowNo);
        else if (rowNo > endNo)
          return endNo(rowNo);
        else
          return this;
      }
    }
    
    
    
    
    private final TreeSet<Long> srcNos = new TreeSet<>();
    
    private final TreeSet<Long> pathNos = new TreeSet<>();
    
    private HashRange hashRange;
    
    private HashRange getOrNewRange() {
      return hashRange == null ? new HashRange() : hashRange;
    }
    
    private HashRange ensureInRange(long rowNo) {
      return hashRange == null ?
          new HashRange(rowNo) :
            hashRange.ensureInRange(rowNo);
    }
    
    private boolean indexOffsets;
    private boolean useOffsetsIndex = true;
    private boolean verifyOffsetsIndex;
    
    
    private Optional<JobResult> result;
    
    
    private Job() {  }
    
    /** (checked while synchronized) */
    private void checkModify() {
      if (result != null) {
        throw new IllegalStateException(
            result.isEmpty() ?
                // assuming not invoked from the parsing thread,
                "attempt to modify failed job" :
                "attempt to modify already processed job");
      }
    }
    
    /**
     * Returns the result, if executed.
     * 
     * @see LogLedger#executeJob()
     */
    public synchronized Optional<JobResult> result() {
      return result == null ? Optional.empty() : result;
    }
    
    
    public boolean indexOffsets() { return indexOffsets; }
    public synchronized Job indexOffsets(boolean index) {
      checkModify();
      this.indexOffsets = index;
      return this;
    }
    
    /**
     * Returns {@code true} if row the offsets-index may be used
     * (if there's one available).
     */
    public boolean useOffsetsIndex() { return useOffsetsIndex; }
    public synchronized Job useOffsetsIndex(boolean use) {
      checkModify();
      this.useOffsetsIndex = use;
      return this;
    }
    
    /**
     * Returns {@code true} if row the offsets-index may be used
     * (if there's one available).
     */
    public boolean verifyOffsetsIndex() { return verifyOffsetsIndex; }
    public synchronized Job verifyOffsetsIndex(boolean verify) {
      checkModify();
      this.verifyOffsetsIndex = verify;
      return this;
    }
    
    
    
    
    /**
     * Determines whether row [commitment] hashes are computed.
     * (If only source rows are to be retrieved, then commitment hashes
     * will be unnecessary.)
     * 
     * @see #computeHash(boolean)
     */
    public boolean computeHash() { return hashRange != null; }
    public synchronized Job computeHash(boolean hashRows) {
      checkModify();
      if (hashRows)
        hashRange = getOrNewRange();
      else
        hashRange = null;
      return this;
    }
    
    /**
     * Returns the first row number whose hash <em>must</em> be
     * computed, or {@linkplain Long#MAX_VALUE}, if no row hashes
     * are computed.
     * 
     * @see #minRowHashed(long)
     * @see #maxRowHashed()
     */
    public long minRowHashed() {
      return hashRange == null ? Long.MAX_VALUE : hashRange.startNo();
    }

    /**
     * Sets the property and returns {@code this}.
     * @see #minRowHashed()
     */
    public synchronized Job minRowHashed(long rowNo) {
      checkModify();
      hashRange = getOrNewRange().startNo(rowNo);
      return this;
    }
    
    
    /**
     * Returns the maximum row number hashed, or {@code 0L} (zero), if no
     * hashes are computed. If the log has more rows than this number,
     * the row-hash computation stops at this row no.
     * 
     * @see #maxRowHashed(long)
     * @see #maxRowHashed()
     */
    public long maxRowHashed() {
      return hashRange == null ? 0L : hashRange.endNo();
    }
    /**
     * Sets the property and returns {@code this}.
     * @see #maxRowHashed()
     */
    public synchronized Job maxRowHashed(long rowNo) {
      checkModify();
      hashRange = getOrNewRange().endNo(rowNo);
      return this;
    }
    
    
    
    /**
     * If {@code true}, then row (line) hashes are computed
     * and the parse state is saved as a checkpoint on the conclusion
     * of parsing. Checkpoints (saved {@linkplain Checkpoint}s) can speed
     * later operations.
     * 
     * @see JobResult#parseState()
     * @see #saveParseState(boolean)
     */
    public boolean saveParseState() {
      return hashRange != null && hashRange.saveParseState();
    }
    /**
     * Sets the property and returns {@code this}.
     * @see #saveParseState()
     */
    public synchronized Job saveParseState(boolean save) {
      checkModify();
      hashRange = getOrNewRange().saveParseState(save);
      return this;
    }
    
    
    
    
    /**
     * Returns {@code true} if saved checkpoints are validated. When set,
     * only the checkpoints encountered along the rows (lines) hashed are
     * checked.
     * 
     * By default, if row-hashes are computed ({@linkplain #computeHash()}
     * returns {@code true}), then this method also returns {@code true}.
     * 
     * @see #validateCheckpoints(boolean)
     * @see #minRowHashed()
     * @see #maxRowHashed()
     */
    public boolean validateCheckpoints() {
      return hashRange != null && hashRange.verifyCheckpoints();
    }

    /**
     * Sets the property and returns {@code this}.
     * @see #overwriteCheckpoints()
     */
    public synchronized Job validateCheckpoints(boolean validate) {
      checkModify();
      if (hashRange == null && !validate)
        return this;
      
      hashRange = getOrNewRange().verifyCheckpoints(validate);
      return this;
    }
    
    /**
     * Returns {@code true} if conflicting checkpoints (saved parse-states) are
     * overwritten.
     */
    public boolean overwriteCheckpoints() {
      return hashRange != null && hashRange.overwrite();
    }
    
    /**
     * Sets the property and returns {@code this}.
     * @see #overwriteCheckpoints()
     */
    public synchronized Job overwriteCheckpoints(boolean overwrite) {
      checkModify();
      if (!overwrite && hashRange == null)
        return this;
      
      hashRange = getOrNewRange().overwrite(overwrite);
      return this;
    }
    
    
    
    
    
    
    /**
     * Adds the given source row no. to be picked up in the parse. Evidence of
     * the contents will be included in the commitment {@linkplain Path path}.
     * 
     * @return {@code addSourceRow(rowNo, true)}
     * @see #addSourceRow(Long, boolean)
     */
    public synchronized boolean addSourceRow(Long rowNo) {
      return addSourceRow(rowNo, true);
    }
    /**
     * Adds the given source row no. to be picked up in the parse.
     * 
     * @param rowNo     &ge; 1
     * @param withPath  if {@code true}, then the row no. will be included
     *                  in the (commitment) {@linkplain Path path}
     *                  
     * @return          {@code false}, iff {@code rowNo} is already added
     */
    public synchronized boolean addSourceRow(Long rowNo, boolean withPath) {
      checkModify();
      checkRowNo(rowNo);
      boolean pathAdded = withPath && addToPath(rowNo);
      return srcNos.add(rowNo) || pathAdded;
    }
    
    /** Returns the source row numbers requested in ascending order. */
    public List<Long> sourceNos() { return rowNos(srcNos);  }
    /**
     * Tests whether source rows are requested.
     * @return {@code !sourceNos().isEmpty()}
     */
    public boolean sourcesRequested() { return !srcNos.isEmpty(); }
    
    
    
    
    
    
    
    /**
     * Adds the given row number to the [commitment] {@linkplain Path path}
     * requested.
     * 
     * @param rowNo     &ge; 1
     * 
     * @return          {@code false}, iff {@code rowNo} is already added
     */
    public synchronized boolean addToPath(Long rowNo) {
      checkModify();
      checkRowNo(rowNo);
      hashRange = ensureInRange(rowNo);
      return pathNos.add(rowNo);
    }
    /**
     * Returns the added path row numbers in ascending order. The acual
     * gathered {@linkplain Path} will usually contain more rows.
     */
    public List<Long> pathStitchNos() { return rowNos(pathNos);  }
    
    /**
     * Test whether a (commitment) {@linkplain Path} is requested.
     * @return {@code !pathStitchNos().isEmpty()}
     */
    public boolean pathRequested() { return !pathNos.isEmpty(); }
    
    
    
    
    /**
     * Returns the first row no. to parse, or {@linkplain Long#MAX_VALUE}, if 
     * there's no work to do (if {@linkplain #isEmpty()} returns {@code true}).
     * <p>
     * Note, unless a checkpoint at this row no. already exists, the actual
     * parsing starts at a lower row no., or zero, if there are no saved
     * checkpoints before this row no.
     * </p><p>
     * Careful: the number returned plus one may be negative.
     * </p>
     * 
     * @return &ge; 1
     */
    public long startRow() {
      return
          Math.min(
            minRowHashed(),
            firstNo(srcNos));
    }
    
    
    /**
     * Returns the last row to parse. If zero, then there's nothing to do.
     * 
     * @return in the range {@code [0, Long.MAX_VALUE]}, inclusive;
     *         careful, extremal values are common.
     */
    public long stopRow() {
      return
          Math.max(
              maxRowHashed(),
              lastNo(srcNos));
    }
    
    
    /**
     * Tests whether there's work.
     * 
     * @return {@code !isEmpty()}
     */
    public boolean hasWork() {
      return !isEmpty();
    }
    
    
    /**
     * Tests whether <em>nothing</em> has been requested.
     * 
     * @see #hasWork()
     */
    public boolean isEmpty() {
      // note the pathNos.isEmpty() check is redundant
      // when hashRange is null: thrown in here to make
      // make independent of details and easier to
      // understand
      
      return
          hashRange == null &&
          !indexOffsets &&
          srcNos.isEmpty() && pathNos.isEmpty();
    }
    
    
    private List<Long> rowNos(TreeSet<Long> set) {
      return set.stream().toList();
    }
    /** @return {@code set.isEmpty() ? Long.MAX_VALUE : set.first()} */
    private long firstNo(TreeSet<Long> set) {
      return set.isEmpty() ? Long.MAX_VALUE : set.first();
    }
    /** @return {@code set.isEmpty() ? 0L : set.last()} */
    private long lastNo(TreeSet<Long> set) {
      return set.isEmpty() ? 0L : set.last();
    }
    private void checkRowNo(long rowNo) {
      if (rowNo <= 0L)
        throw new IllegalArgumentException("rowNo " + rowNo);
    }
    
    
  }
  
  
  
  

}





















