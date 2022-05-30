/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import io.crums.io.Serial;
import io.crums.model.CrumTrail;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.Path;
import io.crums.sldg.PathInfo;
import io.crums.sldg.Row;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.bags.MorselBag;
import io.crums.sldg.src.SourceInfo;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.time.TrailedRow;
import io.crums.util.Lists;
import io.crums.util.Sets;

/**
 * 
 */
public class MorselPackBuilder implements MorselBag, Serial {
  
  
  
  protected final RowPackBuilder rowPackBuilder = new RowPackBuilder();
  protected final TrailPackBuilder trailPackBuilder = new TrailPackBuilder();
  protected final SourcePackBuilder sourcePackBuilder = new SourcePackBuilder();
  protected final PathPackBuilder pathPackBuilder = new PathPackBuilder();
  
  private MetaPack metaPack = MetaPack.EMPTY;
  
  
  protected final Object lock() {
    return rowPackBuilder;
  }
  
  
  public int init(MorselPack pack) throws IllegalStateException {
    synchronized (lock()) {
      int rowsAdded = rowPackBuilder.init(pack.getRowPack());
      int trailsAdded = trailPackBuilder.addAll(pack.getTrailPack());
      int srcsAdded = sourcePackBuilder.addAll(pack.getSourcePack());
      int infosAdded = pathPackBuilder.addPathPack(pack.getPathPack());
      int metaAdded = addMetaPack(pack.getMetaPack());
      return rowsAdded + trailsAdded + srcsAdded + infosAdded + metaAdded;
    }
  }
  
  
  
  public int init(MorselPack pack, List<Long> rows, String comment) {
    if (!pack.getFullRowNumbers().containsAll(rows))
      throw new IllegalArgumentException(
          "morsel does not contain (full) information about rows " + rows);
    
    var pathInfo = new PathInfo(pack.lo(), rows, pack.hi(), comment);
    
    Path path = new Path(
        Lists.map(pathInfo.rowNumbers(), rn -> pack.getRow(rn)));
    
    
    synchronized (lock()) {
      int count = rowPackBuilder.init(path);
      pathPackBuilder.addDeclaredPath(pathInfo);
      return count + 1;
    }
  }
  

  
  
  public void initWithSources(MorselPack pack) {
    initWithSources(pack, List.of(), null);
  }
  
  
  public void initWithSources(MorselPack pack, List<Integer> redactCols, String comment) {
    initWithSources(pack, pack.sourceRowNumbers(), redactCols, comment);
  }
  
  
  
  /**
   * Initializes the builder with the specified source rows, optionally redacting.
   * If there are no source rows, then this constructs a morsel with the minimal
   * (skip) path (from {@code pack.hi()} to 1).
   * 
   * @param pack
   * @param rows
   * @param redactCols 1-based, sorted
   * @param comment nullable
   * 
   * @return the number of trails added (note-to-self: why?)
   */
  public int initWithSources(MorselPack pack, List<Long> rows, List<Integer> redactCols, String comment) {

    // validate --some deferred to init(pack,rows,comment) below
    if (!Lists.isSortedNoDups(rows))
      throw new IllegalArgumentException("rows must be sorted with no dups: " + rows);
    if (!Lists.isSortedNoDups(redactCols))
      throw new IllegalArgumentException("redact-columns must be sorted with no dups: " + redactCols);
    if (!redactCols.isEmpty() && redactCols.get(0) < 1)
      throw new IllegalArgumentException("redact-columns are 1-based (> 0): " + redactCols);
    
    synchronized (lock()) {

      int trailsAdded = 0;  // (return value)
      init(pack, rows, comment);
      
      if (rows.isEmpty())
        return 0;
      
      int trailIndex = pack.indexOfNearestTrail(rows.get(0));
      long nextTrailedRn =
          trailIndex == -1 ?
              Long.MAX_VALUE : pack.getTrailedRows().get(trailIndex).rowNumber();
      long prevRn = 0;
      long lastTrailRn = 0;
      for (long rn : rows) {
        var srcRow = pack.getSourceByRowNumber(rn);
        srcRow = srcRow.redactColumns(redactCols);
        addSourceRow(srcRow);
        if (nextTrailedRn <= rn) {
          ++trailsAdded;
          var trailedRows = pack.getTrailedRows();
          var trailedRow = trailedRows.get(trailIndex);
          if (nextTrailedRn < rn) {
            Path path = pack.getPath(prevRn, nextTrailedRn, pack.hi());
            addPath(path);
          }
          addTrail(trailedRow);
          nextTrailedRn =
              ++trailIndex == trailedRows.size() ?
                  Long.MAX_VALUE :
                    trailedRows.get(trailIndex).rowNumber();
          lastTrailRn = trailedRow.rowNumber();
        }
        prevRn = rn;
      }
      
      // if we don't have a trail for the last row,
      // and if one exists for a row after it, add it..
      if (trailIndex >= 0 && trailIndex < pack.getTrailedRows().size()
          && prevRn > lastTrailRn) {
        Path path = pack.getPath(prevRn, nextTrailedRn, pack.hi());
        addPath(path);
        var trail = pack.getTrailedRows().get(trailIndex);
        addTrail(trail);
        ++trailsAdded;
      }
      
      return trailsAdded;
    }
  }

  /**
   * Merges source rows from the given morsel {@code pack} with this instance
   * with no redactions and returns the result.
   * 
   * @see #mergeSources(MorselPack, List)
   */
  public MergeResult mergeSources(MorselPack pack) {
    return mergeSources(pack, List.of());
  }
  
  /**
   * Merges source rows from the given morsel {@code pack} with this instance and
   * returns the result. A merge will not always succeed, or may only partially
   * succeed. For this reason, the result of the merge is encapsulated in the
   * returned object. Note the return value does not reprsent hash conflicts:
   * those fail uncermoniously with a {@linkplain HashConflictException}.
   * <p>
   * PS. Historical note: this method was introduced so that merges are efficient
   * in space. The old (ATM current) way accumulated needless hash data a user
   * would typically not be interested in.
   * </p><p>
   * PPS. <em>merge</em> here really means <em>add</em>. It's use case is driven
   * by the CLI's merge command. I might rename things later.
   * </p>
   * 
   * @param pack
   * @param redactCols 1-based, ordered list of redacted columns
   * @return the merge result
   * 
   * @see MergeResult
   */
  public MergeResult mergeSources(MorselPack pack, List<Integer> redactCols) {
    Objects.requireNonNull(redactCols, "null redactCols");
    final var srcRns = pack.sourceRowNumbers();
    if (srcRns.isEmpty())
      return new MergeResult(hi(), pack);
    
    final int srcRowsBefore = sourceRowNumbers().size();
    final int trailsBefore = trailedRowNumbers().size();
    final long lastSrcRn = srcRns.get(srcRns.size() - 1);
    final long hi = hi();
    if (lastSrcRn > hi)
      throw new IllegalArgumentException(
          lastSrcRn + " (last source row #) > hi (" + hi + ")");
    
    final var trailedRns = pack.trailedRowNumbers(srcRns.get(0), srcRns.get(srcRns.size() - 1));
    
    
    
    final List<Long> pathRns = sourceEvidencePath(hi, srcRns, trailedRns);
    
    
    var failedRows = new ArrayList<Long>();
    for (int index = pathRns.size(); index-- > 0; ) {
      long pathRn = pathRns.get(index);
      if (!hasFullRow(pathRn) &&
          (!pack.hasFullRow(pathRn) || !addRow(pack.getRow(pathRn)))) {
        failedRows.add(pathRn);
      }
    }
    
    for (long srcRn : srcRns) {
      var srcRow = pack.getSourceByRowNumber(srcRn).redactColumns(redactCols);
      addSourceRow(srcRow);
    }
    
    for (long trailedrn : trailedRns)
      addTrail(trailedrn, pack.crumTrail(trailedrn));
    
    int sourcesAdded = sourceRowNumbers().size() - srcRowsBefore;
    int trailsAdded = trailedRowNumbers().size() - trailsBefore;
    return new MergeResult(hi, pack, Lists.reverse(failedRows), sourcesAdded, trailsAdded);
  }
  
  
  
  
  
  
  
  private static List<Long> sourceEvidencePath(long hi, List<Long> srcRns, List<Long> trailedRns) {
    List<Long> targetRns = new ArrayList<>();
    targetRns.add(1L);
    targetRns.addAll(srcRns);
    targetRns.addAll(trailedRns);
    targetRns.add(hi);
    targetRns = SkipLedger.stitchCollection(targetRns);
    return targetRns.subList(1, targetRns.size() - 1);
  }
  
  
  /**
   * The result of a source merge operation.
   * <p>
   * The lists returned by instance methods here are all in ascending order.
   * </p>
   * @see #success()
   */
  public static class MergeResult {
    
    private final long hi;
    private final MorselBag bag;
    
    private final List<Long> failedRns;
    private final int srcRowsAdded;
    private final int trailsAdded;
    
    /** Creates a nothing-done instance. */
    private MergeResult(long hi, MorselBag bag) {
      this(hi, bag, List.of(), 0, 0);
    }
    
    private MergeResult(long hi, MorselBag bag, List<Long> failedRns, int srcRowsAdded, int trailsAdded) {
      this.hi = hi;
      this.bag = bag;
      this.failedRns = failedRns.isEmpty() ? List.of() : failedRns;
      this.srcRowsAdded = srcRowsAdded;
      this.trailsAdded = trailsAdded;
    }
    
    
    /** Signal whether all rows we care about (source and trails) were merged. */
    public boolean success() {
      return failedRns.isEmpty() || failedSrcRns().isEmpty() && failedTrailRns().isEmpty();
    }
    
    
    /** Returns the number of source rows added. */
    public int srcRowsAdded() {
      return srcRowsAdded;
    }
    
    
    /** Returns the number of trails added. */
    public int trailsAdded() {
      return trailsAdded;
    }
    
    
    /**
     *  Determines whether the merge had no effect.
     *  @see #hadEffect()
     */
    public boolean nothingDone() {
      return srcRowsAdded + trailsAdded == 0;
    }
    
    
    /**
     * Determines whether the merge had <em>any</em> effect.
     * 
     * @return {@link #nothingDone() !nothingDone()}
     */
    public final boolean hadEffect() {
      return !nothingDone();
    }
    
    /** Returns the hi row number (of the authority morsel to be merged with). */
    public long hi() {
      return hi;
    }
    
    /**
     *  Returns the list of rows evidencing both the source rows and their trails.
     *  Includes neither {@linkplain #hi()} nor 1L.
     */
    public List<Long> getEvidencePath() {
      var srcRns = srcRns();
      var trailRns = trailRns(srcRns);
      return sourceEvidencePath(hi, srcRns, trailRns);
    }
    
    /** Returns the source row numbers that were to be merged in. */
    public List<Long> srcRns() {
      return bag.sourceRowNumbers();
    }

    /** Returns the trailed row numbers that were merge candidates from the bag. */
    public List<Long> trailRns() {
      return trailRns(srcRns());
    }
    
    private List<Long> trailRns(List<Long> srcRns) {
      return srcRns.isEmpty() ?
          List.of() :
            bag.trailedRowNumbers(srcRns.get(0), srcRns.get(srcRns.size() - 1));
    }
    
    /** Returns the source row numbers that didn't get merged. */
    public List<Long> failedSrcRns() {
      return filterFailed(srcRns());
    }
    
    private List<Long> filterFailed(List<Long> rowNums) {
      if (failedRns.isEmpty() || rowNums.isEmpty())
        return List.of();
      var rowNumSet = Sets.sortedSetView(rowNums);
      return failedRns.stream().filter(rowNumSet::contains).collect(Collectors.toList());
    }
    

    /** Returns the trailed row numbers that didn't get merged. */
    public List<Long> failedTrailRns() {
      return filterFailed(trailRns());
    }
    
    
    /**
     * Returns <em>all</em> the row numbers that didn't get merged.
     * These include linking rows in addition to source and trailed rows.
     */
    public List<Long> failedRns() {
      return failedRns.isEmpty() ? failedRns : Collections.unmodifiableList(failedRns);
    }
    
    /**
     * Return the bag from which information was merged in.
     */
    public MorselBag getBag() {
      return bag;
    }
  } // END MergeResult
  
  
  public int initPath(Path path, String comment) {
    if (Objects.requireNonNull(path, "null path").hiRowNumber() < 2)
      throw new IllegalArgumentException("path is single row number 1");
    
    boolean declare = comment != null && !comment.isEmpty();
    
    
    synchronized (lock()) {
      int count = rowPackBuilder.init(path);
      if (declare) {
        PathInfo info = new PathInfo(path.loRowNumber(), path.hiRowNumber(), comment);
        pathPackBuilder.addDeclaredPath(info);
        ++count;
      }
      return count;
    }
  }
  
  
  public void setMetaPack(SourceInfo sourceInfo) {
    this.metaPack = new MetaPack(sourceInfo);
  }
  
  public void setMetaPack(MetaPack meta) {
    this.metaPack = Objects.requireNonNull(meta, "null meta");
  }
  
  
  public MetaPack getMetaPack() {
    return metaPack;
  }
  
  
  /**
   * Adds <em>all</em> the information in the given morsel {@code pack}
   * to this instance. This is not terribly efficient (in space), if the sole purpose
   * of the morsel is to prove the age and contents of its sources.
   * (As the ledger accumulates rows, the hash of some older rows no longer figure
   * directly in such proofs.)
   * 
   * @return the number of objects added
   */
  public int addAll(MorselPack pack) {
    synchronized (lock()) {
      if (isEmpty())
        return init(pack);
      int count = 0;
      count += addRows(pack);
      count += addTrails(pack);
      count += addSourceRows(pack);
      count += addDeclaredPaths(pack);
      count += addMetaPack(pack.getMetaPack());
      return count;
    }
  }
  
  
  public int addMetaPack(MetaPack meta) {
    if (meta.isEmpty() || metaPack.isPresent())
      return 0;
    this.metaPack = meta;
    return 1;
  }
  
  
  
  
  
  
  public boolean isEmpty() {
    return rowPackBuilder.isEmpty();
  }
  
  /**
   * Adds the given row if it can be linked from the last row.
   * 
   * @return {@code rowPackBuilder.add(row)}
   * 
   * @see RowPackBuilder#add(Row)
   */
  public boolean addRow(Row row) throws HashConflictException {
    return rowPackBuilder.add(row);
  }
  


  /**
   * Adds all the rows in the given morsel pack. If this instance {@linkplain #isEmpty() is empty}, then all
   * the rows in the pack are added. Otherwise, only <em>linked information</em> is
   * added. I.e. if not empty, this method only allows adding detail to already known information.
   * 
   * @return the number of full rows added
   * 
   * @throws HashConflictException
   *         if a hash in {@code pack} conflicts with an existing hash
   */
  public int addRows(MorselPack pack) throws HashConflictException {
    return rowPackBuilder.addAll(pack.getRowPack());
  }
  

  /**
   * Adds the rows in the given {@code path} that can be linked from the highest
   * row in this instance.
   * 
   * @param path a path whose lowest row number is &le; {@linkplain #hi()}
   * 
   * @return the number of rows added (possibly zero)
   * 
   * @throws HashConflictException
   *         if {@code path} is from another ledger
   */
  public int addPath(Path path) throws IllegalArgumentException, HashConflictException {
    return rowPackBuilder.addPath(path);
  }
  
  

  /**
   * Adds a path from the state-row ({@linkplain #hi()}) to the row with the
   * given row number.
   * 
   * @param rowNumber &ge; 1 and &le; {@linkplain #hi()}
   * @param ledger the ledger (or a descendant of) that created this instance
   * 
   * @return the number of rows added (possibly zero)
   * 
   * @throws HashConflictException
   *         if this instance is not from the given {@code ledger}
   */
  public int addPathToTarget(long rowNumber, SkipLedger ledger) throws HashConflictException {
    long hi = Objects.requireNonNull(ledger, "null ledger").size();
    Path path = ledger.skipPath(rowNumber, hi);
    return addPath(path);
  }
  
  
  
  /**
   * Adds the given path declaration, but only if its full rows are already in this morsel.
   */
  public boolean addDeclaredPath(PathInfo decl) {
    synchronized (lock()) {
      if (!Sets.sortedSetView(getFullRowNumbers()).containsAll(decl.rowNumbers()))
        return false;
      
      return pathPackBuilder.addDeclaredPath(decl);
    }
  }
  
  
  /**
   * Adds the declared paths from the given morsel. Only those whose full row numbers
   * are already in this morsel are added.
   * 
   * @return the number of declarations added
   * 
   * @see #addDeclaredPath(PathInfo)
   */
  public int addDeclaredPaths(MorselPack pack) throws HashConflictException {
    List<PathInfo> declPaths = pack.declaredPaths();
    if (declPaths.isEmpty())
      return 0;
    
    
    synchronized (lock()) {
      int count = 0;
      for (var p : declPaths) {
        if (addDeclaredPath(p))
          ++count;
      }
      return count;
    }
  }
  
  

  /**
   * Adds the trail for the given row, but only if a full row at the given number exists
   * in this morsel.
   * 
   * @throws HashConflictException if the witnessed hash in the crumtrail conflicts
   *          with the hash of the given row number
   */
  public boolean addTrail(long rowNumber, CrumTrail trail) throws HashConflictException {
    synchronized (lock()) {
      if (!hasFullRow(rowNumber))
        return false; // note we *could do this with a referenced-only row also
                      // but it would complicate proving witness time for lower row numbers
                      // For now, we disallow it
      
      if (!trail.crum().hash().equals(rowHash(rowNumber)))
        throw new HashConflictException("attempt to add unrelated crumtrail for row " + rowNumber);
      
      if (!trail.verify())
        throw new HashConflictException(
            "attempt to add crumtrail with inconsistent hashes for row " + rowNumber);
      
      return trailPackBuilder.addTrail(rowNumber, trail);
    }
  }
  
  /**
   * Adds the trail for the given row, but only if a full row at the given number exists
   * in this morsel.
   * 
   * @throws HashConflictException if the witnessed hash in the crumtrail conflicts
   *          with the hash of the given row number
   */
  public boolean addTrail(TrailedRow trailedRow) throws  HashConflictException {
    return addTrail(trailedRow.rowNumber(), trailedRow.trail());
  }
  
  
  /**
   * Adds the trailed rows from the given morsel. Only those whose full row numbers
   * are already in this morsel are added.
   * 
   * @return the number of crumtrails added
   */
  public int addTrails(MorselPack pack) {
    List<Long> trailedRns = pack.trailedRowNumbers();
    if (trailedRns.isEmpty())
      return 0;
    synchronized (lock()) {
      int count = 0;
      for (long rn : trailedRns) {
        if (addTrail(rn, pack.crumTrail(rn)))
          ++count;
      }
      return count;
    }
  }
  
  
  /**
   * Adds the given source-row, but only if its full row already exists in this
   * morsel.
   */
  public boolean addSourceRow(SourceRow row) {
    synchronized (lock()) {
      long rn = row.rowNumber();
      if (!hasFullRow(rn))
        return false;
      
      if (!inputHash(rn).equals(row.rowHash()))
        throw new HashConflictException("at row " + rn);
      
      return sourcePackBuilder.addSourceRow(row);
    }
  }
  
  
  
  
  public int addSourceRows(MorselPack pack) {
    List<SourceRow> sources = pack.sources();
    if (sources.isEmpty())
      return 0;
    
    synchronized (lock()) {
      int count = 0;
      for (SourceRow src : sources) {
        if (addSourceRow(src))
          ++count;
      }
      return count;
    }
  }
  
  
  
  

  
  //  I N T E R F A C E    M E T H O D S
  
  @Override
  public long lo() {
    return rowPackBuilder.lo();
  }
  
  @Override
  public long hi() {
    return rowPackBuilder.hi();
  }
  
  @Override
  public boolean hasFullRow(long rowNumber) {
    return rowPackBuilder.hasFullRow(rowNumber);
  }
  

  @Override
  public ByteBuffer rowHash(long rowNumber) {
    return rowPackBuilder.rowHash(rowNumber);
  }

  @Override
  public ByteBuffer inputHash(long rowNumber) {
    return rowPackBuilder.inputHash(rowNumber);
  }
  
  @Override
  public Row getRow(long rowNumber) {
    return rowPackBuilder.getRow(rowNumber);
  }

  @Override
  public List<Long> getFullRowNumbers() {
    return rowPackBuilder.getFullRowNumbers();
  }

  @Override
  public List<Long> trailedRowNumbers() {
    return trailPackBuilder.trailedRowNumbers();
  }

  @Override
  public CrumTrail crumTrail(long rowNumber) {
    return trailPackBuilder.crumTrail(rowNumber);
  }

  @Override
  public List<SourceRow> sources() {
    return sourcePackBuilder.sources();
  }

  @Override
  public List<PathInfo> declaredPaths() {
    return pathPackBuilder.declaredPaths();
  }

  
  
  
  

  /**
   * @see MorselPack
   */
  @Override
  public int serialSize() {
    int headerBytes = 1 + 4 * MorselPack.VER_PACK_COUNT;
    return
        headerBytes +
        rowPackBuilder.serialSize() +
        trailPackBuilder.serialSize() +
        sourcePackBuilder.serialSize() +
        pathPackBuilder.serialSize() +
        metaPack.serialSize();
  }


  /**
   * @see MorselPack
   */
  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    out.put((byte) MorselPack.VER_PACK_COUNT).putInt(rowPackBuilder.serialSize())
    .putInt(trailPackBuilder.serialSize())
    .putInt(sourcePackBuilder.serialSize())
    .putInt(pathPackBuilder.serialSize())
    .putInt(metaPack.serialSize());
  
  rowPackBuilder.writeTo(out);
  trailPackBuilder.writeTo(out);
  sourcePackBuilder.writeTo(out);
  pathPackBuilder.writeTo(out);
  metaPack.writeTo(out);
  return out;
  }
  
}
