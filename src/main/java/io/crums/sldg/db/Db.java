/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.db;


import static io.crums.sldg.SldgConstants.*;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;

import io.crums.client.Client;
import io.crums.client.ClientException;
import io.crums.client.repo.TrailRepo;
import io.crums.io.Opening;
import io.crums.io.store.table.Table;
import io.crums.model.CrumRecord;
import io.crums.model.CrumTrail;
import io.crums.model.TreeRef;
import io.crums.sldg.CamelPath;
import io.crums.sldg.Ledger;
import io.crums.sldg.Nugget;
import io.crums.sldg.Path;
import io.crums.sldg.Row;
import io.crums.sldg.SkipPath;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.SldgException;
import io.crums.sldg.TargetPath;
import io.crums.sldg.TrailedPath;
import io.crums.util.CachingList;
import io.crums.util.FilteredIndex;
import io.crums.util.Lists;
import io.crums.util.Sets;
import io.crums.util.TaskStack;
import io.crums.util.Tuple;

/**
 * A directory containing a {@linkplain Ledger skip ledger}, a {@linkplain TrailRepo crumtrail repo}
 * (establishing how old) and a table identifying which rows contain beacon hashes (establishing
 * how young). 
 */
public class Db implements Closeable {
  


  
  /**
   * Fixed width index table filename.
   * (Created under {@linkplain #getDir() dir}.)
   */
  public final static String BEACON_TBL = "bn_idx";
  
  private final static int BEACON_TBL_WIDTH = 2 * Long.BYTES;
  
  private final File dir;
  private final CompactFileLedger ledger;
  private final TrailRepo witnessRepo;
  
  private final Table beaconTable;
  
  
  


  /**
   * Creates a lazy-loading instance.
   * 
   * @param dir root directory
   * @param mode opening mode (non-null)
   */
  public Db(File dir, Opening mode) throws IOException {
    this(dir, mode, true);
  }
  
  

  /**
   * Full param constructor.
   * 
   * @param dir root directory
   * @param mode opening mode (non-null)
   * @param lazy if <tt>true</tt>, then the ledger rows are loaded lazily
   */
  public Db(File dir, Opening mode, boolean lazy) throws IOException {
    this.dir = Objects.requireNonNull(dir, "null directory");

    try (TaskStack onFail = new TaskStack(this)) {
      
      this.ledger = new CompactFileLedger(new File(dir, DB_LEDGER), mode, true);
      onFail.pushClose(ledger);
      
      this.witnessRepo = new TrailRepo(dir, mode);
      onFail.pushClose(witnessRepo);
      
      this.beaconTable = Table.newSansKeystoneInstance(
          mode.openChannel(new File(dir, BEACON_TBL)), BEACON_TBL_WIDTH);
      
      onFail.clear();
    }
    
    // TODO: do some sanity checks we loaded plausible files
    //       delaying cuz if we load garbage it's sure to break anyway
    //       (most any structure validates at constuction and is immutable)
  }
  




  @Override
  public void close() {
    try (TaskStack closer = new TaskStack(this)) {
      closer.pushClose(ledger).pushClose(witnessRepo).pushClose(beaconTable);
    }
  }
  
  
  public File getDir() {
    return dir;
  }
  
  
  public Ledger getLedger() {
    return ledger;
  }
  
  
  /**
   * Returns the number of rows in the ledger.
   */
  public long size() {
    return ledger.size();
  }
  
  
  public void truncate(long newSize) {
    ledger.trimSize(newSize);
    
    try {
      int searchIndex = Collections.binarySearch(getBeaconRowNumbers(), newSize);
      if (searchIndex < 0)
        searchIndex = -1 - searchIndex;
      beaconTable.truncate(searchIndex);
    } catch (IOException iox) {
      throw new UncheckedIOException("on truncate(" + newSize + ")", iox);
    }
    
    int searchIndex = Collections.binarySearch(witnessRepo.getIds(), newSize);
    if (searchIndex < 0)
      searchIndex = -1 - searchIndex;
    witnessRepo.trimSize(searchIndex);
  }
  
  
  
  public Path beaconedPath(long lo, long hi) {
    SkipPath vanilla = ledger.skipPath(lo, hi);
    return makeBeaconed(vanilla);
  }
  
  
  public Path beaconedStatePath() {
    return makeBeaconed(ledger.statePath());
  }
  
  
  
  
  private Path makeBeaconed(Path vanilla) {
    List<Tuple<Long,Long>> beacons;
    List<Long> bcnRowNumbers = getBeaconRowNumbers();
    List<Long> bcnUtcs = getBeaconUtcs();
    List<Long> pathNumbers = vanilla.rowNumbers();
    Iterator<Long> i = Sets.intersectionIterator(bcnRowNumbers, pathNumbers);
    if (!i.hasNext())
      return vanilla;

    beacons = new ArrayList<>();
    do {
      
      Long rn = i.next();
      int index = Collections.binarySearch(bcnRowNumbers, rn);
      // index >= 0;
      Long utc = bcnUtcs.get(index);
      beacons.add(new Tuple<>(rn, utc));
      
    } while (i.hasNext());
    
    return vanilla.isTargeted()? new TargetPath(vanilla, beacons) : new Path(vanilla, beacons);
  }
  
  
  
  public Optional<Nugget> getNugget(long rowNumber) {
    return getNugget(rowNumber, true);
  }
  
  
  public Optional<Nugget> getNugget(long rowNumber, boolean withBeaconDate) {
    Optional<TrailedPath> trailOpt = getTrail(rowNumber);
    if (trailOpt.isEmpty())
      return Optional.empty();
    
    Path path;
    if (withBeaconDate) {
      List<Long> bcnRowNums = getBeaconRowNumbers();
      int bindex = Collections.binarySearch(bcnRowNums, rowNumber);
      if (bindex < 0) { // usual path
        bindex = -1 - bindex;
        if (bindex == 0) {
          withBeaconDate = false;
          path = ledger.skipPath(rowNumber, ledger.size());
        } else {
          long bcnRowNumber = bcnRowNums.get(bindex - 1);
          SkipPath head = ledger.skipPath(bcnRowNumber, rowNumber);
          SkipPath tail = ledger.skipPath(rowNumber, ledger.size());
          path = CamelPath.concatInstance(head, tail);
        }
      } else {  // the row itself is a beacon (?) .. ok, we don't disallow it
        assert rowNumber == bcnRowNums.get(bindex).longValue();
        path = ledger.skipPath(rowNumber, ledger.size());
      }
    } else
      path = ledger.skipPath(rowNumber, ledger.size());
    
    if (withBeaconDate)
      path = makeBeaconed(path);
    
    Nugget nug = new Nugget(path, trailOpt.get());
    return Optional.of(nug);
  }
  
  
  
  /**
   * Returns a <tt>TrailedPath</tt> to the given row number, if there is yet
   * evidence (a {@linkplain CrumTrail}) that it has been witnessed . (There
   * is no evidence that a row number has been witnessed until either that row
   * or a subsequent row is witnessed.)
   * 
   * @param rowNumber &ge; 1 and &lte; {@linkplain #size()}
   */
  public Optional<TrailedPath> getTrail(long rowNumber) {
    
    // find the oldest (smallest) row number that is 
    // greater than or equal to rowNumber
    final int witnessedIndex;
    final long witnessedRowNumber;
    {
      List<Long> witnessedRows = witnessRepo.getIds();
      int wIndex = Collections.binarySearch(witnessedRows, rowNumber);
      if (wIndex < 0) {
        int insertionIndex = -1 - wIndex;
        if (insertionIndex == witnessedRows.size())
          return Optional.empty();
        wIndex = insertionIndex;
      }
      witnessedIndex = wIndex;
      witnessedRowNumber = witnessedRows.get(witnessedIndex);
    }
    
    CrumTrail trail = witnessRepo.getTrail(witnessedIndex);
    SkipPath path = ledger.skipPath(rowNumber, witnessedRowNumber);
    
    return Optional.of(new TrailedPath(path, trail));
  }
  
  
  
  public CrumTrail getCrumTrail(int index) {
    return witnessRepo.getTrail(index);
  }
  
  
  
  
  public List<Long> getRowNumbersWitnessed() {
    return witnessRepo.getIds();
  }
  
  
  
  public long lastWitnessNum() {
    List<Long> ids = witnessRepo.getIds();
    return ids.isEmpty() ? 0 : ids.get(ids.size() - 1);
  }
  
  
  
  public WitnessReport witness() {
    return witness(true);
  }
  
  
  public WitnessReport witness(boolean includeLast) {
    
    
    final long unwitnessedRows;
    {
      long lastWitNum = lastWitnessNum();
      unwitnessedRows = ledger.size() - lastWitNum;
      
      if (unwitnessedRows == 0)
        return new WitnessReport(lastWitNum);
      
      else if (unwitnessedRows < 0)
        throw new SldgException(
            "size/lastWitnessNum : " + ledger.size() + "/" + lastWitNum);
    }
    
    int toothExponent;
    {
      int p = 1;
      for (; (unwitnessedRows >> p) > SldgConstants.DEF_TOOTHED_WIT_COUNT; ++p);
      toothExponent = p - 1;
    }
    
    return witness(toothExponent, true);
  }
  
  
  public WitnessReport witness(int exponent, boolean includeLast) {

    if (exponent < 0)
      throw new IllegalArgumentException("negatve exponent " + exponent);
    if (exponent > SldgConstants.MAX_WITNESS_EXPONENT)
      throw new IllegalArgumentException("out of bounds exponent " + exponent);
    
    final long lastRowNum = ledger.size();
    
    final long lastWitNum = lastWitnessNum();
    
    if (lastWitNum == lastRowNum)
      return new WitnessReport(lastWitNum);
    
    sanityCheckWitNum(lastWitNum, lastRowNum);
    
    final long witNumDelta = 1L << exponent;

    final boolean includeUntoothed = lastRowNum % witNumDelta != 0 && includeLast;
    
    
    
    List<Row> rowsToWitness;
    {
      ArrayList<Row> rows = new ArrayList<>(SldgConstants.MAX_BLOCK_WITNESS_COUNT);
      final int maxLoopCount =
          includeUntoothed ? SldgConstants.MAX_BLOCK_WITNESS_COUNT : SldgConstants.MAX_BLOCK_WITNESS_COUNT - 1;
      for (
          long toothedNum = ((lastWitNum + witNumDelta) / witNumDelta) * witNumDelta;
          // i.e. toothedNum % witNumDelta == 0
          toothedNum <= lastRowNum && rows.size() < maxLoopCount;
          toothedNum += witNumDelta) {
        
        rows.add(ledger.getRow(toothedNum));
      }
      
      if (includeUntoothed)
        rows.add(ledger.getRow(lastRowNum));
      
      
      if (rows.isEmpty())
        return new WitnessReport(lastWitNum);
            
      rowsToWitness = Lists.reverse(rows);
    }
    
    
    Client remote = new Client();
    rowsToWitness.forEach(r -> remote.addHash(r.hash()));
    
    List<CrumRecord> records = remote.getCrumRecords();
    
    List<WitnessRecord> zip = zip(records, rowsToWitness);
    
    // FIXME: it's possible to setup an attack that deliberately fails
    // this algorithm (the upshot being that the crumtrails in the
    // repo no longer being ordered by time). This would not be a real data "corruption";
    // instead it would be a broken index that would need mending. Kicking this
    // can down the road, but it's worthwhile to note the fix, if a bit tedious,
    // is still not complicated.
    SortedSet<WitnessRecord> trailedRecords = filterTrailed(zip);
    
    long witnessedRowNumber = 0;
    
    List<WitnessRecord> stored = new ArrayList<>(trailedRecords.size());
    
    for (WitnessRecord trailed : trailedRecords) {
      
      final long rn = trailed.row.rowNumber();
      
      assert rn > lastWitNum;
      
      if (rn <= witnessedRowNumber) {
        assert rn != witnessedRowNumber;
        continue;
      }
      
      witnessedRowNumber = rn;
      witnessRepo.putTrail(trailed.record.trail(), rn);
      stored.add(trailed);
    }
    
    stored = stored.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(stored);
    return new WitnessReport(Lists.reverse(zip), stored, lastWitNum);
  }
  
  
  /**
   * Gets the beacon (the latest published {@linkplain TreeRef} at crums.io),
   * adds its hash as the next entry in the ledger and returns its row number.
   * The beacon's {@linkplain TreeRef#maxUtc() maxUtc} and ledger row number are
   * maintained are in a separate table for fast lookup.
   */
  public long addBeacon() {
    Client remote = new Client();
    TreeRef beacon = remote.getBeacon();
    long beaconRowNumber = ledger.appendRows(beacon.hash());

    ByteBuffer row = ByteBuffer.allocate(BEACON_TBL_WIDTH);
    row.putLong(beacon.maxUtc()).putLong(beaconRowNumber).flip();
    
    try {
      beaconTable.append(row);
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
    return beaconRowNumber;
  }
  
  
  
  public int getBeaconCount() {
    try {
      long count = beaconTable.getRowCount();
      if (count > Integer.MAX_VALUE)
        throw new SldgException(
            "beacon table size " +  count + " beyond max capacity");
      return (int) count;
    
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on getBeaconCount(): " + iox.getMessage(), iox);
    }
  }
  
  
  
  public int sizeSansBeacons() {
    try {
      long count = ledger.size() - beaconTable.getRowCount();
      if (count > Integer.MAX_VALUE)
        throw new SldgException(
            "number of (non-beacon) rows " +  count + " exceeds max capacity");
      return (int) count;
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on sizeSansBeacons(): " + iox.getMessage(), iox);
    }
  }
  
  
  
  public List<Long> getBeaconUtcs() {
    return beaconSnapshot(r -> r.getLong(0));
  }
  
  
  
  public List<Long> getBeaconRowNumbers() {
    return beaconSnapshot(r -> r.getLong(8));
  }
  
  
  /**
   * Translates and returns the given sans-beacon row number as a row number that
   * includes the preceding beacon rows.
   * <h3>Purpose</h3>
   * <p>
   * From a user-perspective, it may be desirable to orthogonalize the bookkeeping of
   * ledger contents from that of timekeeping. Beacon rows serve no purpose other than
   * to establish a <em>minimum bound on the age of rows</em> in a ledger.
   * </p>
   * 
   * @param rowNum non-negative, <em>sans-beacon</em> row number 
   * 
   * @return the row number, after accounting for the preceding beacon rows
   * 
   * @throws IllegalArgumentException if {@code rowNum} &lt; 0
   */
  public long rowNumWithBeacons(final long rowNum) {
    if (rowNum < 0)
      throw new IllegalArgumentException("rowNum " + rowNum);
    
    List<Long> beaconRns = getBeaconRowNumbers();
    
    if (beaconRns.isEmpty())
      return rowNum;
    else
      return new FilteredIndex(beaconRns).toUnfilteredIndex(rowNum);
  }
  
  
  /**
   * Translates and returns the given row number as a <em>sans-beacon</em> row
   * number. This is what the row number would be, if the ledger contained no
   * preceding beacon rows.
   * 
   * @param rowNum a regular row number that is not an actual beacon row
   * 
   * @return the row number, as if the ledger had no beacons
   * @throws IllegalArgumentException if {@code rowNum} is a beacon row
   * 
   * @see #rowNumWithBeacons(long)
   */
  public long rowNumSansBeacons(long rowNum) {
    if (rowNum < 0)
      throw new IllegalArgumentException("rowNum " + rowNum);
    
    int searchIndex = Collections.binarySearch(getBeaconRowNumbers(), rowNum);
    if (searchIndex < 0) {
      int beaconsAhead = -1 - searchIndex;
      return rowNum - beaconsAhead;
    }
    throw new IllegalArgumentException("rowNum " + rowNum + " is a beacon row");
  }
  
  
  private List<Long> beaconSnapshot(Function<ByteBuffer, Long> fun) {
    try {
      
      if (beaconTable.isEmpty())
        return Collections.emptyList();
      
      List<ByteBuffer> snapshot = beaconTable.getListSnapshot();
      
      return CachingList.cache(Lists.map(snapshot, fun));
      
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }




  private SortedSet<WitnessRecord> filterTrailed(Collection<WitnessRecord> records) {
    TreeSet<WitnessRecord> trailed = new TreeSet<>();
    for (WitnessRecord record : records)
      if (record.isTrailed())
        trailed.add(record);
    return trailed;
  }


  
  
  private void sanityCheckWitNum(long lastWitNum, long lastRowNum) {

    if (lastWitNum < 0)
      throw new SldgException(
          "last witnessed row number " + lastWitNum + " is negative");
    else if (lastWitNum > lastRowNum)
      throw new SldgException(
          "last witnessed row number " + lastWitNum +
          " > last row number " + lastRowNum);
  }
  
  
  
  
  
  private List<WitnessRecord> zip(List<CrumRecord> records, List<Row> rowsToWitness) {

    if (records.size() != rowsToWitness.size())
      throw new ClientException(
          "response length mismatch: expected " + rowsToWitness.size() +
          "; actual was " + records.size());
    
    ArrayList<WitnessRecord> zip = new ArrayList<>(records.size());
    for (int index = 0; index < records.size(); ++index)
      zip.add(new WitnessRecord( rowsToWitness.get(index), records.get(index)) );
    return zip;
  }
  
  
  
  public final static class WitnessRecord implements Comparable<WitnessRecord> {
    
    private final Row row;
    private final CrumRecord record;
    
    private WitnessRecord(Row row, CrumRecord record) {
      this.row = row;
      this.record = record;
      if (!record.crum().hash().equals(row.hash()))
        throw new ClientException("hash mismatch from remote: " + record + " / " + row);
    }
    
    @Override
    public int compareTo(WitnessRecord o) {
      int comp = Long.compare(utc(), o.utc());
      return comp == 0 ? - Long.compare(rowNum(), o.rowNum()) : comp;
    }
    
    public long utc() {
      return record.crum().utc();
    }
    
    public long rowNum() {
      return row.rowNumber();
    }
    
    public boolean isTrailed() {
      return record.isTrailed();
    }
    
    
    public Row row() {
      return row;
    }
    
    public CrumRecord record() {
      return record;
    }
    
    /**
     * Equality semantics decided solely by {@linkplain #row}.
     */
    public boolean equals(Object o) {
      return o == this || (o instanceof WitnessRecord) && ((WitnessRecord) o).row.equals(row);
    }
    
    public int hashCode() {
      return row.hashCode();
    }
    
  }
  
  
  /**
   * Summary of a ledger {@linkplain Db#witness() witness} action.
   */
  public final static class WitnessReport {
    
    private final List<WitnessRecord> records;
    private final List<WitnessRecord> stored;
    
    private final long prevLastWitNum;
    
    /**
     * Empty instance.
     * 
     * @param prevLastWitNum
     */
    private WitnessReport(long prevLastWitNum) {
      records = stored = Collections.emptyList();
      this.prevLastWitNum = prevLastWitNum;
    }
    
    private WitnessReport(
        List<WitnessRecord> records, List<WitnessRecord> stored, long prevLastWitNum) {
      this.records = records;
      this.stored = stored;
      this.prevLastWitNum = prevLastWitNum;
    }
    
    /**
     * Returns the list of witnessed records.
     */
    public List<WitnessRecord> getRecords() {
      return records;
    }
    
    
    /**
     * Returns the list of witnessed records that were stored.
     * 
     * @return immutable ordered list
     */
    public List<WitnessRecord> getStored() {
      return stored;
    }
    
    
    public boolean nothingDone() {
      return records.isEmpty();
    }
    
    
    /**
     * Returns the highest previously witnessed (and recorded) row number.
     */
    public long prevLastWitNum() {
      return prevLastWitNum;
    }
    
    
  }

}
































