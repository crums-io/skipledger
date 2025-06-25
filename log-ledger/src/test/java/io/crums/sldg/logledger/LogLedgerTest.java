/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static io.crums.sldg.logledger.LineParserTest.*;

import org.junit.jupiter.api.Test;

import io.crums.io.FileUtils;
import io.crums.io.Opening;
import io.crums.io.channels.ChannelUtils;
import io.crums.sldg.Path;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.logledger.LogLedger.Job;
import io.crums.sldg.logledger.LogLedger.JobResult;
import io.crums.sldg.src.DataType;
import io.crums.sldg.src.SourceRow;
import io.crums.testing.IoTestCase;
import io.crums.util.Lists;
import io.crums.util.Strings;
import io.crums.util.TaskStack;

/**
 * 
 */
public class LogLedgerTest extends IoTestCase {
  
  // over 5k lines
  final static String US = "hd110-50.log";
  final static long US_LINE_COUNT = 5833;


  /**
   * Reloads and returns the given instance after validating it has the same
   * salt scheme and salter.
   * 
   * @param lgl     <em>not from the program's current directory</em>.
   */
  public static LogLedger reload(LogLedger lgl, Grammar grammar) {
    File log = lgl.getLogFile();
    File dir = lgl.lglDir();
    var reloaded = new LogLedger(log, dir, grammar);
    assertEquals(lgl.rules().saltScheme(), reloaded.rules().saltScheme());
    assertEquals(lgl.rules().salter(), reloaded.rules().salter());
    return reloaded;
  }
  
  
  @Test
  public void testInit() throws Exception {
    Object label = new Object() {  };
    
    File dir = makeTestDir(label);
    File log = copyResource(dir, HD);
    
    var lgl = LogLedger.initSalt(log, null);
    assertEquals(Grammar.DEFAULT, lgl.rules().grammar());
    assertTrue(lgl.rules().saltScheme().saltAll());
    assertEquals(new File(dir, Files.EXT_ROOT), lgl.lglDir());
    assertEquals(log, lgl.getLogFile());
    
    assertTrue(lgl.job().isEmpty());
    
    // reload
    reload(lgl, null);
    
    // verify attempt to re-init fails
    try {
      LogLedger.initSalt(log, null);
      fail();
    } catch (Exception expected) {
      System.out.println(method(label) + ": [EXPECTED] " + expected);
    }
    
    // verify attempt to re-init didn't break the rules
    var copy = reload(lgl, null);
    assertEquals(Grammar.DEFAULT, copy.rules().grammar());
  }
  
  
  
  @Test
  public void testInitWithCustomGrammar() throws Exception {
    Object label = new Object() {  };
    
    final boolean skipBlankLines = false;
    final String tokenDelimiters = ", \t\f\r.";
    final String commentPrefix = "//";
    
    File dir = makeTestDir(label);
    File log = copyResource(dir, HD);
    
    var lgl = LogLedger.init(
        log, dir,
        skipBlankLines,
        tokenDelimiters,
        commentPrefix,
        true);

    assertTrue(lgl.rules().saltScheme().saltAll());
    assertEquals(dir, lgl.lglDir());
    
    lgl = reload(lgl, null);
    
    assertTrue(lgl.isSalted());
    
    Grammar grammar = lgl.rules().grammar();
    
    assertEquals(skipBlankLines, grammar.skipBlankLines());
    assertEquals(tokenDelimiters, grammar.tokenDelimiters().get());
    
    String testLine = "this,this is only a test.";
    var expectedTokens = Arrays.asList("this", "this", "is", "only", "a", "test");
    var tokens = grammar.parseTokens(testLine);
    assertEquals(expectedTokens, tokens);
    Predicate<String> p =
        s -> grammar.commentMatcher().get().test(ByteBuffer.wrap(s.getBytes()));
    assertFalse(p.test(testLine));
    assertFalse(p.test("/" + testLine));
    assertFalse(p.test(" //" + testLine));
    assertTrue(p.test("//"));
    assertTrue(p.test("//" + testLine));
    assertTrue(p.test("///" + testLine));
  }
  
  
  
  
  
  
  
  
  
  
  @Test
  public void testParseToEnd() throws Exception {
    Object label = new Object() {  };
    
    File dir = makeTestDir(label);
    File log = copyResource(dir, HD);
    
    Grammar grammar = Grammar.DEFAULT.skipBlankLines(false);
    LogLedger lgl = LogLedger.initSalt(log, grammar);
    assertEquals(grammar, lgl.rules().grammar());
    lgl.job().computeHash(true);
    JobResult result = lgl.executeJob();
    Checkpoint state = result.parseState().get();
    System.out.println(method(label) + ": " + state);
  }
  
  
  
  @Test
  public void testParseToEndAndSave() throws Exception {
    Object label = new Object() {  };
    
    File dir = makeTestDir(label);
    File log = copyResource(dir, HD);
    
    Grammar grammar = Grammar.DEFAULT.skipBlankLines(false);
    LogLedger lgl = LogLedger.initSalt(log, grammar);
    assertEquals(grammar, lgl.rules().grammar());
    lgl.job().saveParseState(true);
    assertTrue(lgl.job().saveParseState());
    assertTrue(lgl.job().computeHash());
    JobResult result = lgl.executeJob();
    Checkpoint state = result.parseState().get();
    System.out.println(method(label) + ": saved " + state);
    assertEquals(state, lgl.nearestCheckpoint(state.rowNo() + 27L).get());
    lgl = reload(lgl, grammar);
    assertEquals(state, lgl.nearestCheckpoint(state.rowNo() + 27L).get());
  }
  
  
  
  @Test
  public void testSaveParseStateTo8() throws Exception {
    Object label = new Object() {  };
    
    File dir = makeTestDir(label);
    File log = copyResource(dir, HD);
    
    final long lastRow = 8;
    
    Grammar grammar = Grammar.DEFAULT.skipBlankLines(false);
    LogLedger lgl = LogLedger.initSalt(log, grammar);
    assertEquals(grammar, lgl.rules().grammar());
    lgl.job().saveParseState(true).maxRowHashed(lastRow);
    assertTrue(lgl.job().saveParseState());
    assertTrue(lgl.job().computeHash());
    assertEquals(lastRow, lgl.job().maxRowHashed());
    JobResult result = lgl.executeJob();
    Checkpoint state = result.parseState().get();
    assertEquals(lastRow, state.rowNo());
    System.out.println(method(label) + ": saved " + state);
    assertEquals(state, lgl.nearestCheckpoint(state.rowNo() + 27L).get());
    lgl = reload(lgl, grammar);
    assertEquals(state, lgl.nearestCheckpoint(state.rowNo() + 27L).get());
  }
  
  
  
  @Test
  public void testHash5Then8To11() throws Exception {
    Object label = new Object() {  };
    
    File dir = makeTestDir(label);
    File log = copyResource(dir, HD);
    
    
    final long initLastRow = 5;
    
    final long firstRow = 8L;
    final long lastRow = 11L;
    
    Grammar grammar = Grammar.DEFAULT.skipBlankLines(false);
    LogLedger lgl = LogLedger.initSalt(log, grammar);
    
    lgl.job().saveParseState(true).maxRowHashed(initLastRow);
    JobResult result = lgl.executeJob();
    Checkpoint state = result.parseState().get();
    assertEquals(initLastRow, state.rowNo());
    System.out.println(method(label) + ": saved " + state);
    
    lgl.newJob()
        .saveParseState(true)
        .minRowHashed(firstRow)
        .maxRowHashed(lastRow);
    
    state = lgl.executeJob().parseState().get();
    System.out.println(method(label) + ": saved " + state);
    assertEquals(lastRow, state.rowNo());
    assertEquals(state, lgl.nearestCheckpoint(state.rowNo() + 27L).get());
    lgl = reload(lgl, grammar);
    assertEquals(state, lgl.nearestCheckpoint(state.rowNo() + 27L).get());
  }
  
  
  
  @Test
  public void testHash6Then8To11() throws Exception {
    Object label = new Object() {  };
    
    File dir = makeTestDir(label);
    File log = copyResource(dir, HD);
    
    
    final long initLastRow = 6;
    
    final long firstRow = 8L;
    final long lastRow = 11L;
    
    Grammar grammar = Grammar.DEFAULT.skipBlankLines(false);
    LogLedger lgl = LogLedger.initSalt(log, grammar);
    
    lgl.job().saveParseState(true).maxRowHashed(initLastRow);
    JobResult result = lgl.executeJob();
    Checkpoint state = result.parseState().get();
    assertEquals(initLastRow, state.rowNo());
    System.out.println(method(label) + ": saved " + state);
    
    lgl.newJob()
        .saveParseState(true)
        .minRowHashed(firstRow)
        .maxRowHashed(lastRow);
    
    state = lgl.executeJob().parseState().get();
    System.out.println(method(label) + ": saved " + state);
    assertEquals(lastRow, state.rowNo());
    assertEquals(state, lgl.nearestCheckpoint(state.rowNo() + 27L).get());
    lgl = reload(lgl, grammar);
    assertEquals(state, lgl.nearestCheckpoint(state.rowNo() + 27L).get());
    assertEquals(Arrays.asList(initLastRow, lastRow), lgl.checkpointNos());
  }
  
  
  
  @Test
  public void testParseUsToEnd() throws Exception {
    Object label = new Object() {  };
    
    File dir = makeTestDir(label);
    File log = copyResource(dir, US);
    
    Grammar grammar = Grammar.DEFAULT.skipBlankLines(false);

    LogLedger lgl = LogLedger.initSalt(log, grammar);
    
    lgl.job().saveParseState(true);
    assertTrue(lgl.job().saveParseState());
    assertTrue(lgl.job().computeHash());
    JobResult result = lgl.executeJob();
    Checkpoint state = result.parseState().get();
    System.out.println(method(label) + ": saved " + state);
    assertEquals(US_LINE_COUNT, state.rowNo());
    assertEquals(log.length(), state.eol());
    lgl = reload(lgl, grammar);
    assertEquals(state, lgl.nearestCheckpoint(state.rowNo() + 1L).get());
  }
  
  
  /**
   * This creates a ~30 MB mock log file. 
   * @throws Exception
   */
  @Test
  public void testUs100Copies() throws Exception {

    Object label = new Object() {  };
    var out = System.out;
    
    final int copies = 100;
    final long expectedRows = US_LINE_COUNT * copies;
    
    out.println();
    out.println(method(label) + ": BENCHMARK");
    
    File dir = makeTestDir(label);

    // prepare the log file..
    File log = new File(dir, "usX" + copies + ".log");
    out.printf("  appending %d copies of %s to %s", copies, US, log.getName());
    {
      File single = copyResource(dir, US);
      var bytes = FileUtils.loadFileToMemory(single).slice();
      assertEquals('\n', bytes.get(bytes.limit() - 1));
      try (var ch = Opening.CREATE.openChannel(log)) {
        for (int countdown = copies; countdown-- > 0; ) {
          bytes.clear();
          ChannelUtils.writeRemaining(ch, bytes);
        }
      }
    }
    out.printf(" [DONE]%n  parsing log (%d bytes)..%n", log.length());
    
    Grammar grammar = Grammar.DEFAULT.skipBlankLines(false);
    

    LogLedger lgl = LogLedger.initSalt(log, grammar);

    assertTrue(lgl.job().computeHash(true).computeHash());
    
    long bench = System.nanoTime();
    
    JobResult result = lgl.executeJob();
    
    Checkpoint state = result.parseState().get();
    var stats = result.hasherStats().get();
    
    long lap = System.nanoTime() - bench;
    
    out.printf(
        "  %s%n  tokenized %d rows (%d tokens) in %s ms%n",
        state,
        stats.rows(),
        stats.tokens(),
        nanosToMillis(lap));
    
    assertEquals(expectedRows, state.rowNo());
    
    out.print("  2nd pass: building skipledger and source index..");
    
    bench = System.nanoTime();
    
    long rowsAdded = lgl.buildSkipLedger();
    
    lap = System.nanoTime() - bench;
    out.printf(" [DONE] (%s ms)%n", nanosToMillis(lap));
    
    assertEquals(expectedRows, rowsAdded);
    assertTrue(lgl.isRandomAccess());
    
    try (var closer = new TaskStack()) {
      SkipLedger sldg = lgl.loadSkipLedger().get();
      closer.pushClose(sldg);
      SourceIndex sources = lgl.loadSourceIndex().get();
      assertEquals(expectedRows, sldg.size());
      assertEquals(expectedRows, sources.rowCount());
      
      out.println("--Indexed retrieval--");
      
      bench = System.nanoTime();
      Path path = sldg.statePath();
      long midLap = System.nanoTime();
      path = path.pack().path();
      lap = System.nanoTime();
      
      
      out.printf("  retrieved state path in %s ms:%n  %s%n",
          nanosToMillis(lap - bench), path);
      out.printf("    lazy reference retrieved in %s ms%n",
          nanosToMillis(midLap - bench));
      out.printf("    references resolved in %s ms%n",
          nanosToMillis(lap - midLap));
      
      midLap = lap - bench;
      
      ArrayList<SourceRow> srcRows = new ArrayList<>();
      
      bench = System.nanoTime();
      for (long rowNo : path.rowNumbers())
        srcRows.add(sources.sourceRow(rowNo));
      
      lap = System.nanoTime();
      
      out.printf("  retrieved %d source rows (along state path) in %s ms:%n",
          path.length(),
          nanosToMillis(lap - bench));
      print(srcRows, path);
    }
    out.println();
  }
  
  @Test
  public void testParseUsPath1ToEnd() throws Exception {
    Object label = new Object() {  };
    
    File dir = makeTestDir(label);
    File log = copyResource(dir, US);
    
    Grammar grammar = Grammar.DEFAULT.skipBlankLines(false);

    LogLedger lgl = LogLedger.initSalt(log, grammar);
    
    Job job = lgl.job();
    assertTrue(job.addToPath(1L));
    assertTrue(job.computeHash());
    assertTrue(job.addToPath(US_LINE_COUNT));
    job.saveParseState(true);
    JobResult result = lgl.executeJob();
    Checkpoint state = result.parseState().get();
    System.out.println(method(label) + ": " + state);
    Path path = result.path().get();
    assertEquals(1L, path.lo());
    assertEquals(US_LINE_COUNT, path.hi());
    assertEquals(state.frontier().frontierHash(), path.last().hash());
    
    assertEquals(US_LINE_COUNT, state.rowNo());
    assertEquals(log.length(), state.eol());
    lgl = reload(lgl, grammar);
    assertEquals(state, lgl.nearestCheckpoint(state.rowNo() + 1L).get());
  }
  
  @Test
  public void testParseUsSrcRows78Thru88() throws Exception {
    Object label = new Object() {  };
    
    File dir = makeTestDir(label);
    File log = copyResource(dir, US);
    
    final long firstSrcNo = 78L;
    final long lastSrcNo = 88L;
    
    Grammar grammar = Grammar.DEFAULT.skipBlankLines(false);

    LogLedger lgl = LogLedger.initSalt(log, grammar);
    
    Job job = lgl.job();
    for (long rn = firstSrcNo; rn <= lastSrcNo; ++rn)
      assertTrue(job.addSourceRow(rn));
    
    // by default, source rows are included in the path
    // so the following must be true
    assertTrue(job.computeHash());
    
    final List<Long> expected = Lists.longRange(firstSrcNo, lastSrcNo);
    assertEquals(expected, job.sourceNos());
    assertEquals(expected, job.pathStitchNos()); // default behavior
    
    assertTrue(job.addToPath(US_LINE_COUNT));
    
    job.saveParseState(true);
    JobResult result = lgl.executeJob();
    Checkpoint state = result.parseState().get();
    System.out.println(method(label) + ": " + state);
    
    assertEquals(log.length(), state.eol());
    Path path = result.path().get();
    assertEquals(firstSrcNo, path.lo());
    assertEquals(US_LINE_COUNT, path.hi());
    assertEquals(state.frontier().frontierHash(), path.last().hash());
    
    assertEquals(expected, Lists.map(result.sources(), SourceRow::no));
    for (long rn : expected)
      assertTrue(path.hasRow(rn));
    
    print(result.sources(), path);
    
    
    assertEquals(US_LINE_COUNT, state.rowNo());
    lgl = reload(lgl, grammar);
    assertEquals(state, lgl.nearestCheckpoint(US_LINE_COUNT + 1L).get());
  }
  
  
  

  
  @Test
  public void testParseUsSrcRows595Thru600() throws Exception {
    Object label = new Object() {  };

    final var out = System.out;
    
    File dir = makeTestDir(label);
    File log = copyResource(dir, US);
    
    final long firstSrcNo = 595L;
    final long lastSrcNo = 600L;
    
    Grammar grammar = Grammar.DEFAULT.skipBlankLines(false);

    LogLedger lgl = LogLedger.initSalt(log, grammar);
    
    Job job = lgl.job();
    // add in reverse order (shouldn't make a difference)
    for (long rn = lastSrcNo; rn >= firstSrcNo; --rn)
      assertTrue(job.addSourceRow(rn));
    
    // by default, source rows are included in the path
    // so the following must be true
    assertTrue(job.computeHash());
    
    final List<Long> expected = Lists.longRange(firstSrcNo, lastSrcNo);
    assertEquals(expected, job.sourceNos());
    assertEquals(expected, job.pathStitchNos()); // default behavior
    
    assertTrue(job.addToPath(US_LINE_COUNT));
    
    job.saveParseState(true);
    
    long now = System.nanoTime();
    
    JobResult result = lgl.executeJob();
    
    long lap = System.nanoTime() - now;
    
    Checkpoint state = result.parseState().get();
    
    out.println(method(label) + ": " + state);
    out.println("  executed in " + nanosToMillis(lap) + " ms");
    
    assertEquals(log.length(), state.eol());
    Path path = result.path().get();
    assertEquals(firstSrcNo, path.lo());
    assertEquals(US_LINE_COUNT, path.hi());
    assertEquals(state.frontier().frontierHash(), path.last().hash());
    
    assertEquals(expected, Lists.map(result.sources(), SourceRow::no));
    for (long rn : expected)
      assertTrue(path.hasRow(rn));
    
    print(result.sources(), path);
    
    
    assertEquals(US_LINE_COUNT, state.rowNo());
    lgl = reload(lgl, grammar);
    assertEquals(state, lgl.nearestCheckpoint(US_LINE_COUNT + 1L).get());
  }
  
  
  
  /** Print source rows, and validate if path provided. */
  private void print(List<SourceRow> sources, Path path) {
    
    final var out = System.out;
    for (SourceRow srcRow : sources) {
      if (path != null)
        assertEquals(
            path.getRowByNumber(srcRow.no()).inputHash(),
            srcRow.hash());
      
      for (var type : srcRow.cellTypes())
        assertEquals(DataType.STRING, type);
      
      out.print("  [" + srcRow.no() + "]");
      for (var cell : srcRow.cells()) {
        assertTrue(cell.hasSalt());
        out.print(' ');
        out.print(Strings.utf8String(cell.data()));
      }
      out.println();
    }
  }
  
  
  

  
  @Test
  public void testRetrieveSourceRowOnly() throws Exception {
    Object label = new Object() {  };

    final var out = System.out;
    
    File dir = makeTestDir(label);
    File log = copyResource(dir, US);
    
    final long testRowNo = 5825;
    
    Grammar grammar = Grammar.DEFAULT.skipBlankLines(false);

    LogLedger lgl = LogLedger.initSalt(log, grammar);
    
    lgl.job().addSourceRow(testRowNo, false);
    
    long now = System.nanoTime();
    JobResult result = lgl.executeJob();
    long lap = System.nanoTime() - now;
    
    out.printf("%s:%n  retrieved row [%d] in %s ms (no index)%n",
        method(label),
        testRowNo,
        nanosToMillis(lap) );
    
    assertEquals(testRowNo, result.sources().get(0).no());
    
    print(result.sources(), null);
  }
  
  @Test
  public void testIndexUsNoHash() throws Exception {
    Object label = new Object() {  };

    final var out = System.out;
    
    File dir = makeTestDir(label);
    File log = copyResource(dir, US);
    
    final long testRowNo = 5825;
    
    Grammar grammar = Grammar.DEFAULT.skipBlankLines(false);

    LogLedger lgl = LogLedger.initSalt(log, grammar);
    
    Job job = lgl.job().indexOffsets(true);
    assertFalse(job.computeHash());
    assertTrue(job.hasWork());
    assertFalse(lgl.hasSourceIndex());
    assertEquals(0L, lgl.maxRowInOffsetsIndex());
    
    out.println(method(label) + ": indexing line offsets..");
    
    JobResult result = lgl.executeJob();
    
    assertTrue(result.isEmpty());
    
    assertTrue(lgl.hasSourceIndex());
    assertEquals(US_LINE_COUNT, lgl.maxRowInOffsetsIndex());
    
    out.println("  reload and print test row ..");
    lgl = reload(lgl, grammar);
    assertEquals(US_LINE_COUNT, lgl.maxRowInOffsetsIndex());

    
    lgl.job().addSourceRow(testRowNo, false);
    
    long now = System.nanoTime();
    result = lgl.executeJob();
    long lap = System.nanoTime() - now;
    
    out.printf("  retrieved in row [%d] in %s ms (with index)%n",
        testRowNo, nanosToMillis(lap));
    
    lgl.newJob().useOffsetsIndex(false).addSourceRow(testRowNo, false);


    now = System.nanoTime();
    result = lgl.executeJob();
    lap = System.nanoTime() - now;
    
    out.printf("  retrieved in row [%d] in %s ms (without index)%n",
        testRowNo, nanosToMillis(lap));
    
    assertEquals(testRowNo, result.sources().get(0).no());
    
    print(result.sources(), null);
  }
  
  
  private String nanosToMillis(long nanos) {
    float millis = ((float)(nanos / 100_000)) / 10;
    if (millis > 10) {
      return Integer.toString((int) millis);
    }
    return Float.toString(millis);
  }
  
  
  @Test
  public void testBuildSkipLedger() throws Exception {
    Object label = new Object() {  };

    final var out = System.out;
    
    File dir = makeTestDir(label);
    File log = copyResource(dir, US);
    
    Grammar grammar = Grammar.DEFAULT.skipBlankLines(false);

    LogLedger lgl = LogLedger.initSalt(log, grammar);
    
    assertTrue(lgl.loadSkipLedger().isEmpty());
    assertFalse(lgl.hasSkipledger());
    assertFalse(lgl.hasSourceIndex());
    
    long start = System.nanoTime();
    long rowsAdded = lgl.buildSkipLedger();
    long lap = System.nanoTime() - start;
    
    out.printf(
        "%s: built skipledger and source index for %d rows in %s ms%n",
        method(label), rowsAdded, nanosToMillis(lap));
    
    assertEquals(US_LINE_COUNT, rowsAdded);

    assertTrue(lgl.hasSkipledger());
    assertTrue(lgl.hasSourceIndex());
    
    try (SkipLedger sldg = lgl.loadSkipLedger().get()) {
      assertEquals(US_LINE_COUNT, sldg.size());
      Path statePath = sldg.statePath();
      out.println("  state path: " + statePath);
    }
    
    
    
  }
  
  
  
  
  
  File makeTestDir(Object label) {
    File dir = getMethodOutputFilepath(label);
    assertTrue( dir.mkdirs() );
    return dir;
  }
  
  
  

}

























