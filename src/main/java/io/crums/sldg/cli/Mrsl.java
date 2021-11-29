/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cli;


import static io.crums.sldg.SldgConstants.MRSL_EXT;
import static io.crums.util.Strings.pluralize;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;

import io.crums.io.Opening;
import io.crums.io.channels.ChannelUtils;
import io.crums.model.CrumTrail;
import io.crums.sldg.ByteFormatException;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.MorselFile;
import io.crums.sldg.PathInfo;
import io.crums.sldg.SldgException;
import io.crums.sldg.json.SourceInfoParser;
import io.crums.sldg.json.SourceRowParser;
import io.crums.sldg.json.TrailedRowWriter;
import io.crums.sldg.packs.MorselPack;
import io.crums.sldg.packs.MorselPackBuilder;
import io.crums.sldg.src.BytesValue;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.DateValue;
import io.crums.sldg.src.DoubleValue;
import io.crums.sldg.src.LongValue;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.src.StringValue;
import io.crums.sldg.time.TrailedRow;
import io.crums.util.IntegralStrings;
import io.crums.util.Lists;
import io.crums.util.Sets;
import io.crums.util.Strings;
import io.crums.util.json.JsonPrinter;
import io.crums.util.main.ArgList;
import io.crums.util.main.MainTemplate;
import io.crums.util.main.Option;
import io.crums.util.main.PrintSupport;
import io.crums.util.main.StdExit;
import io.crums.util.main.TablePrint;

/**
 * CLI for morsel files.
 * <p>
 * TODO:
 * <ul>
 * <li>List {@linkplain PathInfo}s and user-notes.</li>
 * <li>Support adding specific rows instead of bulk merge.</li>
 * <li>Support pruning rows.</li>
 * <li>Support adding new {@linkplain PathInfo}s (and user-notes).</li>
 * </ul>
 * </p>
 */
public class Mrsl extends MainTemplate {
  
  public final static String PROGNAME = Mrsl.class.getSimpleName().toLowerCase();
  
  private String command;
  private File morselFile;
  
  private List<Long> rowNumbers;
  private File saveFile;
  private String sep = " ";
  
  private List<File> morselFiles;
  
  private Set<Option> options = Collections.emptySet();
  
  
  private Mrsl() {
  }
  
  
  public static void main(String[] args) {
    new Mrsl().doMain(args);
  }

  @Override
  protected void init(String[] args) throws IllegalArgumentException, IOException {
    ArgList argList = newArgList(args);
    
    this.command = argList.removeCommand(SUM, STATE, LIST, HISTORY, ENTRY, MERGE);
    switch (command) {
    case SUM:
    case STATE:
    case LIST:
    case HISTORY:
      this.morselFile = argList.removeExistingFile();
      break;
    case ENTRY:
      this.morselFile = argList.removeExistingFile();
      this.rowNumbers = argList.removeNumbers(true);
      if (rowNumbers.isEmpty())
        throw new IllegalArgumentException("missing entry row numbers");
      
      options = JSON_OPT.removeOption(argList);
      setSaveFile(argList);
      String separator = argList.removeValue(SEP);
      if (separator != null)
        this.sep = Strings.unescape(separator);
      break;
    case MERGE:
      this.morselFiles = argList.removeExistingFiles();
      
      if (morselFiles.size() < 2)
        throw new IllegalArgumentException(
            morselFiles.isEmpty() ?
                "missing morsel files for merge" : "missing 2nd morsel file for merge");
      

      setSaveFile(argList);
      break;
      
    default:
      throw new RuntimeException("assertion failed. command=" + command);
    }
    if (morselFile == null && morselFiles == null)
      throw new IllegalArgumentException("required path/to/morsel file missing");
    
    
    argList.enforceNoRemaining();
  }
  
  
  private void setSaveFile(ArgList argList) {
    String filepath = argList.removeValue(SAVE);
    if (filepath != null) {
      saveFile = new File(filepath);
      checkSaveParentDir();
    }
  }
  
  
  private void checkSaveParentDir() {
    File parent = saveFile.getParentFile();
    if (parent != null && !parent.isDirectory())
      throw new IllegalArgumentException(
          "parent directory of given path does not exist: " + saveFile);
    
  }
  

  @Override
  protected void start() throws InterruptedException, IOException {
    try {
      
      startImpl();
      
    } catch (SldgException sx) {
      
      if (sx instanceof HashConflictException) {
        String msg;
        if (morselFile != null)
          msg = "Hash conflict in morsel file " + morselFile;
        else
          msg = "Hash conflict across morsel files " + morselFiles;
        
        if (sx.getMessage() != null)
          msg += ": " + sx.getMessage();
        
        throw new HashConflictException(msg, sx);
      
      } else if (sx instanceof ByteFormatException) {
        throw new ByteFormatException("Illegal byte format in morsel file " + morselFile, sx);
      
      } else
        throw sx;
    }
  }
  
  
  private void startImpl() throws InterruptedException, IOException, HashConflictException {
    switch (command) {
    case SUM:
      sum();
      break;
    case STATE:
      state();
      break;
    case LIST:
      list();
      break;
    case HISTORY:
      history();
      break;
      
    case ENTRY:
      entry();
      break;
    case MERGE:
      merge();
      break;
    default:
      throw new RuntimeException("assertion failure. command=" + command);
    }
  }
  
  
  void state() {
    System.out.println(stateString(new MorselFile(morselFile).getMorselPack()));
  }

  
  void entry() throws IOException {
    if (options.contains(JSON_OPT)) {
      jsonEntry();
    } else {
      MorselFile morsel = new MorselFile(morselFile);
      if (saveFile == null)
        printSources(morsel);
      else
        saveSources(morsel);
    }
  }

  
  void history() {
    MorselPack pack = new MorselFile(morselFile).getMorselPack();

    PrintStream out = System.out;
    out.println();
    
    String prettyName = "<" + morselFile.getName() + ">";
    
    List<Long> trailedRns = pack.trailedRowNumbers();
    if (trailedRns.isEmpty()) {
      out.println("No crumtrails in " + prettyName);
      return;
    }

    out.println();
    int count = trailedRns.size();
    out.println(count + pluralize(" row", count) + " witnessed in " + prettyName + ":");
    out.println();
    
    TablePrint table;
    {
      long maxRn = trailedRns.get(count - 1);
      int maxRnWidth = Math.max(6, Long.toString(maxRn).length());
      table = new TablePrint(maxRnWidth + 2, 78 - maxRnWidth);
    }
    
    // heading
    table.printRow("[row #]", "[date; ref hash; ref URL]");
    table.printRow("-------", "-------------------------");
    table.println();
    for (long witRn : trailedRns) {
      CrumTrail trail = pack.crumTrail(witRn);
      table.printRow(witRn, new Date(trail.crum().utc()));
      table.printRow(null, IntegralStrings.toHex(trail.rootHash()));
      table.printRow(null, trail.getRefUrl());
    }
    table.println();
  }


  private void merge() throws IOException {
    
    final int count = this.morselFiles.size();
    
    var out = System.out;
    
    out.println();
    out.println("Loading " + count + " morsels for merge..");
    
    ArrayList<MorselFile> sources = new ArrayList<>(count);
    MorselFile auth;
    {
      for (File f : morselFiles) {
        out.print(" " + f);
        sources.add(new MorselFile(f));
        out.println();
      }
      
      Collections.sort(sources, (a, b) -> compareMorsels(a, b));
      
      auth = sources.remove(sources.size() - 1);
    }

    out.println();
    out.println(" authority: " + auth.getFile());
    
    
    MorselPackBuilder builder = new MorselPackBuilder();
    int objects = builder.init(auth.getMorselPack());
    out.println();
    out.println("init " + objects + pluralize(" object", objects) + " (hi row " + auth.getMorselPack().hi() + ")");
    
    
    for (var morsel : sources) {
      
      out.print(" " + morsel.getFile() + " ..");
      int added = builder.addAll(morsel.getMorselPack());
      objects += added;
      out.println(". " + added + pluralize(" object", added) + " added");
    }
    
    File dest = MorselFile.createMorselFile(saveFile, builder);
    
    out.println();
    out.println(objects + pluralize(" object", objects) + " merged to " + dest);
  }
  
  private int compareMorsels(MorselFile a, MorselFile b) {
    MorselPack packA = a.getMorselPack();
    MorselPack packB = b.getMorselPack();
    int comp = Long.compare(packA.hi(), packB.hi());
    if (comp == 0)
      comp = packA.getFullRowNumbers().size() - packB.getFullRowNumbers().size();
    return comp;
  }


  private void saveSources(MorselFile morsel) throws IOException {
    checkSourceRowNums(morsel);
    ByteBuffer eol;
    {
      byte[] lineFeed = { (byte) '\n' };
      eol = ByteBuffer.wrap(lineFeed);
    }
    try (var ch = Opening.CREATE.openChannel(this.saveFile)) {

      MorselPack pack = morsel.getMorselPack();
      byte[] sepBytes = (sep == null || sep.isEmpty()) ? null : Strings.utf8Bytes(sep);
      for (long rn : this.rowNumbers) {
        pack.getSourceByRowNumber(rn).writeSource(ch, sepBytes);
        ChannelUtils.writeRemaining(ch, eol.clear());
      }
    }
    
    int count = rowNumbers.size();
    System.out.println(count + pluralize(" source-row", count) + " written to " + saveFile);
  }
  
  
  
  
  
  private void checkSourceRowNums(MorselFile morsel) {
    var srcNums = Sets.sortedSetView(morsel.getMorselPack().sourceRowNumbers());
    if (!srcNums.containsAll(this.rowNumbers)) {
      throw new IllegalArgumentException("illegal / unknown entry numbers: " + rowNumbers);
    }
  }
  
  
  private SortedSet<Long> sourceRowNumSet(MorselPack pack) {
    return Sets.sortedSetView(pack.sourceRowNumbers());
  }
  
  
  @SuppressWarnings("unchecked")
  void jsonEntry() {
    MorselPack pack = new MorselFile(morselFile).getMorselPack();
    
    var entryRns = sourceRowsRequested(pack);
    var trails = pack.getTrailedRows();
    int trailIndex = pack.indexOfNearestTrail(entryRns.first());
    TrailedRow nextTrail;
    long nextTrailedRn;
    if (trailIndex == -1) {
      nextTrail = null;
      nextTrailedRn = Long.MAX_VALUE;
    } else {
      nextTrail = trails.get(trailIndex);
      nextTrailedRn = nextTrail.rowNumber();
    }
    
    DateFormat dateFormat;
    {
      var info = pack.getMetaPack().getSourceInfo();
      if (info.isEmpty())
        dateFormat = null;
      else
        dateFormat = info.get().getDateFormat().orElse(null);
    }
    
    var parser = new SourceRowParser(dateFormat);
    
    var jArray = new JSONArray();
    
    boolean inject = false;
    for (long rn : entryRns) {
      var srcRow = pack.getSourceByRowNumber(rn);
      boolean advanceTrail = rn >= nextTrailedRn;
      inject = rn == nextTrailedRn;
      if (advanceTrail && !inject)
        jArray.add(TrailedRowWriter.DEFAULT_INSTANCE.toJsonObject(nextTrail));
      
      var jObj = inject ?
          parser.toJsonObject(srcRow, nextTrail) :
            parser.toJsonObject(srcRow);
      jArray.add(jObj);
      
      if (advanceTrail) {
        if (++trailIndex >= trails.size()) {
          nextTrail = null;
          nextTrailedRn = Long.MAX_VALUE;
        } else {
          nextTrail = trails.get(trailIndex);
          nextTrailedRn = nextTrail.rowNumber();
        }
      }
    }
    
    if (nextTrail != null && !inject)
      jArray.add(TrailedRowWriter.DEFAULT_INSTANCE.toJsonObject(nextTrail));
    
    var printer = new JsonPrinter(System.out);
    if (jArray.size() == 1)
      printer.print((Map<?,?>) jArray.get(0));
    else
      printer.print(jArray);
    
    System.out.println();
  }

  
  
  private TreeSet<Long> sourceRowsRequested(MorselPack pack) {

    TreeSet<Long> entryRns = new TreeSet<>(this.rowNumbers);
    entryRns.retainAll(sourceRowNumSet(pack));
    
    if (entryRns.isEmpty()) {
      System.err.println("[ERROR] No entries match the row numbers given.");
      StdExit.ILLEGAL_ARG.exit();
      return null; // never reached
    
    } else
      return entryRns;
  }
  
  
  
  

  private void printSources(MorselFile morsel) {
    MorselPack pack = morsel.getMorselPack();
    
    TreeSet<Long> entryRns = new TreeSet<>(this.rowNumbers);
    entryRns.retainAll(sourceRowNumSet(pack));
    
    if (entryRns.isEmpty()) {
      System.err.println("[ERROR] No entries match the row numbers given.");
      StdExit.ILLEGAL_ARG.exit();
      return; // never reached
    }
    
    final Long first  = entryRns.first();
    
    TablePrint table;
    {
      long lastRn = entryRns.last();
      int fcw = Math.max(
          ROOT_HASH.length() + 1 + RN_PAD,
          String.valueOf(lastRn).length() + 1 + RN_PAD);
      
      table = new TablePrint(fcw, DATE_COL, RM - fcw - DATE_COL);
    }
    
    table.println();
    
    
    var trailRns = pack.trailedRowNumbers();
    
    final int tindexStart;
    int tindex;
    long nextWitRn;
    {
      int index = Collections.binarySearch(trailRns, first);
      if (index < 0) {
        tindex = -1 - index;
        nextWitRn = tindex == trailRns.size() ? Long.MAX_VALUE : trailRns.get(tindex);
      } else {
        // an row entry can be trailed
        tindex = index;
        nextWitRn = trailRns.get(index);
      }
      tindexStart = tindex;
    }
    
    boolean didFirst = false;
    for (long entryRn : entryRns) {
      if (entryRn > nextWitRn && didFirst)
        printTrail(nextWitRn, pack, table);
      
      if (entryRn >= nextWitRn)
        nextWitRn = ++tindex == trailRns.size() ? Long.MAX_VALUE : trailRns.get(tindex);
      
      printSourceRowAsText(entryRn, pack, table);
      
      didFirst = true;
    }
    
    if (tindex < trailRns.size())
      printTrail(trailRns.get(tindex++), pack, table);
    
    table.println();
    int count = entryRns.size();
    int trails = tindex - tindexStart;
    table.println(
        count + pluralize(" entry", count) + ", " +
        trails + pluralize(" crumtrail", trails));
  }

  
  private final static String ROOT_HASH = " ref hash:";
  
  private void printTrail(long witRn, MorselPack pack, TablePrint table) {
    
    var trail = pack.crumTrail(witRn);
    
    Date date = new Date(trail.crum().utc());
    
    table.printRow(witRn, "created before " + date + "  (crumtrail)");
    table.printRow(ROOT_HASH,  IntegralStrings.toHex(trail.rootHash()) );
    table.printRow(" ref URL:",  trail.getRefUrl() );
  }


  private final static char PRINT_SEP = '-';
  
  private void printSourceRowAsText(long srcRn, MorselPack pack, TablePrint table) {
    
    SourceRow srcRow = pack.getSourceByRowNumber(srcRn);

    StringWriter string = new StringWriter();
    
    List<ColumnValue> columns = srcRow.getColumns();
    writeColumn(string, columns.get(0));
    
    for (int index = 1; index < columns.size(); ++index)
      writeColumn(string.append(' '), columns.get(index));
    
    table.printRow(srcRn, "source-row as text:");
    table.printHorizontalTableEdge(PRINT_SEP);
    table.println(string.toString());
    table.printHorizontalTableEdge(PRINT_SEP);
  }
  

  private final static String RED_TOKEN = "[X]";
  private final static String NULL_TOKEN = "null";
  
  private void writeColumn(StringWriter writer, ColumnValue col) {
    switch (col.getType()) {
    case NULL:
      writer.append(NULL_TOKEN);
      break;
    case BYTES:
      writer.append(IntegralStrings.toHex(((BytesValue) col).getBytes()));
      break;
    case HASH:
      writer.append(RED_TOKEN);
      break;
    case LONG:
      writer.append(String.valueOf(((LongValue) col).getNumber()));
      break;
    case STRING:
      writer.append(((StringValue) col).getString());
      break;
    case DOUBLE:
      writer.append(String.valueOf(((DoubleValue) col).getNumber()));
      break;
    case DATE:
      writer.append(new Date(((DateValue) col).getUtc()).toString());
      break;
    }
  }



  private final static int INDENT = 1;
  private final static int RM = 80;

  @Override
  protected void printDescription() {
    String paragraph;
    PrintSupport printer = new PrintSupport();
    printer.setMargins(INDENT, RM);
    printer.println();
    System.out.println("DESCRIPTION:");
    printer.println();
    paragraph =
        "Command line tool for reading and combining (merging) morsel files (" + MRSL_EXT + " ext). " +
        "Morsel files are tamper-proof tear-outs of rows from an opaque hash ledger optionally paired with the " +
        "source of those rows (i.e. the content whose hash is the input-hash in its corresponding " +
        " skip ledger row).";
    printer.printParagraph(paragraph);
    printer.println();
    printer.println("State morsels:");
    printer.println();
    paragraph =
        "Morsel files that contain no source rows are called \"state-morsels\". These represent the state of " +
        "the ledger (how many rows and maybe some history). Because they're small (measured in kilo bytes) they're " +
        "used as a richer fingerprint of state than just a single SHA-256 hash: their internal structure allows " +
        "an evolving ledger's fingerprint to be verified for consistency with its previous version.";
    printer.printParagraph(paragraph);
    printer.println();
    printer.println("Merge operations:");
    printer.println();
    paragraph =
        "There are typically 2 reasons to \"" + MERGE + "\" morsel files into one. The first, is to just " +
        "gather source-row information from morsels from the same ledger into one morsel. The second, is to create an updated version " +
        "of a morsel that aligns with the ledger's current state hash using a state-morsel.";
    printer.printParagraph(paragraph);
    printer.println();
  }


  private final static int LEFT_TBL_COL_WIDTH = 15;
  private final static int RIGHT_TBAL_COL_WIDTH =
      RM - INDENT - LEFT_TBL_COL_WIDTH;

  @Override
  protected void printUsage(PrintStream out) {
    PrintSupport printer = new PrintSupport(out);
    printer.setMargins(INDENT, RM);
    printer.println();
    out.println("USAGE:");
    printer.println();
    String paragraph =
        "Commands in the table below each take a morsel file as argument. The order in which " +
        "arguments are entered doesn't matter. (Reminder: if a required command collides with a " +
        "local filename, prefix the filename with '." + File.separatorChar + "' in order to " +
        "disambiguate.)";
    printer.printParagraph(paragraph);
    printer.println();

    TablePrint table = new TablePrint(out, LEFT_TBL_COL_WIDTH, RIGHT_TBAL_COL_WIDTH);
    table.setIndentation(INDENT);
    
    table.printRow(SUM, "prints a summary of the morsel's contents");
    table.println();
    
    table.printRow(STATE, "prints the highest row number and that row's hash");
    table.println();
    
    table.printRow(LIST, "lists the morsel's rows and entries (source-rows)");
    table.println();
    
    table.printRow(HISTORY, "lists the witnessed rows and their evidence");
    table.println();
    
    table.printRow(ENTRY, "lists in detail the given source-rows or saves them to a file");
    table.printRow(null,  "Arguments:");
    table.println();
    table.printRow(null,   "<row-numbers>       (required)");
    table.println();
    table.printRow(null,   "Strictly ascending line numbers separated by commas or spaces");
    table.printRow(null,   "Ranges (incl.) may be substituted for numbers. For example:");
    table.printRow(null,   "308,466,592-598,717");
    table.println();
    table.printRow(null, SAVE + "=<path/to/file> (optional)");
    table.println();
    table.printRow(null,  "If provided, then source-rows are saved to the given file path");
    table.printRow(null,  "in the order given; otherwise they are printed to the console.");
    table.println();
    table.printRow(null, SEP + "=<separator>     (optional)");
    table.println();
    table.printRow(null,  "If provided with " + SAVE + "=<path/to/file>, then column values");
    table.printRow(null,  "are separated with the given string. To set with no separator,");
    table.printRow(null,  "leave its value empty.");
    table.printRow(null,  "DEFAULT: ' '        (single whitespace)");
    table.println();
    table.printRow(null, JSON_OPT + "  (or -" + JSON_OPT.getSym() + ")");
    table.println();
    table.printRow(null,  "If set, then the output is in JSON format");
    table.println();

    table.printRow(MERGE, "merges the given morsel files to a new morsel file. The morsels");
    table.printRow(null,  "must come from the same ledger.");
    table.printRow(null,  "Arguments:");
    table.println();
    table.printRow(null,   "<path/to/morsel_1> <path/to/morsel_2> ..  (2 or more required)");
    table.println();
    table.printRow(null, SAVE + "=<path/to/file> (optional)");
    table.println();
    table.printRow(null,  "If path/to/file doesn't exist, then the merged morsel gets");
    table.printRow(null,  "created there; if the path is an existing directory, then a");
    table.printRow(null,  "file with a merge-generated name is created in that directory.");
    table.printRow(null,  "DEFAULT: '.'        (current directory)");
    table.println();
    
    
  }
  
  


  private final static int LEFT_SUM_COL_WIDTH = 17;
  private final static int RIGHT_SUM_COL_WIDTH = 25;
  private final static int MID_SUM_COL_WIDTH = RM - LEFT_SUM_COL_WIDTH - RIGHT_SUM_COL_WIDTH;
  

  void sum() {
    var morsel = new MorselFile(morselFile);
    MorselPack pack = morsel.getMorselPack();
    PrintStream out = System.out;
    out.println();
    out.println("<" + morsel.getFile().getName() + ">");
    out.println();
    
    TablePrint table = new TablePrint(LEFT_SUM_COL_WIDTH, MID_SUM_COL_WIDTH, RIGHT_SUM_COL_WIDTH);
    
    List<Long> rns = pack.getFullRowNumbers();
    var entries = pack.sources();
    var trails = pack.trailedRowNumbers();
    
    final long hi = pack.hi();

    out.println("Rows:");
    table.printRow(" count:", rns.size());
    table.printRow(" # range:", "lo: " + pack.lo(), "hi: " + hi);
    table.printRow(" with sources:", entries.size());
    out.println();
    
    // !beacons.isEmpty() || !trails.isEmpty() == !(beacons.isEmpty() && trails.isEmpty())
    if (!trails.isEmpty()) {
      out.println("History:");
      
      if (!trails.isEmpty()) {
        long witRn = trails.get(trails.size() - 1);
        var trail = pack.crumTrail(witRn);
        table.printRow(" created before:", new Date(trail.crum().utc()), "(row " + witRn + ")");
      }
      out.println();
    }
    
    out.println("<" + stateString(pack) + ">");
  }
  
  
  
  private String stateString(MorselPack pack) {
    long hi = pack.hi();
    return hi + "-" + IntegralStrings.toHex(pack.rowHash(hi));
  }
  
  
  private final static String TRL_TAG = "W";
  private final static String ENT_TAG = "S";
  
  private final static int RN_PAD = 1;
  private final static int FLAG_PAD = 2;
  private final static int DATE_COL = 32;
  
  void list() {
    MorselPack pack = new MorselFile(morselFile).getMorselPack();
    
    var entryInfos = pack.sources();
    List<Long> entryRns = Lists.map(entryInfos, e -> e.rowNumber());
    List<Long> trailRns = pack.trailedRowNumbers();
    List<Long> allRns = pack.getFullRowNumbers();
    if (allRns.isEmpty()) {
      System.out.println("Empty");
      return;
    }
    TablePrint table;
    {
      long lastRn = allRns.get(allRns.size() - 1);
      int c0w = String.valueOf(lastRn).length() + 1 + RN_PAD;
      table = new TablePrint(
          c0w, 1, 1, 1 + FLAG_PAD, DATE_COL,
          RM - c0w - 3 - FLAG_PAD - DATE_COL);
    }

    final int postFlagsSpace = RM - table.getColStart(4);
    final int postDateSpace = postFlagsSpace - DATE_COL;

    // there are more efficient ways to do below
    // (if it should become an issue)..

    for (long rn : allRns) {
      int ei = Collections.binarySearch(entryRns, rn);
      int ti = Collections.binarySearch(trailRns, rn);

      String e, t;
      e = ei < 0 ? null : ENT_TAG;
      t = ti < 0 ? null : TRL_TAG;

      String date;
      if (t != null)
        date = new Date(pack.crumTrail(rn).crum().utc()).toString();
      else
        date = null;

      if (e != null) {

        SourceRow srcRow = pack.getSourceByRowNumber(rn);
        
        String srcDesc = srcRow.toString(" ", RED_TOKEN);

        if (date == null) {

          int spaceAvail = postFlagsSpace;
          if (srcDesc.length() > spaceAvail)
            srcDesc = srcDesc.substring(0, spaceAvail - 2) + "..";
          table.printRow(rn, null, e, t, srcDesc);

        } else  {

          int spaceAvail = postDateSpace - date.length();
          if (srcDesc.length() > spaceAvail)
            srcDesc = srcDesc.substring(0, spaceAvail - 2) + "..";
          
          table.printRow(rn, null, e, t, date, srcDesc);

        }


      } else  // e == null
        table.printRow(rn, null, e, t, date);
    }

    table.println();
    int rows = allRns.size();
    int ents = entryRns.size();
    int trails = trailRns.size();
    table.println(
        rows + pluralize(" row", rows) + "; " + trails + pluralize(" crumtrail", trails) + "; " +
        ents + pluralize(" source-row", ents) + ".");
    table.println();
  }
  
  
  
  
  
  private final static String SUM = "sum";
  private final static String STATE = "state";
  private final static String LIST = "list";
  private final static String HISTORY = "history";
  private final static String ENTRY = "entry";
  private final static String MERGE = "merge";
  
  // named values (name=value)
  
  private final static String SAVE = "save";
  private final static String SEP = "sep";
  
  private final static Option JSON_OPT = new Option("json");
  private final static Option SLIM_OPT = new Option("slim");

}













