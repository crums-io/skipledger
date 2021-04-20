/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cli;


import static io.crums.sldg.SldgConstants.MRSL_EXT;
import static io.crums.util.Strings.pluralize;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.io.Opening;
import io.crums.io.channels.ChannelUtils;
import io.crums.model.Beacon;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.SldgException;
import io.crums.sldg.db.ByteFormatException;
import io.crums.sldg.db.MorselFile;
import io.crums.sldg.entry.Entry;
import io.crums.sldg.packs.MorselPack;
import io.crums.util.IntegralStrings;
import io.crums.util.Lists;
import io.crums.util.Sets;
import io.crums.util.Strings;
import io.crums.util.Tuple;
import io.crums.util.main.ArgList;
import io.crums.util.main.MainTemplate;
import io.crums.util.main.PrintSupport;
import io.crums.util.main.StdExit;
import io.crums.util.main.TablePrint;

/**
 * 
 */
public class Mrsl extends MainTemplate {
  
  private String command;
  private File morselFile;
  
//  private String listOpts;
  private List<Long> rowNumbers;
  private File saveFile;
  private String sep = "\n";
  
  private Mrsl() {
  }
  
  
  public static void main(String[] args) {
    new Mrsl().doMain(args);
  }

  @Override
  protected void init(String[] args) throws IllegalArgumentException, IOException {
    ArgList argList = newArgList(args);
    
    this.command = argList.removeCommand(SUM, STATE, LIST, ENTRY);
    this.morselFile = argList.removeExistingFile();
    switch (command) {
    case SUM:
    case STATE:
    case LIST:
      break;
    case ENTRY:
      this.rowNumbers = argList.removeNumbers(true);
      if (rowNumbers.isEmpty())
        throw new IllegalArgumentException("missing entry row numbers");
      String filepath = argList.removeValue(FILE);
      if (filepath != null) {
        saveFile = new File(filepath);
        File parent = saveFile.getParentFile();
        if (parent != null && !parent.isDirectory())
          throw new IllegalArgumentException("parent directory of given path does not exist: " + filepath);
      }
      break;
    default:
      throw new RuntimeException("assertion failed. command=" + command);
    }
    if (morselFile == null)
      throw new IllegalArgumentException("required path/to/morsel file missing");
    
    
    argList.enforceNoRemaining();
  }

  @Override
  protected void start() throws InterruptedException, IOException {
    try {
      
      startImpl();
      
    } catch (SldgException sx) {
      
      if (sx instanceof HashConflictException) {
        throw new SldgException("Hash conflict in morsel file " + morselFile, sx);
      
      } else if (sx instanceof ByteFormatException) {
        throw new SldgException("Illegal byte format in morsel file " + morselFile, sx);
      
      } else
        throw sx;
    }
  }
  
  
  private void startImpl() throws InterruptedException, IOException, HashConflictException {
    MorselFile morsel = new MorselFile(morselFile);
    switch (command) {
    case SUM:
      printSummary(morsel);
      break;
    case STATE:
      System.out.println(stateString(morsel.getMorselPack()));
      break;
    case LIST:
      list(morsel);
      break;
    case ENTRY:
      if (saveFile == null)
        printEntries(morsel);
      else
        saveEntries(morsel);
      break;
    default:
      throw new RuntimeException("assertion failure. command=" + command);
    }
  }
  


  private void saveEntries(MorselFile morsel) throws IOException {
    checkEntryNums(morsel);
    try (var ch = Opening.CREATE.openChannel(this.saveFile)) {

      MorselPack pack = morsel.getMorselPack();
      if (sep == null || sep.isEmpty()) {
        for (long rn : this.rowNumbers)
          ChannelUtils.writeRemaining(ch, pack.entry(rn).content());
      } else {
        ByteBuffer separator = Strings.utf8Buffer(sep);
        for (long rn : this.rowNumbers) {
          ChannelUtils.writeRemaining(ch, pack.entry(rn).content());
          ChannelUtils.writeRemaining(ch, separator.clear());
        }
      }
    }
    
    int count = rowNumbers.size();
    System.out.println(count + pluralize(" entry", count) + " written to " + saveFile);
  }
  
  
  
  private void checkEntryNums(MorselFile morsel) {
    var entryNums = entryNumSet(morsel.getMorselPack());
    if (!entryNums.containsAll(this.rowNumbers)) {
      throw new IllegalArgumentException("illegal / unknown entry numbers: " + rowNumbers);
    }
  }
  
  
  private SortedSet<Long> entryNumSet(MorselPack pack) {
    List<Long> rns = Lists.map(pack.availableEntries(), e -> e.rowNumber());
    return Sets.sortedSetView(rns);
  }
  


  private void printEntries(MorselFile morsel) {
    MorselPack pack = morsel.getMorselPack();
    
    TreeSet<Long> entryRns = new TreeSet<>(this.rowNumbers);
    entryRns.retainAll(entryNumSet(pack));
    
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
    
    var beacons = pack.beaconRows();
    
    final int bindexStart;
    int bindex;
    long nextBcnRn;
    
    {
      int index = Collections.binarySearch(Lists.map(beacons, b -> b.a), first);
      if (index < 0) {
        int insertIndex = -1 - index;
        if (insertIndex > 0) {
          bindex = insertIndex - 1;
          nextBcnRn = beacons.get(bindex).a;
        } else {
          bindex = insertIndex;
          nextBcnRn = bindex == beacons.size() ? Long.MAX_VALUE : beacons.get(bindex).a;
        }
      } else {
        throw failBothBeaconAndEntry(first).fillInStackTrace();
      }
      bindexStart = bindex;
    }
    
    
    var trailRns = pack.trailedRows();
    
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
      if (entryRn == nextBcnRn)
        throw failBothBeaconAndEntry(entryRn).fillInStackTrace();
      if (entryRn > nextWitRn && didFirst)
        printTrail(nextWitRn, pack, table);
      else if (entryRn > nextBcnRn)
        printBeacon(beacons.get(bindex), pack, table);
      
      if (entryRn >= nextWitRn)
        nextWitRn = ++tindex == trailRns.size() ? Long.MAX_VALUE : trailRns.get(tindex);
      if (entryRn >= nextBcnRn)
        nextBcnRn = ++bindex == beacons.size() ? Long.MAX_VALUE : beacons.get(bindex).a;
      
      printEntryText(entryRn, pack, table);
      
      didFirst = true;
    }
    
    if (tindex < trailRns.size())
      printTrail(trailRns.get(tindex++), pack, table);
    
    table.println();
    int count = entryRns.size();
    int bcns = bindex - bindexStart;
    int trails = tindex - tindexStart;
    table.println(
        count + pluralize(" entry", count) + ", " +
        bcns + pluralize(" beacon", bcns) + ", " +
        trails + pluralize(" crumtrail", trails));
  }

  
  private final static String ROOT_HASH = " ref hash:";
  
  private void printTrail(long witRn, MorselPack pack, TablePrint table) {
    
    var trail = pack.crumTrail(witRn);
    trail.verify(); // TODO: verify a pack already verifies so I don't have to do this
    
    Date date = new Date(trail.crum().utc());
    
    table.printRow(witRn, "created before " + date + "  (crumtrail)");
    table.printRow(ROOT_HASH,  IntegralStrings.toHex(trail.rootHash()) );
    table.printRow(" ref URL:",  trail.getRefUrl() );
  }


  private final static char PRINT_SEP = '-';
  private void printEntryText(long entryRn, MorselPack pack, TablePrint table) {
    
    Entry entry = pack.entry(entryRn);
    table.printRow(entryRn, "Entry (" + entry.contentSize() + " bytes). As text:");
    table.printHorizontalTableEdge(PRINT_SEP);
    table.println(Strings.utf8String(pack.entry(entryRn).content()));
    table.printHorizontalTableEdge(PRINT_SEP);
  }


  private ByteFormatException failBothBeaconAndEntry(long rn) {
    return new ByteFormatException("row " + rn + " purports to be both beacon and entry");
  }


  private void printBeacon(Tuple<Long, Long> beacon, MorselPack pack, TablePrint table) {
    Beacon bcn = new Beacon(pack.inputHash(beacon.a), beacon.b);
    table.printRow(beacon.a, "created after " + new Date(bcn.utc()) + "  (beacon)");
    table.printRow(" beacon:",  bcn.hashHex() );
    table.printRow(" ref URL:",  bcn.getRefUrl() );
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
        "Command line tool for reading and manipulating morsel files ('" + MRSL_EXT + "' ext). " +
        "Morsel files are tamper-proof tear-outs of rows from an opaque skip ledger paired with the " +
        "source of those rows (i.e. the content whose hash is the row's input-hash).";
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
        "Commands in the table below each take a morsel file as argument.";
    printer.printParagraph(paragraph);
    printer.println();

    TablePrint table = new TablePrint(out, LEFT_TBL_COL_WIDTH, RIGHT_TBAL_COL_WIDTH);
    table.setIndentation(INDENT);
    
    table.printRow(SUM, "prints a summary of the morsel's contents");
    table.println();
    
    table.printRow(STATE, "prints the highest row number and that row's hash");
    table.println();
    
    table.printRow(LIST, "lists the morsel's rows and entries");
    table.println();
    
    table.printRow(ENTRY, "lists in detail the given entries or saves them to a file");
    table.printRow(null,  "Arguments:");
    table.println();
    table.printRow(null,   "<line-numbers>  (required)");
    table.println();
    table.printRow(null,   "Strictly ascending line numbers separated by commas or spaces");
    table.printRow(null,   "Ranges (incl.) may be substituted for numbers. For example:");
    table.printRow(null,   "308,466,592-598,717");
    table.println();
    table.printRow(null, FILE + "=<path/to/file> (optional)");
    table.println();
    table.printRow(null,  "If provided, then entries are saved to the given file path in");
    table.printRow(null,  "the order given; otherwise lists the entries as text.");
    table.println();
    table.printRow(null, SEP + "=<separator> (optional)");
    table.println();
    table.printRow(null,  "If provided with " + FILE + "=<path/to/file>, then each entry is");
    table.printRow(null,  "is trailed with the <seperator> string. To set with no separator,");
    table.printRow(null,   "leave the value empty.");
    table.printRow(null,  "DEFAULT: " + SEP + "=\\n     (new line)");
    table.println();
  }
  
  


  private final static int LEFT_SUM_COL_WIDTH = 17;
  private final static int RIGHT_SUM_COL_WIDTH = 25;
  private final static int MID_SUM_COL_WIDTH = RM - LEFT_SUM_COL_WIDTH - RIGHT_SUM_COL_WIDTH;
  

  private void printSummary(MorselFile morsel) {
    MorselPack pack = morsel.getMorselPack();
    PrintStream out = System.out;
    out.println();
    out.println("<" + morsel.getFile().getName() + ">");
    out.println();
    
    TablePrint table = new TablePrint(LEFT_SUM_COL_WIDTH, MID_SUM_COL_WIDTH, RIGHT_SUM_COL_WIDTH);
    
    List<Long> rns = pack.getFullRowNumbers();
    var entries = pack.availableEntries();
    var beacons = pack.beaconRows();
    var trails = pack.trailedRows();
    
    final long hi = pack.hi();

    out.println("Rows:");
    table.printRow(" count:", rns.size());
    table.printRow(" # range:", "lo: " + pack.lo(), "hi: " + hi);
    table.printRow(" with entries:", entries.size());
    out.println();
    
    // !beacons.isEmpty() || !trails.isEmpty() == !(beacons.isEmpty() && trails.isEmpty())
    if (!beacons.isEmpty() || !trails.isEmpty()) {
      out.println("History:");
      if (!beacons.isEmpty()) {
        var firstBcn = beacons.get(0);
        table.printRow(" created after:", new Date(firstBcn.b), "(row " + firstBcn.a + ")");
      }
      
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
  
  
  private final static String BCN_TAG = "A";
  private final static String TRL_TAG = "W";
  private final static String ENT_TAG = "E";
  
  private final static int RN_PAD = 1;
  private final static int FLAG_PAD = 2;
  private final static int DATE_COL = 32;
  
  private void list(MorselFile morsel) {
    MorselPack pack = morsel.getMorselPack();
    
    var beacons = pack.beaconRows();
    var entryInfos = pack.availableEntries();
    List<Long> beaconRns = Lists.map(beacons, b -> b.a);
    List<Long> entryRns = Lists.map(entryInfos, e -> e.rowNumber());
    List<Long> trailRns = pack.trailedRows();
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
      int bi = Collections.binarySearch(beaconRns, rn);
      int ei = Collections.binarySearch(entryRns, rn);
      int ti = Collections.binarySearch(trailRns, rn);

      String b, e, t;
      b = bi < 0 ? null : BCN_TAG;
      e = ei < 0 ? null : ENT_TAG;
      t = ti < 0 ? null : TRL_TAG;

      String date;
      if (t != null)
        date = new Date(pack.crumTrail(rn).crum().utc()).toString();
      else if (b != null)
        date = new Date(beacons.get(bi).b).toString();
      else
        date = null;

      if (e != null) {

        Entry entry = pack.entry(rn);
        int size = entry.contentSize();
        String sizeString; 
        if (size / 1024 < 9)
          sizeString = size + " B |";
        else
          sizeString = (size / 1024) + " kB |";


        if (date == null) {

          String out = sizeString;
          int spaceAvail = postFlagsSpace - out.length();
          ByteBuffer raw = entry.content();
          if (raw.remaining() > spaceAvail)
            raw.limit(raw.position() + spaceAvail);
          out += Strings.utf8String(raw);
          table.printRow(rn, b, e, t, out);

        } else  {

          String out = sizeString;
          int spaceAvail = postDateSpace - out.length();
          ByteBuffer raw = entry.content();
          if (raw.remaining() > spaceAvail)
            raw.limit(raw.position() + spaceAvail);
          out += Strings.utf8String(raw);
          table.printRow(rn, b, e, t, date, out);

        }


      } else  // e == null
        table.printRow(rn, b, e, t, date);
    }

    table.println();
    int rows = allRns.size();
    int bcns = beaconRns.size();
    int ents = entryRns.size();
    int trails = trailRns.size();
    table.println(
        rows + pluralize(" row", rows) + "; " + bcns + pluralize(" beacon", bcns) +
        "; " + trails + pluralize(" crumtrail", trails) + "; " +
        ents + pluralize(" entry", ents) + ".");
    table.println();
  }
  
  
  
  
  
  private final static String SUM = "sum";
  private final static String STATE = "state";
  private final static String LIST = "list";
  private final static String ENTRY = "entry";
  
  // named values (name=value)
  
  private final static String FILE = "save";
  private final static String SEP = "sep";
  
  // list options
  
  // scrapping this for now (?)
//  /**
//   * Beacon.
//   */
//  private final static char B = 'b';
//  /**
//   * Entry.
//   */
//  private final static char E = 'e';
//  /**
//   * Numbers.
//   */
//  private final static char N = 'n';
//  /**
//   * Hash.
//   */
//  private final static char S = 's';
//  /**
//   * Hash.
//   */
//  private final static char C = 'c';
//  
//  
//  private final static String LIST_OPTS = new String(new char[] { B, E, N, S, });
  
  

}













