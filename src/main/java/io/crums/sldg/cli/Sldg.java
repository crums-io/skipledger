/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.cli;


import static io.crums.util.main.Args.*;
import static io.crums.util.Strings.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.json.simple.JSONObject;

import io.crums.io.Opening;
import io.crums.model.CrumTrail;
import io.crums.sldg.Path;
import io.crums.sldg.Row;
import io.crums.sldg.SkipPath;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.SldgException;
import io.crums.sldg.db.Db;
import io.crums.sldg.db.Finder;
import io.crums.sldg.db.Format;
import io.crums.sldg.db.VersionedSerializers;
import io.crums.sldg.json.NuggetParser;
import io.crums.sldg.json.PathParser;
import io.crums.sldg.scraps.Nugget;
import io.crums.util.IntegralStrings;
import io.crums.util.Lists;
import io.crums.util.TaskStack;
import io.crums.util.main.ArgList;
import io.crums.util.main.MainTemplate;
import io.crums.util.main.PrintSupport;
import io.crums.util.main.StdExit;
import io.crums.util.main.TablePrint;

/**
 * Manages a {@linkplain Db} from the command line.
 */
public class Sldg extends MainTemplate {


  // to minimize/organize class members
  private static class IngestCommand {
    private int start = 1;
    private File file;
  }
  
  private static class WriteCommand {
    private boolean add;
    
    private List<ByteBuffer> entryHashes;
    
    private boolean addBeacon;
    
    private boolean witness;
    private int toothExponent;
    private boolean witnessLast;
    
    private IngestCommand ingest;
  }
  
  private final static int DEFAULT_LIMIT = 8;
  
  private static class FindCommand {
    private ByteBuffer prefix;
    private long startRn;
    private int limit = DEFAULT_LIMIT;
  }
  

  private static class ReadCommand {
    private boolean state;
    private boolean path;
    
    private boolean list;
    private List<Long> rowNumbers;
    
    private FindCommand find;
    
    private boolean nug;
    private boolean status;
    
    private File file;
    private Format format = Format.BINARY;
    private boolean enforceExt = true;
    
    boolean takesRowNumbers() {
      return path || list || nug;
    }
    
    boolean supportsFileOutput() {
      return state || path || nug;
    }
    
    
    boolean isPathResponse( ) {
      return state || path;
    }
    
    boolean toFile() {
      return file != null;
    }
  }
  
  
  
  private File dir;
  private Opening mode;
  
//  private String command;
  
  private WriteCommand writeCommand;
  
  private ReadCommand readCommand;
  
  
  private boolean info;

  private Db db;
  
  
  /**
   * Invoked by main.
   */
  private Sldg() {  }
  
  

  /**
   * @param args
   */
  public static void main(String[] args) {
    new Sldg().doMain(args);
  }

  @Override
  protected void init(String[] args) throws IllegalArgumentException, Exception {
    if (args.length == 0)
      throw new IllegalArgumentException("No arguments specified");
    
    ArgList argList = new ArgList(args);
    
    this.dir = new File(argList.removeRequiredValue(DIR));
    this.mode = getMode(argList);

    configureCommon(argList);
    
    boolean noCommand =
        !configureWriteCommands(argList) &&
        !configureReadCommands(argList);
    
    if (noCommand && mode != Opening.CREATE)
      throw new IllegalArgumentException("missing command");
    
    this.db = new Db(dir, mode, true);  // (lazy=true)
    
    
    
    if (mode == Opening.CREATE)
      System.out.println("ledger created at " + db.getDir());
    
    // check the maximum row number is not out-of-bounds
    else if (readCommand != null && readCommand.takesRowNumbers()) {
      
      long maxRowNumber = readCommand.rowNumbers.stream().max(Comparator.naturalOrder()).get();
      long max = db.size();
      if (maxRowNumber > max) {
        db.close();
        throw new IllegalArgumentException(
            "row number " + maxRowNumber + " > max row number in ledger (" + max + ")"); 
      }
    }
  }

  
  
  @Override
  protected void start() throws InterruptedException, Exception {
    
    try (Db db = this.db) {
      
      // write command
      if (writeCommand != null) {
        if (writeCommand.addBeacon) {
          System.out.print("beacon hash");
          long rowNumber = db.addBeacon();
          System.out.println(" entered in row [" + rowNumber + "]");
        }
        if (writeCommand.add) {
          writeCommand.entryHashes.forEach(h -> db.getLedger().appendRows(h));
          if (info) {
            int count = writeCommand.entryHashes.size();
            System.out.println(count + pluralize(" row", count) + " added");
          }
        }
        
        if (writeCommand.ingest != null)
          doIngest();
        
        if (writeCommand.witness) {
          Db.WitnessReport report =
              writeCommand.toothExponent == -1 ?
                  db.witness(writeCommand.witnessLast) :
                    db.witness(writeCommand.toothExponent, writeCommand.witnessLast);
          
          int count = report.getStored().size();
          String msg;
          if (info) {
            int witCount = report.getRecords().size();
            msg = witCount + pluralize(" row", witCount) + " witnessed; " + count + pluralize(" crumtrail", count) + " stored";
            
          } else
            msg = String.valueOf(count);
          System.out.println(msg);
        }
        

      // read command
      } else if (readCommand != null) {
        
        if (readCommand.state) {
          Path statePath = db.beaconedStatePath();
          if (readCommand.toFile())
            outputState(statePath);
          else
            printPath(statePath);
        
        } else if (readCommand.nug) {
          long rowNumber = readCommand.rowNumbers.get(0);
          Optional<Nugget> nuggetOpt = db.getNugget(rowNumber);
          
          if (readCommand.toFile())
            outputNug(nuggetOpt);
          else
            printNugget(nuggetOpt);
        
        } else if (readCommand.path) {
          
          long lo, hi;
          {
            long one = readCommand.rowNumbers.get(0);
            long another = readCommand.rowNumbers.get(1);
            if (one < another) {
              lo = one;
              hi = another;
            } else {
              lo = another;
              hi = one;
            }
          }
          SkipPath path = db.getLedger().skipPath(lo, hi);
          
          if (readCommand.toFile())
            outputPath(path);
          else
            printPath(path);
          
        } else if (readCommand.list) {
          List<Row> rows = new ArrayList<>(readCommand.rowNumbers.size());
          for (long rowNumber : readCommand.rowNumbers)
            rows.add(db.getLedger().getRow(rowNumber));
          listRows(rows);
          int count = rows.size();
          System.out.println(count + pluralize(" row", count) + ".");
        
        } else if (readCommand.status) {
          printStatus();
          
        } else if (readCommand.find != null) {
          Finder finder = new Finder(db.getLedger());
          List<Row> rows =finder.byEntryPrefix(
              readCommand.find.prefix,
              readCommand.find.startRn,
              readCommand.find.limit);
          listRows(rows);
          int count = rows.size();
          System.out.print(count + pluralize(" row", count) + " found.");
          if (count == readCommand.find.limit)
            System.out.println(" (limit=" + count + ")");
          else
            System.out.println();
        }
      }
    }
  }
  
  
  private void doIngest() throws IOException {
    
    try (TaskStack closer = new TaskStack(this)) {
      Reader reader;
      File file = writeCommand.ingest.file;
      if (file == null)
        reader = new BufferedReader(new InputStreamReader(System.in));
      else
        reader = new BufferedReader(new FileReader(writeCommand.ingest.file));
      closer.pushClose(reader);
      
      StringBuilder buffer = new StringBuilder(128);
      int count = 0;
      final int start = writeCommand.ingest.start;
      while (true) {
        
        int b = reader.read();
        if (b == -1)
          break;
        
        char c = (char) b;
        if (isWhitespace(c)) {
          int len = buffer.length();
          if (len == 0)
            continue;
          if (len != SldgConstants.HASH_WIDTH * 2)
            throw new IllegalArgumentException(
                "on parsing " + nTh(count + 1) + " hex entry");
          byte[] entry = IntegralStrings.hexToBytes(buffer);
          buffer.setLength(0);
          ++count;
          if (count >= start)
            db.getLedger().appendRows(ByteBuffer.wrap(entry));
          
        } else {
          buffer.append(c);
        }
      }
      int skipped = Math.min(count, start - 1);
      int added = count - skipped;
      System.out.println(
          added + pluralize(" entry", added) + " added; " + skipped + pluralize(" entry", skipped) + " skipped");
      long size = db.size();
      System.out.println("Final ledger size: " + size + pluralize(" row", size));
    }
  }
  
  
  private boolean isWhitespace(char c) {
    switch (c) {
    case '\t':
    case '\n':
    case '\f':
    case '\r':
    case ' ':
      return true;
    default:
      return false;
    }
  }



  private void outputPath(SkipPath path) {
    
    File out;
    if (readCommand.file.isDirectory()) {
      String file = FilenamingConvention.INSTANCE.pathFilename(path, readCommand.format);
      out = new File(readCommand.file, file);
      exitIfExists(out);
    } else
      out = readCommand.file;
    
    VersionedSerializers.PATH_SERIALIZER.write(path, out, readCommand.format);
    System.out.println("path written to " + out);
  }



  private void outputNug(Optional<Nugget> nuggetOpt) {
    
    if (nuggetOpt.isEmpty()) {
      System.err.println("[ERROR] untrailed row (no crumtrail yet for this or later row number)");
      StdExit.ILLEGAL_ARG.exit();
      return;
    }
    
    Nugget nugget = nuggetOpt.get();
    
    File out;
    if (readCommand.file.isDirectory()) {
      String file = FilenamingConvention.INSTANCE.nuggetFilename(nugget, readCommand.format);
      out = new File(readCommand.file, file);
      exitIfExists(out);
    } else
      out = readCommand.file;
    
    VersionedSerializers.NUGGET_SERIALIZER.write(nugget, out, readCommand.format);
    System.out.println("nugget written to " + out);
  }
  
  
  
  private void exitIfExists(File out) {
    if (!out.exists())
      return;
    
    System.err.println("[ERROR] file already exists: " + out);
    StdExit.ILLEGAL_ARG.exit();
  }



  private void outputState(Path statePath) {
    
    if (statePath == null) {
      System.err.println("[ERROR] empty ledger has no state");
      StdExit.ILLEGAL_ARG.exit();
    }
    
    File out;
    if (readCommand.file.isDirectory()) {
      String file = FilenamingConvention.INSTANCE.stateFilename(statePath, readCommand.format);
      out = new File(readCommand.file, file);
      exitIfExists(out);
    } else
      out = readCommand.file;
    
    VersionedSerializers.PATH_SERIALIZER.write(statePath, out, readCommand.format);
    System.out.println("state-path written to " + out);
  }
  
  
  



  
  private void printStatus() {
    long size = db.size();
    List<Long> witnessed = db.getRowNumbersWitnessed();
    int witCount = witnessed.size();
    int bnCount = db.getBeaconCount();
    DecimalFormat format = new DecimalFormat("#,###.###");
    System.out.println(
        format.format(size) + pluralize(" row", size) + ", " +
        format.format(bnCount) + pluralize(" of which are beacon", bnCount) + "; " +
        format.format(witCount) + pluralize(" crumtrail", witCount) + " attached");

    if (bnCount != 0) {
      List<Long> bnRowNums = db.getBeaconRowNumbers();
      List<Long> bnUtcs = db.getBeaconUtcs();
      long bnRowNum = bnRowNums.get(0);
      long bnUtc = bnUtcs.get(0);
      System.out.println("row [" + format.format(bnRowNum) + "] created after " + new Date(bnUtc));
    }
    
    if (witCount == 1) {
      CrumTrail trail = db.getCrumTrailByIndex(0);
      long witnessedRow = witnessed.get(0);
      System.out.println(
          "first (and last) row witnessed: [" + format.format(witnessedRow) + "] " +
          new Date(trail.crum().utc()));
    } else if (witCount > 1) {
      long firstRow = witnessed.get(0);
      long lastRow = witnessed.get(witnessed.size() - 1);
      
      CrumTrail firstTrail = db.getCrumTrailByIndex(0);
      CrumTrail lastTrail = db.getCrumTrailByIndex(witnessed.size() - 1);
      
      if (firstTrail.crum().utc() > lastTrail.crum().utc())
        throw new SldgException("corrupt repo: " + db.getDir());
      
      System.out.println(
          "first row witnessed: [" + format.format(firstRow) + "] " + new Date(firstTrail.crum().utc()));
      System.out.println(
          "last row witnessed: [" + format.format(lastRow) + "] " + new Date(lastTrail.crum().utc()));
    }
    System.out.println("Ledger (last row's) Hash:");
    System.out.println(IntegralStrings.toHex(db.getLedger().rowHash(size)));
    System.out.println("OK");
  }



  private void printNugget(Optional<Nugget> nuggetOpt) {
    String output =
        nuggetOpt.isEmpty() ?
            "{}" :
              injectVersion(NuggetParser.INSTANCE.toJsonObject(nuggetOpt.get())).toString();
    
    System.out.println(output);
  }



  private void listRows(List<Row> rows) {
//  System.out.println(RowParser.INSTANCE.toJsonArray(rows));
    if (!rows.isEmpty()) {
      
      // make it pretty: align hex column
      final int hexStartPos;
      {
        long maxRowNum =
            rows.stream().max((a, b) -> Long.compare(a.rowNumber(), b.rowNumber()))
            .get().rowNumber();
        String proto = String.valueOf(maxRowNum);
        // >[proto] <
        hexStartPos = proto.length() + 3;
      }

      for (Row row : rows) {
        System.out.print('[');
        String value = String.valueOf(row.rowNumber());
        System.out.print(value);
        System.out.print(']');
        for (int index = value.length() + 2; index < hexStartPos; ++index)
          System.out.print(' ');
        System.out.println(IntegralStrings.toHex(row.hash()));
      }
    }
  }



  private void printPath(Path path) {
    if (path == null)
      System.out.println("{}");
    else
      System.out.println(injectVersion(PathParser.INSTANCE.toJsonObject(path)));
  }
  
  
  @SuppressWarnings("unchecked")
  private JSONObject injectVersion(JSONObject obj) {
    obj.put(SldgConstants.VERSION_TAG, SldgConstants.VERSION);
    return obj;
  }
  
  
  
  
  
  private void configureCommon(ArgList argList) {
    this.info = argList.removeBoolean(INFO, true);
  }



  private boolean configureReadCommands(ArgList argList) {
    
    List<String> readCommands = argList.removeContained(STATE, PATH, LIST, FIND, NUG, STATUS);
    switch (readCommands.size()) {
    case 0:   return false;
    case 1:   break;
    default:
      throw new IllegalArgumentException(
          "duplicate commands in '" + argList.getArgString() + "'");
    }
    
    this.readCommand = new ReadCommand();
    String command = readCommands.get(0);
    readCommand.state = STATE.equals(command);
    readCommand.path = PATH.equals(command);
    readCommand.list = LIST.equals(command);
    readCommand.nug = NUG.equals(command);
    readCommand.status = STATUS.equals(command);
    
    if (FIND.equals(command)) {
      readCommand.find = new FindCommand();
      
      readCommand.find.prefix = getHashPrefix(argList);
      
      readCommand.find.limit = (int) argList.removeLong(LIMIT, DEFAULT_LIMIT);
      if (readCommand.find.limit < 1)
        throw new IllegalArgumentException(LIMIT + "=" + readCommand.find.limit + " < 1");
      
      readCommand.find.startRn = argList.removeLong(START, 1);
    }
    
    if (readCommand.takesRowNumbers()) {
      readCommand.rowNumbers = argList.removeNumbers();
      if (readCommand.rowNumbers.isEmpty())
        throw new IllegalArgumentException(
            "missing row number[s] in arguments: " + argList.getArgString());
      if (readCommand.path && readCommand.rowNumbers.size() != 2)
        throw new IllegalArgumentException(
            (readCommand.rowNumbers.size() > 2 ? "too many row numbers" : "missing row number") +
            " in arguments: " + argList.getArgString());
      
      long minRowNum = readCommand.rowNumbers.stream().min(Comparator.naturalOrder()).get();
      if (minRowNum < 1)
        throw new IllegalArgumentException("one or more row numbers < 1: " + argList.getArgString());
      
      if (readCommand.nug && readCommand.rowNumbers.size() != 1)
        throw new IllegalArgumentException(
            NUG + " command takes a single row number. Too many given: " + argList.getArgString());
    }
    

    configureOutput(argList);
    
    argList.enforceNoRemaining();
    
    return true;
  }
  
  
  
  private void configureOutput(ArgList argList) {

    String outpath = argList.removeValue(FILE);
    if (outpath == null)
      return;

    if (!readCommand.supportsFileOutput())
      throw new IllegalArgumentException(
          FILE + "=" + outpath + " does not apply: " + argList.getArgString());
    
    readCommand.file = new File(outpath);
    
    // set the format
    String fmt = argList.removeValue(FMT, BINARY);
    if (BINARY.equals(fmt))
      readCommand.format = Format.BINARY;
    else if (JSON.equalsIgnoreCase(fmt))
      readCommand.format = Format.JSON;
    else
      throw new IllegalArgumentException(
          "illegal setting " + FMT + "=" + fmt + " in arg list: " + argList.getArgString());
    
    readCommand.enforceExt = argList.removeBoolean(EXT, true);
    
    // if file is a directory (auto-naming), check other settings aren't wonkie
    if (readCommand.file.isDirectory()) {
      
      if (!readCommand.enforceExt)
        throw new IllegalArgumentException(
            "illegal setting " + EXT + "=false while file=" + outpath + " is a directory");
    
    // check we don't overwrite
    } else if (readCommand.file.exists()) {
      throw new IllegalArgumentException("file already exist: " + outpath);
    
    } else {
      // not a directory; and doesn't exist
      
      // check the parent directory exists
      File parent = readCommand.file.getParentFile();
      if (parent != null && !parent.exists())
        throw new IllegalArgumentException(
            "'" + FILE + "=" + outpath + "': first create parent dir " + parent);
      
      // normalize the filename, if it requires an extension
      if (readCommand.enforceExt) {
        String name = readCommand.file.getName();
        String normalizedName;
        if (readCommand.isPathResponse())
          normalizedName = FilenamingConvention.INSTANCE.normalizePathFilename(name, readCommand.format);
        else
          normalizedName = FilenamingConvention.INSTANCE.normalizeNuggetFilename(name, readCommand.format);
        if (!normalizedName.equals(name)) {
          readCommand.file = new File(parent, normalizedName);
          if (readCommand.file.exists())
            throw new IllegalArgumentException(
                "normalized file path " + readCommand.file + " already exists");
        }
      }
    }
  }
  
  
  
  private boolean configureWriteCommands(ArgList argList) {

    List<String> writeCommands = argList.removeContained(ADD, ADDB, WIT, INGEST);
    switch (writeCommands.size()) {
    case 0:
      return false;
    case 1:
    case 2:
    case 3:
      break;
    default:
      throw new IllegalArgumentException(
          "duplicate commands in '" + argList.getArgString() + "'");
    }
    
    this.writeCommand = new WriteCommand();
    writeCommand.add = writeCommands.contains(ADD);
    writeCommand.addBeacon = writeCommands.contains(ADDB);
    writeCommand.witness = writeCommands.contains(WIT);
    
    if (writeCommands.contains(INGEST)) {
      if (writeCommands.size() > 1)
        throw new IllegalArgumentException(INGEST + " cannot be combined with other commands: " + writeCommands);
      
      this.writeCommand.ingest = new IngestCommand();
      List<File> file = argList.removeExistingFiles();
      switch (file.size()) {
      case 0:
        break;
//        throw new IllegalArgumentException(
//            "missing or non-existent file in arguments: " + argList.getArgString());
      case 1:
        writeCommand.ingest.file = file.get(0);
        break;
      default:
        throw new IllegalArgumentException(INGEST + " command takes only a single file: " + file);
      }
      
      writeCommand.ingest.start = (int) argList.removeLong(START, 1);
      if (writeCommand.ingest.start < 1)
        throw new IllegalArgumentException(START + "=" + writeCommand.ingest.start + " < 1");
    }
    
    if (writeCommand.add) {
      writeCommand.entryHashes = getHashes(argList);
      if (writeCommand.entryHashes.isEmpty())
        throw new IllegalArgumentException(
            "missing entry hashes for '" + ADD + "' command: " + argList.getArgString());
    }
    
    
    if (writeCommand.witness) {
      long e = argList.removeLong(TEX, Long.MIN_VALUE);
      if (e == Long.MIN_VALUE) {
        writeCommand.toothExponent = -1;
      } else {
        if (e < 0 || e > SldgConstants.MAX_WITNESS_EXPONENT)
          throw new IllegalArgumentException("out of bounds " + TEX + "=" + e);
        writeCommand.toothExponent = (int) e;
      }
      writeCommand.witnessLast = argList.removeBoolean(WSTATE, true);
    }
    
    argList.enforceNoRemaining();
    
    return true;
  }
  
  
  
  
  
  private ByteBuffer getHashPrefix(ArgList args) {
    
    List<String> prefix = args.removeMatched(s -> isHashPrefix(s));
    
    switch (prefix.size()) {
    case 0:
      throw new IllegalArgumentException("'" + FIND + "' command requires a search key (in hex)");
    case 1:
      break;
    default:
      throw new IllegalArgumentException("only one search key may be provided; parsed these: " + prefix);
    }
    
    return ByteBuffer.wrap(IntegralStrings.hexToBytes(prefix.get(0)));
  }
  
  
  private List<ByteBuffer> getHashes(ArgList args) {
    List<String> hexes = args.removeMatched(s -> isHash(s));
    return Lists.map(hexes, s -> ByteBuffer.wrap(IntegralStrings.hexToBytes(s)));
  }
  
  
  private boolean isHash(String arg) {
    return IntegralStrings.isHex(arg) && arg.length() == 2 * SldgConstants.HASH_WIDTH;
  }
  
  private boolean isHashPrefix(String arg) {
    int len = arg.length();
    return len > 1 && (len & 1) == 0 && len <= 2 * SldgConstants.HASH_WIDTH && IntegralStrings.isHex(arg);
  }
  
  
  
  

  // NOTE: this code may belong in Opening
  private Opening getMode(ArgList args) {
    String mi = args.removeValue(MODE, MODE_RW);
    String m = orderCharOptions(mi, MODE_CRW);
    if (MODE_R.equals(m))
      return Opening.READ_ONLY;
    else if (MODE_RW.equals(m))
      return Opening.READ_WRITE_IF_EXISTS;
    else if (MODE_CRW.equals(m))
      return Opening.CREATE_ON_DEMAND;
    else if (MODE_C.equals(m))
      return Opening.CREATE;
    else
      throw new IllegalArgumentException("illegal " + MODE + " parameter: " + mi);
  }
  
  
  
  private final static int RM = 80;
  private final static int INDENT = 1;


  @Override
  protected void printDescription() {
    PrintSupport printer = new PrintSupport();
    printer.setIndentation(INDENT);
    printer.println();
    System.out.println("DESCRIPTION:");
    printer.println(); // 105
    printer.printParagraph(
        "Command line tool for accessing and maintaining a skip ledger stored on the file system.", RM);
    printer.println();
    String paragraph =
        "A skip ledger is a tamper-proof, append-only list of SHA-256 hashes that are added by its " +
        "user[s]. Since hashes are opaque, a skip ledger itself conveys little information. " +
        "If paired with the actual object whose hash matches the entry (e) in a given row " +
        "however, then it is evidence the object belongs in the ledger at the advertised " +
        "row number. Ledgers also support maintaining the times they are modified by " +
        "calling out the SHA-256 hash of their latest row to the crums.io hash/witness " +
        "time service. (See https://crums.io/docs/rest.html )";
    printer.printParagraph(paragraph, RM);
    printer.println();
    System.out.println("Representations:");
    printer.println();
    paragraph =
        "Beside thru sharing it in its entirety, the state of a ledger can optionally be " +
        "advertised compactly. The most compact of all is to just advertise the hash of " +
        "the last row in the ledger. A more detailed, but still compact representation is " +
        "achieved by enumerating a list of rows whose SHA-256 hashpointers connect the " +
        "last (latest) row to the first. The number of rows in this list grows by the log " +
        "of the size of the ledger, so it's always compact. (See '" + STATE + "' command)";
    printer.printParagraph(paragraph, RM);
    printer.println();
    paragraph =
        "This same structure is also used to provide a compact, standalone proof that an item at a " +
        "specific row number is indeed inside the ledger. I.e. a list of rows that connect " +
        "the latest row to the row of interest. If the row (or any row after it) has been " +
        "witnessed, then the crumtrail witness evidence together with these rows can be " +
        "packaged as a \"nugget\". (See '" + NUG + "' command)";
    printer.printParagraph(paragraph, RM);
    printer.println();
    System.out.println("Row Age & Witness Evidence:");
    printer.println();
    paragraph =
        "A row's minimum age is established by storing a crumtrail of the row's hash. (The row's hash is not " +
        "exactly the user-input hash: it's the hash of that plus the row's hash pointers.) A crumtrail is a tamper-proof hash structure that leads to a unitary root hash that is published every minute or so " +
        "by crums.io and is also maintained at multiple 3rd party sites: it is evidence of witnessing a hash in a small window of time. " +
        "Since the hash of every row in a skip ledger is dependent on the hash of every row before it, " +
        "witnessing a given row number also means effectively witnessing all its predecessors.";
    printer.printParagraph(paragraph, RM);
    printer.println();
    paragraph =
        "The witnessing algorithm then only witnesses [the hashes of] monotonically increasing row numbers. " +
        "The default behavior when there are unwitnessed rows is to always witness the next few subsequent rows that can't be matched with an " +
        "already stored crumtrail as well as the last row. This is because crumtrails are not generated right away: they're typically generated " +
        "a few minutes after the service first witnesses a hash. By default, up to 9 unwitnessed rows are submitted: 8 " +
        "'evenly' spaced at row numbers that are multiples of a power of 2, and the last unwitnessed row. The " +
        "exponent of this power of 2 for witnessing is called tooth-exponent. (See '" + WIT + "' command)";
    printer.printParagraph(paragraph, RM);
    printer.println();
    paragraph =
        "In order to establish the maximum age of entries in a ledger a beacon hash entry may be added. This hash is just " +
        "the root of the latest Merkle tree published at crums.io every minute or so. Since it's value cannot be predicted in advance, and it comes with a UTC timestamp, " +
        "it can be used to establish \"freshness\". The recommended practice is to simplify the evolution of row numbers by either (i) add only a single beacon as the very first row, " +
        "or (ii) add beacons at row numbers that are always a multiple of some constant that is a power of 2.  (See '" + ADDB + "' command)";
    printer.printParagraph(paragraph, RM);
    
  }

  @Override
  protected void printUsage(PrintStream out) {
    PrintSupport printer = new PrintSupport(out);
    printer.setIndentation(INDENT);
    printer.println();
    out.println("USAGE:");
    printer.println();
    String paragraph =
        "Arguments that are specified as name/value pairs are designated in the form " +
        "'name=*' below, with '*' standing for user input. A required argument is marked '" + REQ + "' in the rightmost column; " +
        "one-of-many, required arguments are marked '" + REQ_CH + "'; '" + REQ_PLUS + "' satisfies either as a " +
        "required one-of-many, or may be grouped with adjacently documented " + REQ_PLUS + " commands.";
    printer.printParagraph(paragraph, RM);
    printer.println();
    

    TablePrint table, subTable, subWideKeyTable;
    { 
      int fcw = 8;
      int lcw = 2;
      int mcw = RM - fcw - lcw - INDENT;
      
      int subFcw = 7;
      int subLcw = RM - fcw - subFcw;
      
      int subWkFcw = 13;
      int subWkLcw = RM - fcw - subWkFcw;
      
      
      table = new TablePrint(out, fcw, mcw, lcw);
      subTable = new TablePrint(out, subFcw, subLcw);
      subWideKeyTable = new TablePrint(out, subWkFcw, subWkLcw);
      
      table.setIndentation(INDENT);
      subTable.setIndentation(fcw + INDENT);
      subWideKeyTable.setIndentation(fcw + INDENT);
    }

    table.printHorizontalTableEdge('=');
    out.println();
    
    table.printRow(DIR + "=*", "path to skip ledger directory", REQ);
    out.println();
    
    table.printRow(MODE + "=*", "mode with which DB is opened. Options are one of the following", OPT);
    table.printRow(null,        "(char order doesn't matter):", OPT);
    out.println();
    subTable.printRow(           MODE_R,   "read only");
    subTable.printRow(           MODE_RW,  "read/write existing (DEFAULT)");
    subTable.printRow(           MODE_CRW, "read/write, create if doesn't exist");
    subTable.printRow(           MODE_C,   "create a new DB (must not exist)");
    subTable.printRow(           null,     "(To create an empty DB provide no additional arguments.)");

    out.println();
    table.printHorizontalTableEdge('-');
    table.printlnCenteredSpread("ADD / WITNESS", 2);
    table.printHorizontalTableEdge('-');
    out.println();
    
    table.printRow(ADD ,  "adds one or more hexidecimal SHA-256 hash entries in the order", REQ_PLUS);
    table.printRow(null,  "entered", null);
    out.println();
    
    table.printRow(ADDB , "adds the latest beacon hash as the next SHA-256 entry", REQ_PLUS);
    table.printRow(null,  "Establishes how new (!) subsequent rows in the ledger are.", null);
    table.printRow(null,  "If combined with other commands, then this command executes", null);
    table.printRow(null,  "first.", null);
    out.println();
    
    table.printRow(WIT ,  "witnesses the last row and/or previous unwitnessed rows whose", REQ_PLUS);
    table.printRow(null,  "numbers match the tooth-exponent", null);
    table.printRow(null,  "Establishes how old the latest rows in the ledger are.", null);
    table.printRow(null,  "If combined with other commands, then this command executes", null);
    table.printRow(null,  "last.", null);
    table.printRow(null,  "Options (inclusive):", null);
    out.println();
    subWideKeyTable.printRow(TEX + "=*", "witness numbered rows that are multiples of");
    subWideKeyTable.printRow(null,       "2 raised to the power of this number.");
    subWideKeyTable.printRow(null,       "Valid range: [0,62]");
    subWideKeyTable.printRow(null,       "DEFAULT: dynamically computed value that generates");
    subWideKeyTable.printRow(null,       "up to 8 tooth-included rows to witness");
    out.println();
    subWideKeyTable.printRow(WSTATE + "=true",  "witness last row, even if its number is not toothed");
    subWideKeyTable.printRow(null,              "Valid values: 'true' or 'false'");
    subWideKeyTable.printRow(null,              "DEFAULT: true");
    out.println();
    
    table.printRow(INGEST, "adds hexidecimal SHA-256 hash entries from stdin or the given file", REQ_CH);
    table.printRow(null,   "Entries must be whitespace-delimited. The recommended practice", null);
    table.printRow(null,   "is one per line. Note this is a streaming operation: if a bad", null);
    table.printRow(null,   "argument is encountered midway in the file previous rows may", null);
    table.printRow(null,   "already have been added.", null);
    table.printRow(null,   "Option:", null);
    out.println();
    subWideKeyTable.printRow(START + "=*", "starting from this entry number in the file");
    subWideKeyTable.printRow(null,         "I.e. if " + START + "=N, then the first N-1 entries in the file");
    subWideKeyTable.printRow(null,         "are skipped. (Use for incremental updates and tooling.)");
    subWideKeyTable.printRow(null,         "DEFAULT: 1");
    out.println();


    out.println();
    table.printHorizontalTableEdge('-');
    table.printlnCenteredSpread("OUTPUT / PRINT", 2);
    table.printHorizontalTableEdge('-');
    out.println();

    table.printRow(STATE ,      "prints or outputs abbreviated evidence of the state the ledger by", REQ_CH);
    table.printRow(null,        "outputing the shortest path of rows that connnect to the first row", null);
    table.printRow(null,        "from the last thru the rows' hashpointers", null);
    out.println();
    table.printRow(LIST ,       "prints the given numbered rows", REQ_CH);
    out.println();
    table.printRow(FIND,        "finds and prints rows whose entry hash (e) match the given", REQ_CH);
    table.printRow(null,        "hexadecimal prefix (must have an even number of hex digits)", null);
    table.printRow(null,        "Options (inclusive):", null);
    out.println();
    subWideKeyTable.printRow(START + "=*", "starting row number to begin the search");
    subWideKeyTable.printRow(null,         "DEFAULT: 1");
    out.println();
    subWideKeyTable.printRow(LIMIT + "=*", "maximum number of rows returned");
    subWideKeyTable.printRow(null,         "DEFAULT: " + DEFAULT_LIMIT);
    out.println();
    table.printRow(PATH ,       "prints or outputs the shortest path connecting the given pair of", REQ_CH);
    table.printRow(null,        "numbered rows", null);
    out.println();
    table.printRow(NUG,         "prints or outputs a nugget for the given row number argument.", REQ_CH);
    table.printRow(null,        "A nugget proves that the entry (input) hash at a given row number is", null);
    table.printRow(null,        "linked from a row at a higher number (highest, at the time published)", null);
    table.printRow(null,        "in the ledger. It may also contains evidence seting the row's minimum", null);
    table.printRow(null,        "age.", null);
    out.println();
    table.printRow(STATUS,      "prints the status of the ledger", REQ_CH);
    table.println();
    table.printHorizontalTableEdge('-');
    table.printlnCenteredSpread("Output Options:", 1);
    table.printHorizontalTableEdge('-');
    table.println();
    table.printRow(FILE + "=*", "outputs to the specified filepath", null);
    table.printRow(null,        "If the given path is an existing directory (recommended!), then the", null);
    table.printRow(null,        "filename is dynamically generated. Otherwise, unless overriden (see", null);
    table.printRow(null,        "next), if the pathname doesn't already sport the standard extension,", null);
    table.printRow(null,        "the extension is appended.", null);
    table.println();
    table.printRow(EXT + "=*",  "if 'false', then no extension is appended to the given '" + FILE + "'", null);
    table.printRow(null,        "(Valid only if '" + FILE + "' is not a directory)", null);
    table.printRow(null,        "Valid values: 'true' or 'false'", null);
    table.printRow(null,        "DEFAULT: true", null);
    table.println();
    table.printRow(FMT + "=*",  "sets the format for the file output", null);
    table.printRow(null,        "Valid values: '" + JSON + "' or '" + BINARY + "'", null);
    table.printRow(null,        "DEFAULT: " + BINARY, null);
    table.println();
    
  }


  private final static String REQ = "R";
  private final static String REQ_CH = "R?";
  private final static String REQ_PLUS = "R+";
  private final static String OPT = "";
  

  private final static String DIR = "dir";
  private final static String MODE = "mode";
  
  private final static String MODE_R = "r";
  private final static String MODE_RW = "rw";
  private final static String MODE_CRW = "crw";
  private final static String MODE_C = "c";
  
  
  


  private final static String ADD = "add";
  private final static String ADDB = "addb";
  private final static String INGEST = "ingest";
  
  private final static String WIT = "wit";
  private final static String TEX = "tex";
  private final static String WSTATE = "wstate";
  

  private final static String STATE = "state";
  private final static String PATH = "path";
  private final static String STATUS = "status";
  private final static String LIST = "list";
  private final static String FIND = "find";
  private final static String NUG = "nug";
  
  

  private final static String INFO = "info";
  private final static String FILE = "file";
  private final static String EXT = "ext";
  private final static String FMT = "fmt";
  private final static String JSON = "json";
  private final static String BINARY = "binary";
  private final static String LIMIT = "limit";
  private final static String START = "start";
  
  
  
  public final static String PATH_EXT = ".spath";
  public final static String STATE_EXT = "." + STATE + PATH_EXT;
  public final static String NUG_EXT = "." + NUG;
  
  
  
  
  
}
