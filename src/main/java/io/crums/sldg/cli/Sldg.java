/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.cli;


import static io.crums.util.main.Args.*;

import java.io.File;
import java.io.PrintStream;
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
import io.crums.sldg.Nugget;
import io.crums.sldg.Path;
import io.crums.sldg.Row;
import io.crums.sldg.SkipPath;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.SldgException;
import io.crums.sldg.db.Db;
import io.crums.sldg.db.Format;
import io.crums.sldg.db.VersionedSerializers;
import io.crums.sldg.json.NuggetParser;
import io.crums.sldg.json.PathParser;
import io.crums.sldg.json.RowParser;
import io.crums.util.IntegralStrings;
import io.crums.util.Lists;
import io.crums.util.main.ArgList;
import io.crums.util.main.MainTemplate;
import io.crums.util.main.PrintSupport;
import io.crums.util.main.TablePrint;

/**
 * 
 */
public class Sldg extends MainTemplate {

  
  // to minimize/organize class members
  private static class WriteCommand {
    private boolean add;
    
    private List<ByteBuffer> entryHashes;
    
    private boolean addBeacon;
    
    private boolean witness;
    private int toothExponent;
    private boolean witnessLast;
  }
  

  // to minimize/organize class members
  private static class ReadCommand {
    private boolean state;
    private boolean path;
    
    private boolean list;
    private List<Long> rowNumbers;
    
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
    
    this.dir = new File(argList.popRequiredValue(DIR));
    this.mode = getMode(argList);

    configureCommon(argList);
    
    boolean noCommand =
        !configureWriteCommands(argList) &&
        !configureReadCommands(argList);
    
    if (noCommand && mode != Opening.CREATE)
      throw new IllegalArgumentException("missing command");
    
    this.db = new Db(dir, mode);
    
    
    
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
          SkipPath statePath = db.getLedger().statePath();
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
          printRows(rows);
        
        } else if (readCommand.status) {
          printStatus();
        }
      }
    }
  }
  
  
  private void outputPath(SkipPath path) {
    
    File out;
    if (readCommand.file.isDirectory()) {
      String file = FilenamingConvention.INSTANCE.pathFilename(path, readCommand.format);
      out = new File(readCommand.file, file);
      if (out.exists()) {
        System.err.println("[ERROR] file already exists: " + out);
        return;
      }
    } else
      out = readCommand.file;
    
    VersionedSerializers.PATH_SERIALIZER.write(path, out, readCommand.format);
    System.out.println("path written to " + out);
  }



  private void outputNug(Optional<Nugget> nuggetOpt) {
    
    if (nuggetOpt.isEmpty()) {
      System.err.println("[ERROR] row as yet untrailed (no crumtrail)");
      return;
    }
    
    Nugget nugget = nuggetOpt.get();
    
    File out;
    if (readCommand.file.isDirectory()) {
      String file = FilenamingConvention.INSTANCE.nuggetFilename(nugget, readCommand.format);
      out = new File(readCommand.file, file);
    } else
      out = readCommand.file;
    
    VersionedSerializers.NUGGET_SERIALIZER.write(nugget, out, readCommand.format);
    System.out.println("nugget written to " + out);
  }



  private void outputState(SkipPath statePath) {
    
    File out;
    if (readCommand.file.isDirectory()) {
      String file = FilenamingConvention.INSTANCE.stateFilename(statePath, readCommand.format);
      out = new File(readCommand.file, file);
    } else
      out = readCommand.file;
    
    VersionedSerializers.PATH_SERIALIZER.write(statePath, out, readCommand.format);
    System.out.println("state-path written to " + out);
  }
  
  
  



  
  private void printStatus() {
    long size = db.size();
    List<Long> witnessed = db.getRowNumbersWitnessed();
    int witCount = witnessed.size();
    DecimalFormat format = new DecimalFormat("#,###.###");
    System.out.println(
        format.format(size) + pluralize(" row", size) + "; " +
        format.format(witCount) + pluralize(" row", witCount) + " witnessed");
    if (witCount == 1) {
      CrumTrail trail = db.getCrumTrail(0);
      long witnessedRow = witnessed.get(0);
      System.out.println(
          "first (and last) row witnessed: [" + format.format(witnessedRow) + "] " +
          new Date(trail.crum().utc()));
    } else if (witCount > 1) {
      long firstRow = witnessed.get(0);
      long lastRow = witnessed.get(witnessed.size() - 1);
      
      CrumTrail firstTrail = db.getCrumTrail(0);
      CrumTrail lastTrail = db.getCrumTrail(witnessed.size() - 1);
      
      if (firstTrail.crum().utc() > lastTrail.crum().utc())
        throw new SldgException("corrupt repo: " + db.getDir());
      
      System.out.println(
          "first row witnessed: [" + format.format(firstRow) + "] " + new Date(firstTrail.crum().utc()));
      System.out.println(
          "last row witnessed: [" + format.format(lastRow) + "] " + new Date(lastTrail.crum().utc()));
    }
    System.out.println("OK");
  }



  private void printNugget(Optional<Nugget> nuggetOpt) {
    String output =
        nuggetOpt.isEmpty() ?
            "{}" :
              injectVersion(NuggetParser.INSTANCE.toJsonObject(nuggetOpt.get())).toString();
    
    System.out.println(output);
  }



  private void printRows(List<Row> rows) {
    System.out.println(RowParser.INSTANCE.toJsonArray(rows));
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
    this.info = argList.popBoolean(INFO, true);
  }
  
  
  private String pluralize(String single, long count) {
    return count == 1 ? single : single + "s";
  }



  private boolean configureReadCommands(ArgList argList) {
    
    List<String> readCommands = argList.removedContained(STATE, PATH, LIST, NUG, STATUS);
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
    
    if (readCommand.takesRowNumbers()) {
      readCommand.rowNumbers = argList.popNumbers();
      if (readCommand.rowNumbers.isEmpty())
        throw new IllegalArgumentException(
            "missing row number[s] in arguments: " + argList.getArgString());
      if (readCommand.path && readCommand.rowNumbers.size() != 2)
        throw new IllegalArgumentException(
            (readCommand.rowNumbers.size() > 2 ? "too many" : "missing") +
            " row numbers in arguments: " + argList.getArgString());
      
      long minRowNum = readCommand.rowNumbers.stream().min(Comparator.naturalOrder()).get();
      if (minRowNum < 1)
        throw new IllegalArgumentException("one or more row numbers < 1: " + argList.getArgString());
      
      if (readCommand.nug && readCommand.rowNumbers.size() != 1)
        throw new IllegalArgumentException(
            NUG + " command takes a single row number. Too many given: " + argList.getArgString());
      
      String outpath = argList.popValue(FILE);
      if (outpath != null) {
        // 
        if (!readCommand.supportsFileOutput())
          throw new IllegalArgumentException(
              FILE + "=" + outpath + " does not apply to '" + command + "'");
        readCommand.file = new File(outpath);
        String fmt = argList.popValue(FMT, BINARY);
        if (BINARY.equals(fmt))
          readCommand.format = Format.BINARY;
        else if (JSON.equalsIgnoreCase(fmt))
          readCommand.format = Format.JSON;
        else
          throw new IllegalArgumentException(
              "illegal setting " + FMT + "=" + fmt + " in arg list: " + argList.getArgString());
        
        readCommand.enforceExt = argList.popBoolean(EXT, true);
        
        if (readCommand.file.isDirectory()) {
          if (!readCommand.enforceExt)
            throw new IllegalArgumentException(
                "illegal setting " + EXT + "=false while file=" + outpath + " is a directory");
        } else if (readCommand.file.exists())
          throw new IllegalArgumentException("file already exist: " + outpath);
        else {
          // not a directory; and doesn't exist
          // check the parent directory exists
          File parent = readCommand.file.getParentFile();
          if (parent != null && !parent.exists())
            throw new IllegalArgumentException(
                "'" + FILE + "=" + outpath + "': first create parent dir " + parent);
          
          if (readCommand.enforceExt) {
            String name = readCommand.file.getName();
            String normalizedName;
            if (readCommand.isPathResponse())
              normalizedName = FilenamingConvention.INSTANCE.
                normalizePathFilename(name, readCommand.format);
            else
              normalizedName = FilenamingConvention.INSTANCE.
                normalizeNuggetFilename(name, readCommand.format);
            if (!normalizedName.equals(name)) {
              readCommand.file = new File(parent, normalizedName);
              if (readCommand.file.exists())
                throw new IllegalArgumentException(
                    "normalized file path " + readCommand.file + " already exists");
            }
          }
        }
      } // if
      

      enforceNoRemainingArgs(argList);
    }
    
    return true;
  }
  
  
  
  private boolean configureWriteCommands(ArgList argList) {

    List<String> writeCommands = argList.removedContained(ADD, ADDB, WIT);
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
    
    if (writeCommand.add) {
      writeCommand.entryHashes = getHashes(argList);
      if (writeCommand.entryHashes.isEmpty())
        throw new IllegalArgumentException(
            "missing entry hashes for " + ADD + " command: " + argList.getArgString());
    }
    
    
    if (writeCommand.witness) {
      long e = argList.popLong(TEX, Long.MIN_VALUE);
      if (e == Long.MIN_VALUE) {
        writeCommand.toothExponent = -1;
      } else {
        if (e < 0 || e > SldgConstants.MAX_WITNESS_EXPONENT)
          throw new IllegalArgumentException("out of bounds " + TEX + "=" + e);
        writeCommand.toothExponent = (int) e;
      }
      writeCommand.witnessLast = argList.popBoolean(WSTATE, true);
    }
    
    
    enforceNoRemainingArgs(argList);
    
    return true;
  }
  
  
  
  private void enforceNoRemainingArgs(ArgList argList) {
    if (!argList.isEmpty())
      throw new IllegalArgumentException(
          "illegal arguments / combination: " + argList.getArgString());
  }
  
  
  
  
  private List<ByteBuffer> getHashes(ArgList args) {
    List<String> hexes = args.removeMatched(s -> isHash(s));
    return Lists.map(hexes, s -> ByteBuffer.wrap(IntegralStrings.hexToBytes(s)));
  }
  
  
  private boolean isHash(String arg) {
    return IntegralStrings.isHex(arg) && arg.length() == 2 * SldgConstants.HASH_WIDTH;
  }
  
  
  
  

  // NOTE: this code may belong in Opening
  private Opening getMode(ArgList args) {
    String mi = args.popValue(MODE, MODE_RW);
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
        "A skip ledger is a tamper-proof, append-only list of SHA-256 hashes added by its " +
        "user. Since hashes are opaque, a skip ledger itself conveys little information. " +
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
        "witnessed, then the crumtrail witness evidence together with these rows can be" +
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
    table.printRow(PATH ,       "prints or outputs the shortest path connecting the given pair of", REQ_CH);
    table.printRow(null,        "numbered rows", null);
    out.println();
    table.printRow(NUG,         "prints or outputs a nugget. A nugget proves that the hash of a given", REQ_CH);
    table.printRow(null,        "entry is on a numbered row that is linked from the last row in its", null);
    table.printRow(null,        "ledger. It also contains evidence that sets the row's minimum age.", null);
    out.println();
    table.printRow(STATUS,      "prints the status of the ledger", REQ_CH);
    table.println();
    table.printHorizontalTableEdge('_');
    table.printlnCenteredSpread("Output Options:", 1);
    table.printHorizontalTableEdge('-');
    table.println();
    table.printRow(FILE + "=*", "outputs to the specified path", null);
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
  
  private final static String WIT = "wit";
  private final static String TEX = "tex";
  private final static String WSTATE = "wstate";
  

  private final static String STATE = "state";
  private final static String PATH = "path";
  private final static String STATUS = "status";
  private final static String LIST = "list";
  private final static String NUG = "nug";
  

  private final static String INFO = "info";
  private final static String FILE = "file";
  private final static String EXT = "ext";
  private final static String FMT = "fmt";
  private final static String JSON = "json";
  private final static String BINARY = "binary";
  
  
  public final static String PATH_EXT = ".spath";
  public final static String STATE_EXT = "." + STATE + PATH_EXT;
  public final static String NUG_EXT = "." + NUG;
  
  
  
  
  
}
