/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.cli;


import static io.crums.util.main.Args.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.List;

import io.crums.io.Opening;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.db.Db;
import io.crums.util.IntegralStrings;
import io.crums.util.Lists;
import io.crums.util.main.ArgList;
import io.crums.util.main.MainTemplate;
import io.crums.util.main.TablePrint;

/**
 * 
 */
public class Sldg extends MainTemplate {
  
  
  private String[] args;
  
  private File dir;
  private Opening mode;
  
//  private String command;
  
  
  private boolean add;
  
  private List<ByteBuffer> entryHashes;
  
  private boolean witness;
  private int toothExponent;
  private boolean witnessLast;
  
  private boolean info;
  
  private boolean state;
  
  private boolean list;
  private List<Long> rowNumbers;
  
  private boolean nug;
  private boolean status;
  

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
    this.args = args;
    if (args.length == 0)
      throw new IllegalArgumentException("No arguments specified");
    
    ArgList argList = new ArgList(args);
    
    this.dir = new File(argList.popRequiredValue(DIR));
    this.mode = getMode(argList);

    configureCommon(argList);
    
    boolean noCommand =
        !configureWriteCommands(argList) &&
        !configureReadCommands(argList);
    
    if (noCommand && !configureReadCommands(argList) &&
        mode != Opening.CREATE)
      throw new IllegalArgumentException("missing command");
    
    this.db = new Db(dir, mode);
    
    if (noCommand && info)
      System.out.println("ledger created at " + db.getDir());
  }

  
  
  @Override
  protected void start() throws InterruptedException, Exception {
    
    try (Db db = this.db) {
      
      if (add) {
        entryHashes.forEach(h -> db.getLedger().appendRows(h));
        if (info) {
          int count = entryHashes.size();
          System.out.println(count + pluralize(" row", count) + " added");
        }
      }
      if (witness) {
        Db.WitnessReport report =
            toothExponent == -1 ?
                db.witness(witnessLast) :
                  db.witness(toothExponent, witnessLast);
        
        int count = report.getStored().size();
        String msg;
        if (info) {
          int witCount = report.getRecords().size();
          msg = witCount + pluralize(" row", witCount) + " witnessed; " + count + pluralize(" crumtrail", count) + " stored";
          
        } else
          msg = String.valueOf(count);
        System.out.println(msg);
      }
      if (add || witness)
        return;
      
      
    }
  }
  
  
  private void configureCommon(ArgList argList) {
    this.info = argList.popBoolean(INFO, true);
  }
  
  
  private String pluralize(String single, int count) {
    return count == 1 ? single : single + "s";
  }



  private boolean configureReadCommands(ArgList argList) {
    
    List<String> readCommands = argList.removedContained(STATE, LIST, NUG, STATUS);
    switch (readCommands.size()) {
    case 0:   return false;
    case 1:   break;
    default:
      throw new IllegalArgumentException(
          "duplicate commands in '" + argList.getArgString() + "'");
    }
    
    String command = readCommands.get(0);
    this.state = STATE.equals(command);
    this.list = LIST.equals(command);
    this.nug = NUG.equals(command);
    this.status = STATUS.equals(command);
    
    if (list || nug) {
      this.rowNumbers = argList.popNumbers();
      if (rowNumbers.isEmpty())
        throw new IllegalArgumentException(
            "missing row number[s] in arguments: " + argList.getArgString());
    }
    return true;
  }
  
  private boolean configureWriteCommands(ArgList argList) {

    List<String> writeCommands = argList.removedContained(ADD, WIT);
    switch (writeCommands.size()) {
    case 0:
      break;
    case 1:
      this.add = ADD.equals(writeCommands.get(0));
      this.witness = !this.add;
      break;
    case 2:
      this.add = this.witness = true;
      break;
    default:
      throw new IllegalArgumentException(
          "duplicate commands in '" + argList.getArgString() + "'");
    }
    
    if (add) {
      this.entryHashes = getHashes(argList);
      if (entryHashes.isEmpty())
        throw new IllegalArgumentException(
            "missing entry hashes for " + ADD + " command: " + argList.getArgString());
    }
    
    
    if (witness) {
      long e = argList.popLong(TEX, Long.MIN_VALUE);
      if (e == Long.MIN_VALUE) {
        this.toothExponent = -1;
      } else {
        if (e < 0 || e > SldgConstants.MAX_WITNESS_EXPONENT)
          throw new IllegalArgumentException("out of bounds " + TEX + "=" + e);
        this.toothExponent = (int) e;
      }
      this.witnessLast = argList.popBoolean(WSTATE, true);
    }
    
    
    boolean configured = add || witness;
    if (configured && !argList.isEmpty())
      throw new IllegalArgumentException("illegal arguments / combination: " + argList.getArgString());
    
    return configured;
  }
  
  
  
  
  private List<ByteBuffer> getHashes(ArgList args) {
    List<String> hexes = args.removeMatched(s -> isHash(s));
    return Lists.map(hexes, s -> ByteBuffer.wrap(IntegralStrings.hexToBytes(s)));
  }
  
  
  private boolean isHash(String arg) {
    return IntegralStrings.isHex(arg) && arg.length() == 2 * SldgConstants.HASH_WIDTH;
  }
  
  
  
  private Db getDb(ArgList args) throws IOException {
    File dir = new File(args.popRequiredValue(DIR));
    Opening mode = getMode(args);
    return new Db(dir, mode);
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
  
  
  


  @Override
  protected void printDescription() {
    System.out.println();
    System.out.println("DESCRIPTION:");
    System.out.println();
    System.out.println("Command line tool for accessing a skip ledger on the file system.");
    System.out.println();
    
  }

  @Override
  protected void printUsage(PrintStream out) {
    out.println();
    out.println("USAGE:");
    out.println();
    out.println("Arguments are specified as 'name=value' pairs.");
    out.println();
    

    TablePrint table, subTable, subWideKeyTable;
    {
      int console = 80;
      
      int fcw = 8;
      int lcw = 2;
      int mcw;
      
      
      int indent = 1;
      mcw = console - fcw - lcw - indent;
      
      int subFcw = 7;
      int subLcw = console - fcw - subFcw;
      
      int subWkFcw = 13;
      int subWkLcw = console - fcw - subWkFcw;
      
      
      table = new TablePrint(out, fcw, mcw, lcw);
      subTable = new TablePrint(out, subFcw, subLcw);
      subWideKeyTable = new TablePrint(out, subWkFcw, subWkLcw);
      
      table.setIndentation(indent);
      subTable.setIndentation(fcw + indent);
      subWideKeyTable.setIndentation(fcw + indent);
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
    subTable.printRow(           null,     "(to create an empty DB supply no additional arguments)");

    out.println();
    table.printHorizontalTableEdge('-');
    table.printlnCenteredSpread("ADD / WITNESS", 2);
    table.printHorizontalTableEdge('-');
    out.println();
    
    table.printRow(ADD ,  "adds one or more hexidecimal SHA-256 hash entries", REQ_CH);
    out.println();
    
    table.printRow(WIT ,  "witnesses the last row and/or previous unwitnessed rows whose", REQ_PLUS);
    table.printRow(null,  "numbers match the tooth-exponent. Outputs the number of rows", null);
    table.printRow(null,  "witnessed. Options (inclusive):", null);
    out.println();
    subWideKeyTable.printRow(TEX + "=*", "witness numbered rows that are multiples of");
    subWideKeyTable.printRow(null,       "2 raised to the power of this number.");
    subWideKeyTable.printRow(null,       "Valid range: [0,62]");
    subWideKeyTable.printRow(null,       "DEFAULT: dynamically computed value that generates");
    subWideKeyTable.printRow(null,       "up to 8 tooth-included rows to witness");
    out.println();
    subWideKeyTable.printRow(WSTATE + "=true",  "witness last row, even if its number is not toothed");
    subWideKeyTable.printRow(null,              "DEFAULT: true");


    out.println();
    table.printHorizontalTableEdge('-');
    table.printlnCenteredSpread("OUTPUT / PRINT", 2);
    table.printHorizontalTableEdge('-');
    out.println();

    table.printRow(STATE ,      "prints or outputs abbreviated evidence of the state the ledger", REQ_CH);
    table.printRow(null,        "by outputing the shortest path of rows connnecting the 1st row", null);
    table.printRow(null,        "to the last thru their hashpointers, i.e. the nugget for row # 1", null);
    out.println();
    table.printRow(LIST ,       "lists (prints or outputs) the numbered rows", REQ_CH);
    out.println();
    table.printRow(NUG,         "prints or outputs nuggets. A nugget proves that the hash of a given", REQ_CH);
    table.printRow(null,        "entry is on a numbered row that is linked from the last row in its", null);
    table.printRow(null,        "ledger.", null);
    out.println();
    table.printRow(STATUS,      "prints the status of the ledger", REQ_CH);
    out.println();
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
  
  
  private final static String OUT = "out";
  private final static String LIMIT = "limit";
  


  private final static String ADD = "add";
  
  private final static String WIT = "wit";
  private final static String TEX = "tex";
  private final static String WSTATE = "wstate";
  

  private final static String STATE = "state";
  private final static String STATUS = "status";
  private final static String LIST = "list";
  private final static String NUG = "nug";
  

  private final static String INFO = "info";
  
  
  
  
}
