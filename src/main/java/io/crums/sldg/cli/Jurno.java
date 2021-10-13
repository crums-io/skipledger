/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cli;


import static io.crums.util.Strings.nTh;
import static io.crums.util.Strings.pluralize;
import static io.crums.util.Strings.singularVerb;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import io.crums.client.ClientException;
import io.crums.io.Opening;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.demo.jurno.Journal;
import io.crums.sldg.time.TrailedRow;
import io.crums.sldg.time.WitnessReport;
import io.crums.util.IntegralStrings;
import io.crums.util.Lists;
import io.crums.util.main.ArgList;
import io.crums.util.main.MainTemplate;
import io.crums.util.main.NumbersArg;
import io.crums.util.main.PrintSupport;
import io.crums.util.main.StdExit;
import io.crums.util.main.TablePrint;

/**
 * Manages a {@linkplain Journal journal}ed text file from the command line.
 */
public class Jurno extends MainTemplate {
  
  private final static String EXTENSION = SldgConstants.DB_EXT;
  
  
  public static void main(String[] args) {
    Jurno jurno = new Jurno();
    jurno.doMain(args);
  }
  

  
  
  private static class RowCommand {

    /**
     * Ledgered lines
     */
    List<Long> rowNums = Collections.emptyList();
    
    
    
    void setRowNums(ArgList args) {
      String match = args.removeSingle(NumbersArg.MATCHER);
      if (match == null)
        throw new IllegalArgumentException("missing row-numbers arg");
      
      rowNums = NumbersArg.parse(match);
      if (rowNums.get(0) < 1 || !Lists.isSortedNoDups(rowNums))
        throw new IllegalArgumentException(
            "row numbers must be > 0 and strictly ascending. Numbers parsed: " + rowNums);
    }
    
  }
  
  private static class MakeMorselCmd extends RowCommand {
    
    File morselFile = new File(".");
    
    void setMorselFile(ArgList args) {
      this.morselFile = new File(args.removeValue(FILE, "."));
      File parent = morselFile.getParentFile();
      if (parent != null && !parent.exists())
        throw new IllegalArgumentException(
            "expected parent directory for morsel file does not exist: " + this.morselFile);
      
      if (morselFile.isFile())
        throw new IllegalArgumentException("morsel file already exists: " + morselFile);
      
    }
  }
  
  
  
  
  
  
  private File textFile;
  private File ledgerDir;
  private Opening opening;
  private String command;
  
  
  private Journal journal;
  
  private MakeMorselCmd makeMorsel;
  private RowCommand list;
  

  /**
   * 
   */
  private Jurno() {
  }

  @Override
  protected void init(String[] args) throws IllegalArgumentException, Exception {
    ArgList argList = newArgList(args);
    
    this.command = argList.removeCommand(CREATE, STATUS, HISTORY, UPDATE, TRIM, LIST, MAKE_MORSEL, STATE_MORSEL);
    
    
    // set the opening preamble
    switch (command) {
    case STATUS:
    case HISTORY:
    case LIST:
    case MAKE_MORSEL:
    case STATE_MORSEL:
      this.opening = Opening.READ_ONLY;
      break;
    case TRIM:
    case UPDATE:
      opening = Opening.READ_WRITE_IF_EXISTS;
      break;
    case CREATE:
      opening = Opening.CREATE;
      break;
    }
    
    setJournoFiles(argList);
    
    switch (command) {
    case MAKE_MORSEL:
      this.makeMorsel = new MakeMorselCmd();
      makeMorsel.setRowNums(argList);
      makeMorsel.setMorselFile(argList);
      break;
    case LIST:
      this.list = new RowCommand();
      list.setRowNums(argList);
      break;
    case STATE_MORSEL:
      this.makeMorsel = new MakeMorselCmd();
      makeMorsel.setMorselFile(argList);
      break;
    }
    
    argList.enforceNoRemaining();
  }
  
  
  
  /**
   * Invoked only if this.opening != null.
   */
  private void setJournoFiles(ArgList args) {

    assert opening != null && opening != Opening.CREATE_ON_DEMAND;
    
    this.textFile = args.removeExistingFile();
    if (textFile == null)
      throw new IllegalArgumentException("requried text file is missing");
    
    if (!args.removeContained(LD).isEmpty()) {
      
      if (opening.exists()) {
        ledgerDir = args.removeExistingDirectory();
        if (ledgerDir == null)
          throw new IllegalArgumentException("required ledger directory missing with option " + LD);
        
      } else {
        
        assert opening == Opening.CREATE;
        
        // we get finicky on the create path
        List<String> dir = args.argsRemaining();
        if (dir.isEmpty())
          throw new IllegalArgumentException("option " + LD + " requires a path to the ledger directory");
        if (dir.size() > 1)
          throw new IllegalArgumentException("ambiguous ledger directory with " + LD + " option: " + dir);
        
        this.ledgerDir = new File(dir.get(0));
        if (!ledgerDir.exists()) {
          File parent = ledgerDir.getParentFile();
          if (parent != null && !parent.exists()) {
            throw new IllegalArgumentException("parent of ledger directory must be an existing directory: " + ledgerDir);
          }
        }
      }
      
    } else {  // default
      ledgerDir = new File(textFile.getPath() + EXTENSION);
    }
  }

  @Override
  protected void start() throws IOException {
    if (opening == null)
      return;
    
    
    this.journal = Journal.loadInstance(textFile, opening, ledgerDir);
    journal.validateState();
    
    switch (command) {
    case CREATE:
    case UPDATE:
      update();
      break;
      
    case STATUS:
      printStatus();
      break;
      
    case HISTORY:
      history();
      break;
      
    case LIST:
      listRows();
      break;
      
    case TRIM:
      trim();
      break;
      
    case MAKE_MORSEL:
      makeMorsel();
      break;
      
    case STATE_MORSEL:
      makeMorsel();
      break;
      
    }   // switch (command
  }
  
  

  void update() {
    String message;
    final int alreadyLedgered = journal.getLedgeredLines();
    switch (journal.getState()) {
    case FORKED:
      int textLineNo = journal.getForkedLineNumber();
      long forkRn = journal.getFirstConflict();
      message = textFile + " has forked from its ledger at line # " + textLineNo;
      if (textLineNo != forkRn) {
        message += " (which was the " + nTh(forkRn) + " row (ledgerable line)";
      }
      break;
    case TRIMMED:
      int ledgeredLines = journal.getLedgeredLines();
      int actualLines = journal.getLedgerableLines();
      message =
          textFile + " has been trimmed to fewer rows (ledgerable lines):" +
          ledgeredLines + " -> " + actualLines;
      break;
    case PENDING:
      
      journal.update();
      
    case COMPLETE:
      
      int newLines = journal.getLedgeredLines() - alreadyLedgered;
      if (opening == Opening.CREATE) {
        message =
            "Created journal with " + newLines + pluralize(" new row", newLines) +
            pluralize(" (ledgerable line", newLines) + ")";
      } else {
        message =
            newLines + pluralize(" new row", newLines) +
            pluralize(" (ledgerable line", newLines) + ") added";
      }
      try {
        WitnessReport witReport = journal.witness();
        if (witReport.nothingDone())
          message += "." + System.lineSeparator() + "Up to date.";
        else {
          int crums = witReport.getRecords().size();
          int crumtrails = witReport.getStored().size();
          message += "." + System.lineSeparator() +
              crums + pluralize(" crum", crums) + " submitted; " + crumtrails +
              pluralize(" crumtrail", crumtrails) + pluralize(" (witness record", crumtrails) +
              ") stored";
          
          if (crumtrails == 0)
            message += System.lineSeparator() + "Run '" + UPDATE + "' in a few";
        }
      } catch (ClientException cx) {
        message += "." + System.lineSeparator();
        long lastWitRn = journal.lastWitnessedRowNumber();
        int unwitnessed = (int) (journal.hashLedgerSize() - lastWitRn);
        message += unwitnessed + pluralize(" row", unwitnessed);
        message += singularVerb(" remain", unwitnessed) + " unwitnessed.";
        message += System.lineSeparator() + "error message: " + cx.getMessage();
        Throwable cause = cx.getRootCause();
        if (cause != null)
          message += System.lineSeparator() + "cause: " + cause;
      }
      break;
    
    default:
      throw new RuntimeException("unaccounted state assertion: " + journal.getState());
    }
    
    if (journal.getState().needsMending()) {
      message += "." + System.lineSeparator() + "The only way to fix this is to ";
      if (journal.getState().isForked())
        message += "either restore the text at the forked line number, or ";
      
      message +=  "run '" + TRIM + "' (which causes loss of historical info)." + System.lineSeparator();
    }
    
    System.out.println(message);
  }
  
  void trim() {
    if (!journal.getState().needsMending()) {
      System.err.println("[ERROR] Journal does not need mending.");
      return;
    }
    
    int lastGoodRow = (int) journal.lastValidRow();
    int size = (int) journal.getLedgeredLines();
    int rowsToLose = size - lastGoodRow;
    int trailsToLose = numToLose(journal.getHashLedger().getTrailedRowNumbers(), lastGoodRow);
    
    String message =
            "Confirm '" + TRIM + "' ledger to row %d%n" + // lastGoodRow
            "  trails to lose: %d%n" +  // trailsToLose
            "  ledgered lines to lose: %d%n%n" + // rowsToLose
            "Current rows in ledger: %d%n" +  // size
            "            after trim: %d%n" +  // lastGoodRow
            "Proceed to trim? [Type 'yes']:%n";
    
    Console console = System.console();
    if (console == null) {
      System.err.println("[ERROR] No console. Aborting.");
      StdExit.GENERAL_ERROR.exit();
    }
    console.printf(
        message,
        lastGoodRow, trailsToLose, rowsToLose, size, lastGoodRow);
    String ack = console.readLine();
    if ("yes".equals(ack)) {
      journal.rollback();
      System.out.println(rowsToLose + pluralize(" row", rowsToLose) + " trimmed.");
      System.out.printf("Current number of rows in ledger: %d%n", journal.hashLedgerSize());
    } else {
      console.writer().println();
      console.writer().println("Aborted.");
    }
  }
  
  
  void makeMorsel() throws IOException {
    if (journal.getState().needsMending())
      throw new IllegalStateException(
          "Journaled file " + journal.getTextFile() + " is out-of-sync with its ledger." + System.lineSeparator() +
          "Run '" + STATUS + "' for details.");
    
    File file = journal.writeMorselFile(makeMorsel.morselFile, makeMorsel.rowNums, null);
    int entries = makeMorsel.rowNums.size();
    if (entries == 0)
      System.out.println("state path written to morsel: " + file);
    else
      System.out.println(entries + pluralize(" entry", entries) + " written to morsel: " + file);
  }
  
  private int numToLose(List<Long> rns, long lastGoodRow) {
    int index = Collections.binarySearch(rns, lastGoodRow);
    int goodOnes = index < 0 ? -1 - index : index;
    return rns.size() - goodOnes;
  }


  private final static int RM = 80;
  private final static int LEFT_STATUS_COL_WIDTH = 16;
  private final static int RIGHT_STATUS_COL_WIDTH = 18;
  private final static int MID_STATUS_COL_WIDTH = RM - LEFT_STATUS_COL_WIDTH - RIGHT_STATUS_COL_WIDTH;
  
  
  private TablePrint statusTable() {
    return new TablePrint(
        LEFT_STATUS_COL_WIDTH, MID_STATUS_COL_WIDTH, RIGHT_STATUS_COL_WIDTH);
  }
  
  void printStatus() {
    TablePrint table = statusTable();
    table.println();
    switch (journal.getState()) {
      
      
    case COMPLETE:
      
      if (journal.sourceLedgerSize() == 0) {
        table.println("Journal initialized. No ledgerable lines in " + journal.getTextFile());
        table.println("OK");
        break;
      }
      printStatus(table);
      table.println();
      int unwitnessedRows = (int) journal.unwitnessedRowCount();
      if (unwitnessedRows == 0) {
        table.println("Journal is complete. Nothing to update.");
        table.println("OK");
      } else {
        table.println(
            "Ledger is up-to-date; " + unwitnessedRows + pluralize(" row", unwitnessedRows) +
            " not witnessed. ");
        table.println("If you have a network connection, invoke '" + UPDATE + "' once,");
        table.println("then a few minutes later to fix this");
      }
      break;
    case PENDING:

      printStatus(table);
      table.println();
      int pendingLines = journal.getLedgerableLines() - journal.getLedgeredLines();
      table.println(pendingLines + pluralize(" line", pendingLines) + " pending update.");
      break;
      
    case FORKED:

      table.println(journal.getTextFile() + " has forked from its ledger.");
      table.println();
      table.printRow("forked line #:", journal.getForkedLineNumber());
      table.printRow("forked row #:", journal.getFirstConflict());
      table.println();
      table.println("There are 2 ways to fix this:");
      table.println(" 1) restore the text at the forked line number (" + journal.getForkedLineNumber() + "), or");
      table.println(" 2) run '" + TRIM +  "' (which causes loss of historical info)");
      table.println();
      // TODO:
      // 1) if only one line has changed, let the user know this is the case
      table.println("FORKED");
      break;
    case TRIMMED:
      
      int linesRemoved = journal.getLedgeredLines();
      table.println(
          journal.getTextFile() + " has been trimmed to " + linesRemoved +
          pluralize(" fewer line", linesRemoved) + " than recorded in its ledger. Run '" + TRIM + "' to fix this.");
      table.println();
      table.println("TRIMMED");
      break;
    }
    
    
  }
  
  
  
  private void listRows() {
    TablePrint table;
    {
      List<Long> rowNums = list.rowNums;
      long last = rowNums.get(rowNums.size() - 1);
      long lastLineNo = journal.getLineNumber(last);
      int decimalWidth = Math.max(3, Long.toString(lastLineNo).length());
      
      table = new TablePrint(decimalWidth + 1, decimalWidth + 3, 76 - 2*decimalWidth);
    }
    
    table.printRow("row", "[line]");
    table.printHorizontalTableEdge('-');
    
    for (long rn: list.rowNums) {
      long lineNo = journal.getLineNumber(rn);
      String line = journal.getRowText(rn);
      line = line.substring(0, line.length() - 1);
      table.printRow(rn, "[" + lineNo + "]", line);
    }
  }
  
  
  private void printStatus(TablePrint table) {
    table.printRow("Lines in file:", journal.getLinesInFile() );
    table.printRow("  ledgerable:", journal.getLedgerableLines() );
    table.printRow("  ledgered:", journal.getLedgeredLines() );
    table.println();
    table.println();
    int witnessedRows = journal.getHashLedger().getTrailCount();
    table.printRow("Witnessed rows:", witnessedRows);
    if (witnessedRows != 0) {
      table.println();
      table.printRow(null, "First crumtrail");
      printTrailDetail(0, table);
      if (witnessedRows > 1) {
        table.println();
        table.printRow(null, "Last crumtrail");
        printTrailDetail(witnessedRows - 1, table);
      }
    }

    table.println();
    table.printRow("Ledger state:", IntegralStrings.toHex(journal.getHashLedger().getSkipLedger().stateHash()));
  }
  
  

  void history() {
    var table = statusTable();

    int witnessedRows = journal.getHashLedger().getTrailCount();
    
    for (int index = 0; index < witnessedRows; ++index) {
      table.println();
      printTrailDetail(index, table);
    }
    table.println();
    long ledgeredRows = journal.hashLedgerSize();
    table.println(ledgeredRows + pluralize(" ledgered row", ledgeredRows));
    table.println(
        "state witnessed and recorded " +  witnessedRows + pluralize(" time", witnessedRows));
    table.println();
  }
  
  
  private void printTrailDetail(int index, TablePrint table) {
    TrailedRow trailedRow = journal.getHashLedger().getTrailByIndex(index);
    long utc = trailedRow.utc();
    table.printRow("row #: ", trailedRow.rowNumber());
    table.printRow("created before:", new Date(utc), "UTC: " + utc);
    table.printRow("trail root:", IntegralStrings.toHex(trailedRow.trail().rootHash()));
    table.printRow("ref URL:", trailedRow.trail().getRefUrl());
  }


  private final static int INDENT = 1;
  
  
  @Override
  protected void printLegend(PrintStream out) {
    out.println();
    out.println("For additional info on morsels try");
    out.println("  " + Mrsl.PROGNAME + " -help");
    out.println("from the console.");
    out.println();
  }

  @Override
  protected void printDescription() {
    String paragraph;
    PrintSupport printer = new PrintSupport();
    printer.setMargins(INDENT, RM);
    printer.println();
    System.out.println("DESCRIPTION:");
    printer.println();
    paragraph =
        "Command line tool for creating and maintaining a tamper-proof, journaled, " +
        "append-only text file using a skip ledger. Supports line-by-line extractions " +
        "from the text file, annotated with tamper-proof timestamps that can be verified " +
        "against a compact, opaque hash structure that captures the state of the text file.";
    printer.printParagraph(paragraph);
    printer.println();
    printer.println("Lines & Rows");
    printer.println();
    paragraph =
        "The lines in a text file compose the rows in the ledger. However not all lines count " +
        "as rows: blank, empty lines do not figure in the accounting and are skipped. Nor does " +
        "the 1st line: editing it doesn't break the ledger.";
    printer.printParagraph(paragraph);
    printer.println();
    paragraph =
        "Each row (ledgerable line) consists of a sequence of tokens (usually words) identified by surrounding " +
        "whitespace. For this reason, neither indentation nor [the amount of] word-spacing matter on a " +
        "ledgerable line.";
    printer.printParagraph(paragraph);
    printer.println();
    printer.println();
    printer.println("Trailed Rows");
    printer.println();
    paragraph =
        "A row whose hash has been witnessed, evidenced by a crumtrail attachment. " +
        "Trailed rows establish the maximum creation-date " +
        "(minimum age) for both the row they reference and also every row before them. This is " + 
        "because every row in a skip ledger is linked (thru hash pointers) to every row before it.";
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
        "In addition to the explicitly named arguments below, each command also takes the journaled text file as argument. " +
        "By default the ledger (the opaque database that backs the journal) is found in the same directory " +
        "as the text file and is named the same as the text file, but with an added '" + EXTENSION + "' extension.";
    printer.printParagraph(paragraph);
    printer.println();

    TablePrint table = new TablePrint(out, LEFT_TBL_COL_WIDTH, RIGHT_TBAL_COL_WIDTH);
    table.setIndentation(INDENT);
    
    table.printRow(CREATE, "creates a new journal for the given text file");
    table.println();
    table.printRow(STATUS, "prints the status of the journaled text file");
    table.println();
    table.printRow(HISTORY, "prints the trails (witness records) in the ledger");
    table.println();
    table.printRow(UPDATE, "updates the ledger to the state of the journaled text file. Date");
    table.printRow(null,   "and time information (in the form of crumtrails and beacons) are");
    table.printRow(null,   "automatically embedded in the ledger as necessary.");
    table.println();
    table.printRow(TRIM,   "trims (truncates) the ledger. This is necessary when an already");
    table.printRow(null,   "ledgered line in the text file has later been edited.");
    table.println();

    out.println();
    table.printHorizontalTableEdge('-');
    table.printlnCenteredSpread("OUTPUT", 2);
    table.printHorizontalTableEdge('-');
    out.println();
    

    table.printRow(LIST,   "lists the contents of the rows (ledgerable lines) in the journal");
    table.printRow(null,   "with the given row number(s). The line-number is also shown.");
    table.printRow(null,   "Args:");
    table.println();
    table.printRow(null,   "<row-numbers>  (required)");
    table.println();
    table.printRow(null,   "Strictly ascending row numbers separated by commas (no spaces)");
    table.printRow(null,   "Ranges may be substituted for numbers. For example:");
    table.printRow(null,   "250,692-717");
    table.println();
    
    table.printRow(MAKE_MORSEL, "creates a morsel file containing the contents of the given");
    table.printRow(null,   "row (ledgered line) numbers");
    table.printRow(null,   "Args:");
    table.println();
    table.printRow(null,   "<row-numbers>  (required)");
    table.println();
    table.printRow(null,   "Strictly ascending row numbers separated by commas (no spaces)");
    table.printRow(null,   "Ranges may be substituted for numbers. For example:");
    table.printRow(null,   "308,466,592-598,717");
    table.println();
    table.printRow(null,   FILE + "=<path/to/morselfile>  (optional)");
    table.println();
    table.printRow(null,   "If provided, then <path/to/morselfile> is the destination path.");
    table.printRow(null,   "The given path should not be an existing file; however, if it's");
    table.printRow(null,   "an existing directory, then a filename is generated for the");
    table.printRow(null,   "file in the chosen directory.");
    table.printRow(null,   "DEFAULT: '.'  (current directory)");
    table.println();
    
    table.printRow(STATE_MORSEL, "creates a morsel file containing only the ledger's state-");
    table.printRow(null,   "path. That is the shortest list of rows connecting the last row");
    table.printRow(null,   "to the first. It serves as a fingerprint against which older");
    table.printRow(null,   "morsels can be validated (and optionally be updated).");
    table.println();
    table.printRow(null,   FILE + "=<path/to/morselfile>  (optional)");
    table.println();
    table.printRow(null,   "If provided, then <path/to/morselfile> is the destination path.");
    table.printRow(null,   "The given path should not be an existing file; however, if it's");
    table.printRow(null,   "an existing directory, then a filename is generated for the");
    table.printRow(null,   "file in the chosen directory.");
    table.printRow(null,   "DEFAULT: '.'  (current directory)");
    table.println();
    
    table.printHorizontalTableEdge('-');
    table.printlnCenteredSpread("OPTIONS", 2);
    table.printHorizontalTableEdge('-');
    table.println();
    
    table.printRow(LD,    "sets the ledger directory");
    table.printRow(null,  "DEFAULT: same as the text file, but with the '" + EXTENSION + "' extension");
    table.printRow(null,  "         added.");
    table.println();
    
  }
  
  
  //  - - C O M M A N D S - -
  
  private final static String CREATE = "create";
  private final static String STATUS = "status";
  private final static String HISTORY = "history";
  private final static String UPDATE = "update";
  private final static String TRIM = "trim";
  
  private final static String LIST = "list";
  
  
  private final static String MAKE_MORSEL = "make-morsel";
  private final static String STATE_MORSEL = "state-morsel";
  
  private final static String FILE = "save";
  
  //  - - O P T I O N S - -
  
  private final static String LD = "-ld";

}





















