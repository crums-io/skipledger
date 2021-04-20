/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cli;


import static io.crums.util.Strings.*;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import io.crums.client.ClientException;
import io.crums.io.Opening;
import io.crums.model.Beacon;
import io.crums.model.CrumTrail;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.db.Db;
import io.crums.sldg.demo.jurno.Journal;
import io.crums.sldg.demo.jurno.JurnoMorselBuilder;
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
  
  private static class MakeMorselCmd {
    
    File morselFile = new File(".");
    
    /**
     * Ledgered lines
     */
    List<Integer> lineNums;
    
    void setMorselFile(String filepath) {
      this.morselFile = new File(filepath);
      File parent = morselFile.getParentFile();
      // don't care if failing after set.. we won't be see this instance again
      if (parent != null && !parent.exists())
        throw new IllegalArgumentException(
            "expected parent directory for morsel file does not exist: " + this.morselFile);
      
      if (morselFile.isFile())
        throw new IllegalArgumentException("morsel file already exists: " + morselFile);
      
    }
    
    private void setLineNums(List<Integer> lineNums) {
      if (lineNums == null || lineNums.isEmpty())
        throw new IllegalArgumentException("missing or illegally formatted line numbers");
      if (lineNums.get(0) < 1 || !Lists.isSortedNoDups(lineNums))
        throw new IllegalArgumentException(
            "line numbers must be > 0 and strictly ascending. Numbers parsed: " + lineNums);
      
      this.lineNums = lineNums;
    }
  }
  
  
  
  
  
  
  private File textFile;
  private File ledgerDir;
  private Opening opening;
  private String command;
  
  private boolean abortOnFork;
  
  
  private Journal journal;
  
  private MakeMorselCmd makeMorsel;
  

  /**
   * 
   */
  private Jurno() {
  }

  @Override
  protected void init(String[] args) throws IllegalArgumentException, Exception {
    ArgList argList = newArgList(args);
    
    this.command = argList.removeCommand(CREATE, STATUS, UPDATE, TRIM, MAKE_MORSEL);
    
    
    // set the opening preamble
    switch (command) {
    case STATUS:
    case MAKE_MORSEL:
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
    
    if (opening != null)
      setJournoFiles(argList);
    
    if (MAKE_MORSEL.equals(command)) {

      this.makeMorsel = new MakeMorselCmd();
      
      List<String> remainingArgs = argList.argsRemaining();
      switch (remainingArgs.size()) {
      case 0:
        throw new IllegalArgumentException("missing line numbers");
      case 1:
        makeMorsel.setLineNums(NumbersArg.parseInts(remainingArgs.get(0)));
        argList.removeContained(remainingArgs.get(0));
        break;
      case 2:
        String file = argList.removeValue(FILE, ".");
        if (argList.size() != 1)
          throw new IllegalArgumentException(
              "unrecognized (or too many) command line args in remaining in pipeline [a]: " + remainingArgs);
        
        List<Integer> lineNums = NumbersArg.parseInts(argList.removeFirst());
        
        makeMorsel.setLineNums(lineNums);
        makeMorsel.setMorselFile(file);
        break;
      default:
        throw new IllegalArgumentException(
            "unrecognized (or too many) command line args in remaining in pipeline [b]: " + remainingArgs);
      }
      
      argList.removeContained(remainingArgs);
      
    } //  if (MAKE_MORSEL
    
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
    if (opening != null) {
      Db db = new Db(ledgerDir, opening);
      this.journal = new Journal(textFile, db);
      switch (command) {
      case CREATE:
      case UPDATE:
        
        final int alreadyLedgered = journal.getLedgeredLines();
        
        journal.update(abortOnFork);
        
        String message;
        {
          switch (journal.getState()) {
          case INIT:
            message = "OK. No ledgerable lines in " + textFile;
            break;
          case FORKED:
            int textLineNo = journal.getForkTextLineNumber();
            int lineNo = journal.getForkLineNumber();
            message = textFile + " has forked from its ledger at line # " + textLineNo;
            if (textLineNo != lineNo) {
              message += " (which was the " + nTh(lineNo) + " ledgerable line)";
            }
            break;
          case TRIMMED:
            int ledgeredLines = journal.getLedgeredLines();
            int actualLines = journal.getLines();
            message = textFile + " has been trimmed to fewer ledgerable lines (" + ledgeredLines +
                " -> " + actualLines + ")";
            break;
          case COMPLETE:
            
            int newLines = journal.getLedgeredLines() - alreadyLedgered;
            if (opening == Opening.CREATE) {
              message = "Created journal with " + newLines + pluralize(" new line", newLines);
            } else {
              message = newLines + pluralize(" new line", newLines) + " added";
            }
            try {
              Db.WitnessReport witReport = journal.witness();
              if (witReport.nothingDone())
                message += "." + System.lineSeparator() + "Up-to-date.";
              else {
                int crums = witReport.getRecords().size();
                int crumtrails = witReport.getStored().size();
                message += "." + System.lineSeparator() + crums + pluralize(" crum", crums) + " submitted; " + crumtrails +
                    pluralize(" crumtrail", crumtrails) + pluralize(" (witness record", crumtrails) +
                    ") stored";
                
                if (crumtrails == 0)
                  message += System.lineSeparator() + "Run '" + UPDATE + "' in a few";
              }
            } catch (ClientException cx) {
              message += "." + System.lineSeparator();
              int unwitnessed = journal.db().unwitnessedRowCount();
              message += unwitnessed + pluralize(" row", unwitnessed);
              message += singularVerb(" remain", unwitnessed) + " unwitnessed.";
              message += System.lineSeparator() + "error message: " + cx.getMessage();
              Throwable cause = cx.getRootCause();
              if (cause != null)
                message += System.lineSeparator() + "cause: " + cause;
            }
            break;
          default:
            throw new RuntimeException("assertion failure: " + journal.getState());
          }
          
          if (journal.getState().needsMending()) {
            message += "." + System.lineSeparator() + "The only way to fix this is to ";
            if (journal.getState().isForked())
              message += "either restore the text at the forked line number, or ";
            
            message +=  "run '" + TRIM + "' (which causes loss of historical info)." + System.lineSeparator();
          }
          
          System.out.println(message);
        }
        
          
        break;
      case STATUS:
        printStatus();
        break;
        
      case TRIM:
        journal.dryRun();
        if (!journal.getState().needsMending()) {
          System.err.println("[ERROR] Journal does not need mending.");
          return;
        }
        
        int lastGoodRow = journal.lastValidLedgerRow();
        int lastGoodLine = journal.lastValidLedgeredLine();
        int linesToLose = journal.getLedgeredLines() - lastGoodLine;
        int size = (int) journal.db().size();
        int rowsToLose = size - lastGoodRow;
        int beaconsToLose = numToLose(journal.db().getBeaconRowNumbers(), lastGoodRow);
        int trailsToLose = numToLose(journal.db().getTrailedRowNumbers(), lastGoodRow);
        
        message = "Confirm '" + TRIM + "' ledger to row %d%n" + // lastGoodRow
                  "  trails to lose: %d%n" +  // trailsToLose
                  "  beacons to lose: %d%n" + // beaconsToLose
                  "  ledgered lines to lose: %d%n%n" + // linesToLose
                  "Total rows to lose: %d%n" +  // rowsToLose
                  "Current rows in ledger: %d%n" +  // size
                  "Proceed to trim? [Type 'yes']:%n";
        
        Console console = System.console();
        if (console == null) {
          System.err.println("[ERROR] No console. Aborting.");
          StdExit.GENERAL_ERROR.exit();
          break;  // never reached
        }
        console.printf(
            message,
            lastGoodRow, trailsToLose, beaconsToLose, linesToLose, rowsToLose, size);
        String ack = console.readLine();
        if ("yes".equals(ack)) {
          journal.trim();
          System.out.println(rowsToLose + pluralize(" row", rowsToLose) + " trimmed.");
          System.out.printf("Current number of rows in ledger: %d%n", journal.db().size());
        } else {
          console.writer().println();
          console.writer().println("Aborted.");
        }
        break;
        
      case MAKE_MORSEL:
        makeMorsel();
        break;
        
      }   // switch (command
      
      
    }
  }
  
  
  
  private void makeMorsel() throws IOException {
    journal.dryRun();
    if (journal.getState().needsMending())
      throw new IllegalStateException(
          "Journaled file " + journal.getTextFile() + " is out-of-sync with its ledger." + System.lineSeparator() +
          "Run '" + STATUS + "' for details.");
    
    
    var builder = new JurnoMorselBuilder(journal);
    
    builder.addEntriesByLineNumber(makeMorsel.lineNums);  // for now we canonicalize,
                                                          // keep it simple for validation
    
    
    File file = builder.createMorselFile(makeMorsel.morselFile);
    int entries = makeMorsel.lineNums.size();
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
  
  protected void printStatus() {
    journal.dryRun();
    TablePrint table = new TablePrint(
        LEFT_STATUS_COL_WIDTH, MID_STATUS_COL_WIDTH, RIGHT_STATUS_COL_WIDTH);
    table.println();
    switch (journal.getState()) {
    case INIT:
      
      table.println("Journal initialized. No ledgerable lines in " + journal.getTextFile());
      table.println("OK");
      break;
    case COMPLETE:

      printStatus(table);
      table.println();
      int unwitnessedRows = journal.db().unwitnessedRowCount();
      if (unwitnessedRows == 0) {
        table.println("Journal is complete. Nothing to update.");
        table.println("OK");
      } else {
        table.println(
            "Ledger is up-to-date; " + unwitnessedRows + pluralize(" row", unwitnessedRows) +
            " not witnessed. If you have a network connection, invoking '" + UPDATE + "' should fixes this.");
      }
      break;
    case PENDING:

      printStatus(table);
      table.println();
      int pendingLines = journal.getLines() - journal.getLedgeredLines();
      table.println(pendingLines + pluralize(" line", pendingLines) + " pending update.");
      break;
    case FORKED:

      table.println(journal.getTextFile() + " has forked from its ledger.");
      table.println();
      table.printRow("forked line #:", journal.getForkTextLineNumber());
      table.printRow("forked row #:", journal.getForkLineNumber());
      table.println();
      table.println("There are 2 ways to fix this:");
      table.println(" 1) restore the text at the forked line number (" + journal.getForkTextLineNumber() + "), or");
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
  
  
  private void printStatus(TablePrint table) {
    table.printRow("Lines in file:", journal.getTextLineCount() );
    table.printRow("  ledgerable:", journal.getLines() );
    table.printRow("  ledgered:", journal.getLedgeredLines() );
    table.println();
    int beacons = journal.db().getBeaconCount();
    table.printRow("Beacon rows:", beacons );
    if (beacons > 0) {
      table.println();
      table.printRow(null, "First beacon");
      printBeaconDetail(0, table);
      if (beacons > 1) {
        table.println();
        table.printRow(null, "Last beacon");
        printBeaconDetail(beacons - 1, table);
      }
    }
    table.println();
    int witnessedRows = journal.db().getTrailedRowCount();
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
    table.printRow("Ledger state:", IntegralStrings.toHex(journal.db().getLedger().stateHash()));
  }
  

  private void printBeaconDetail(int index, TablePrint table) {
    long utc = journal.db().getBeaconUtcs().get(index);
    long rn = journal.db().getBeaconRowNumbers().get(index);
    ByteBuffer hash =journal.db().getLedger().getRow(rn).inputHash();
    Beacon beacon = new Beacon(hash, utc);
    table.printRow("row #: ", journal.db().getBeaconRowNumbers().get(index));
    table.printRow("created after:", new Date(utc), "UTC: " + utc);
    table.printRow("ref URL:", beacon.getRefUrl());
    table.printRow("beacon:", beacon.hashHex());
  }
  
  
  private void printTrailDetail(int index, TablePrint table) {
    CrumTrail trail = journal.db().getCrumTrailByIndex(index);
    long utc = trail.crum().utc();
    table.printRow("row #: ", journal.db().getTrailedRowNumbers().get(index));
    table.printRow("created before:", new Date(utc), "UTC: " + utc);
    table.printRow("trail root:", IntegralStrings.toHex(trail.rootHash()));
    table.printRow("ref URL:", trail.getRefUrl());
  }


  private final static int INDENT = 1;

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
    printer.println("Line Numbers");
    printer.println();
    paragraph =
        "The lines in a text file are numbered in one of two ways: actual and ledgerable. Actual " +
        "line numbers are just the ordinary line numbers you see in a typical " +
        "text editor that start from 1. However, not all lines in the text file figure in the " +
        "journal's state: blank, empty, or comment lines do not count. Ledgerable " +
        "line numbers exclude these ignored lines from the accounting.";
    printer.printParagraph(paragraph);
    printer.println();
    printer.println("Row Numbers");
    printer.println();
    paragraph =
        "These denote a row in the backing skip ledger. Since some rows are beacon rows (used " +
        "to establish maximum row age, see next) there are usually a few more rows in the ledger than there are " +
        "lines in the file. Row numbers too start from 1.";
    printer.printParagraph(paragraph);
    printer.println();
    printer.println("Beacon Rows");
    printer.println();
    paragraph =
        "These rows contain special hash values that have nothing to do with the text file. " +
        "They record the minimum creation-date (maximum age) of subsequent rows by recording the root hash " +
        "of the most recent merkle tree published by crums.io. Since it cannot be computed in " +
        "advance, it functions as a beacon.";
    printer.printParagraph(paragraph);
    printer.println();
    printer.println();
    printer.println("Trailed Rows");
    printer.println();
    paragraph =
        "A row whose hash has been witnessed, evidenced by a crumtrail attachment. Both beacon- and regular rows may be " +
        "witnessed this way. Trailed rows establish maximum creation-date " +
        "(minimum age) for both themselves and every row before them. This is " + 
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
        "In addition to those explicitly below, each command takes the journaled text file as argument. " +
        "By default the ledger (the opaque database that backs the journal) is found in the same directory " +
        "as the text file and is named the same as the text file, but with an added '" + EXTENSION + "' extension.";
    printer.printParagraph(paragraph);
    printer.println();

    TablePrint table = new TablePrint(out, LEFT_TBL_COL_WIDTH, RIGHT_TBAL_COL_WIDTH);
    table.setIndentation(INDENT);
    
    table.printRow(CREATE, "creates a new journal for the given text file.");
    table.println();
    table.printRow(STATUS, "prints the status of the journaled text file");
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
    
    table.printRow(MAKE_MORSEL, "creates a morsel file containing the contents of the given");
    table.printRow(null,   "given ledgered line number(s)");
    table.printRow(null,   "Args:");
    table.println();
    table.printRow(null,   "<line-numbers>  (required)");
    table.println();
    table.printRow(null,   "Strictly ascending line numbers separated by commas (no spaces)");
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
  private final static String UPDATE = "update";
  private final static String TRIM = "trim";
  
  
  private final static String MAKE_MORSEL = "make-morsel";
//  private final static String READ_MORSEL = "read-morsel";
//  private final static String JURNO_STATE = "jurno-state";
  
  private final static String FILE = "file";
  
  //  - - O P T I O N S - -
  
  private final static String LD = "-ld";

}





















