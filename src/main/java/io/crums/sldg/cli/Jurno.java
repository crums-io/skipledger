/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cli;


import static io.crums.util.Strings.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import io.crums.io.Opening;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.db.Db;
import io.crums.sldg.demo.jurno.Journal;
import io.crums.util.main.ArgList;
import io.crums.util.main.MainTemplate;
import io.crums.util.main.PrintSupport;
import io.crums.util.main.StdExit;
import io.crums.util.main.TablePrint;

/**
 * Manages a {@linkplain Journal journal}ed text file from the command line.
 */
public class Jurno extends MainTemplate {
  
  private final static String PROG_NAME = Jurno.class.getSimpleName().toLowerCase();
  private final static String EXTENSION = SldgConstants.DB_EXT;
  
  
  public static void main(String[] args) {
    Jurno jurno = new Jurno();
    jurno.doMain(args);
  }
  
  
  
  private File textFile;
  private File ledgerDir;
  private Opening opening;
  private String command;
  
  private boolean addBeacon;
  private boolean abortOnFork;
  
  private File morselFile;
  private Journal journal;
  
  
  

  /**
   * 
   */
  private Jurno() {
  }

  @Override
  protected void init(String[] args) throws IllegalArgumentException, Exception {
    ArgList argList = new ArgList(args);
    List<String> cmd = argList.removeContained(
        CREATE, STATUS, UPDATE, TRIM, MAKE_MORSEL, READ_MORSEL);
    if (cmd.isEmpty())
      throw new IllegalArgumentException("no command specified");
    if (cmd.size() != 1)
      throw new IllegalArgumentException("only one command at a time may be given: " + cmd);
    this.command = cmd.get(0);
    
    
    
    switch (command) {
    case STATUS:
    case MAKE_MORSEL:
      this.opening = Opening.READ_ONLY; break;
    case TRIM:
    case UPDATE:
      opening = Opening.READ_WRITE_IF_EXISTS; break;
    case CREATE:
      opening = Opening.CREATE; break;
    case READ_MORSEL:
      List<File> files = argList.removeExistingFiles();
      if (files.isEmpty())
        throw new IllegalArgumentException("no morsel file given");
      if (files.size() > 1)
        throw new IllegalArgumentException("ambiguous, multiple files given: " + files);
      this.morselFile = files.get(0);
    }
    
    if (opening != null) {
      setJournoFiles(argList);
    }
    
    
  }
  
  
  
  private void setJournoFiles(ArgList args) {
    List<File> textSource = args.removeExistingFiles();
    if (textSource.isEmpty())
      throw new IllegalArgumentException("requried text file is missing");
    
    else if (textSource.size() > 1)
      throw new IllegalArgumentException("only one text file can be journaled: " + textSource);
    
    this.textFile = textSource.get(0);
    
    if (!args.removeContained(LD).isEmpty()) {
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
      } else if (ledgerDir.isFile())
        throw new IllegalArgumentException("ledger directory given is actaully a file: " + ledgerDir);
    
    } else {
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
        if (addBeacon)
          db.addBeacon();
        
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
            message = textFile + " has forked from its ledger at line-no " + textLineNo;
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
      }
    }
  }


  protected void printStatus() {
    
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
        "Command line tool for creating and maintaining a tamper-proof, journaled, " +
        "append-only text file using a skip ledger. Supports line-by-line extractions " +
        "from the text file, annotated with tamper-proof timestamps that can be verified " +
        "against a compact, opaque hash structure that captures the state of the text file.";
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
        "Commands in the table below each take a filename (file path) as an " +
        "argument. The filename is the name of the journaled text file, not the similarly named ledger directory. By default the ledger " +
        "(an opaque database that backs the journal) is found in the same directory " +
        "as the text file and is named the same as the text file but with an additional '" +
        EXTENSION + "' filename extension.";
    printer.printParagraph(paragraph);
    printer.println();

    TablePrint table = new TablePrint(out, LEFT_TBL_COL_WIDTH, RIGHT_TBAL_COL_WIDTH);
    table.setIndentation(INDENT);
    
    table.printRow(CREATE, "creates a new journal for the given text file.");
    table.println();
    table.printRow(STATUS, "prints the status of the journal associated with the given");
    table.printRow(null,   "text file");
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
    table.printlnCenteredSpread("OUTPUT / PRINT", 2);
    table.printHorizontalTableEdge('-');
    out.println();
    
    table.printRow(MAKE_MORSEL, "prints or outputs a morsel file containing the contents of");
    table.printRow(null,   "the given ledgered line number(s). Additonal parameters:");
    table.println();
    table.printRow(READ_MORSEL, "reads/verifies the contents of a morsel file");
    table.println();
    table.printRow(JURNO_STATE, "prints or outputs structured hashes encapsulating all the");
    table.printRow(null,   "ledgered lines in the journal in an opaque way.");
    table.printRow(null,   "");
    table.println();
    
    table.printHorizontalTableEdge('-');
    table.printlnCenteredSpread("OPTIONS", 2);
    table.printHorizontalTableEdge('-');
    table.println();
    
    table.printRow(LD,    "sets the ledger directory");
    table.printRow(null,  "DEFAULT: same as the text file, but with a '" + EXTENSION + "' extension");
    table.println();
//    table.printRow(null,   "Line number(s):");
//    table.printRow(null,   "==============");
//    table.println();
//    table.printRow(null,   "Lines are numbered in one of two ways: actual and ledgerable. Actual");
//    table.printRow(null,   "line numbers are just the ordinary line numbers you see in a typical");
//    table.printRow(null,   "text editor. However, not all lines in the text file figure in the");
//    table.printRow(null,   "journal's state: blank, empty, or comment lines do not count: the");
//    table.printRow(null,   "ledgerable line numbers exclude these.");
//    table.println();
    
  }
  
  
  //  - - C O M M A N D S - -
  
  private final static String CREATE = "create";
  private final static String STATUS = "status";
  private final static String UPDATE = "update";
  private final static String TRIM = "trim";
  /*
   * TODO:
   * Whatever you name this..
   * 1) write path: lines (line range)
   * 2) read path:
   *  *  verify against journal skip path
   *  * use common hash grammar
   * 
   */
  private final static String MAKE_MORSEL = "make-morsel";
  private final static String READ_MORSEL = "read-morsel";
  private final static String JURNO_STATE = "jurno-state";
  
  
  //  - - O P T I O N S - -
  
  private final static String LD = "-ld";

}





















