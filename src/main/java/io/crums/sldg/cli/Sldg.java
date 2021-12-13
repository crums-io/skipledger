/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cli;

import static io.crums.util.Strings.*;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import io.crums.client.ClientException;
import io.crums.sldg.Ledger.State;
import io.crums.sldg.SourceLedger;
import io.crums.sldg.sql.Config;
import io.crums.sldg.sql.ConfigFileBuilder;
import io.crums.sldg.sql.ConnectionInfo;
import io.crums.sldg.sql.SqlLedger;
import io.crums.sldg.sql.SqlSourceQuery;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.SourceInfo;
import io.crums.sldg.time.TrailedRow;
import io.crums.util.IntegralStrings;
import io.crums.util.Strings;
import io.crums.util.cc.ThreadUtils;
import io.crums.util.main.ArgList;
import io.crums.util.main.PrintSupport;
import io.crums.util.main.StdExit;
import io.crums.util.main.TablePrint;
import io.crums.util.ticker.Progress;

/**
 * Manages a ledger formed from a relational database table or view that is
 * operationally append-only.
 */
public class Sldg extends BaseMain {

  /**
   * @param args
   */
  public static void main(String[] args) {
    new Sldg().doMain(args);
  }
  
  

  private class RowArg {

    List<Long> rowNums = Collections.emptyList();
    
    
    
    void setRowNums(ArgList args) {
      this.rowNums = getRowNums(args);
    }
    
  }
  
  
  
  private class MorselArg extends RowArg {
    
    File morselFile = new File(".");
    SourceInfo sourceInfo;
    
    List<Integer> redactCols = Collections.emptyList();
    
    void setArgs(ArgList args, boolean state) {
      
      if (!state) {
        setRowNums(args);
        redactCols = getRedactColumns(args);

        this.sourceInfo = getMeta(args);
        if (sourceInfo == null)
          sourceInfo = config.getSourceInfo().orElse(null);
      }

      
      switch (args.argsRemaining().size()) {
      case 0: return;
      case 1: break;
      default:
        throw new IllegalArgumentException("too many arguments: " + args.getArgString());
      }
      
      this.morselFile = new File(args.removeFirst());
      File parent = morselFile.getParentFile();
      if (parent != null && !parent.exists())
        throw new IllegalArgumentException(
            "expected parent directory for morsel file does not exist: " + this.morselFile);
      
      if (morselFile.isFile())
        throw new IllegalArgumentException("morsel file already exists: " + morselFile);
      
    }
    
    
    
  }
  
  
  
  
  
  
  
  
  private String command;
  
  private RowArg listArgs;
  
  private MorselArg morselArgs;
  
  private Config config;
  
  private SqlLedger ledger;
  
  private long updateCount;
  
  private long conflictRow;
  
  
  @Override
  protected void close() {
    if (ledger != null)
      ledger.close();
  }

  @Override
  protected void init(String[] args) throws IllegalArgumentException, Exception {
    ArgList argList = newArgList(args);
    
    this.command = argList.removeCommand(
        SETUP, CREATE, STATUS, LIST, HISTORY, UPDATE, WITNESS, VALIDATE, ROLLBACK,
        MAKE_MORSEL, STATE_MORSEL);
    
    if (SETUP.equals(command)) {
      argList.enforceNoRemaining();
      return;
    }
    
    // all other commands require the config file
    loadConfig(argList);
    
    switch (command) {
    case UPDATE:
      {
        var nums = argList.removeNumbers();
        if (nums.size() > 1)
          throw new IllegalArgumentException(
              "too many number arguments with '" + UPDATE + "' command: " + nums);
        
        this.updateCount = nums.isEmpty() ? Long.MAX_VALUE : nums.get(0);
      }
      break;

    case WITNESS:
    case CREATE:
    case STATUS:
    case HISTORY:
    case VALIDATE:
      break;
    
    case LIST:
      listArgs = new RowArg();
      listArgs.setRowNums(argList);
      break;
      
    case ROLLBACK:
      initLedger();
      {
        String numArg = argList.removeFirst();
        argList.enforceNoRemaining();
        
        if (numArg == null && !ledger.getState().isTrimmed()) {
          throw new IllegalArgumentException("missing <conflict-number>");
        }
        
        if (numArg != null) {
          try {
            this.conflictRow = Long.parseLong(numArg);
          } catch (NumberFormatException nfx) {
            throw new IllegalArgumentException(
                "expected <conflict-number>; actual given: " + numArg);
          }
          
          if (conflictRow < 2)
            throw new IllegalArgumentException("<conflict-number> " + conflictRow + " < 2");
          
          long hashLdgrSz = ledger.hashLedgerSize();
          if (conflictRow > hashLdgrSz)
            throw new IllegalArgumentException(
                "<conflict-number> " + conflictRow +
                " > hash ledger size (" + hashLdgrSz + ")");
          
          if (ledger.checkRow(conflictRow) && ledger.lastValidRow() > conflictRow)
            throw new IllegalArgumentException(
                "no conflict found at/before row [" + conflictRow + "]");
        
        } else if (!ledger.getState().isTrimmed())
          throw new IllegalArgumentException("missing <conflict-number>");
        
      }
      break;
      
    case STATE_MORSEL:
    case MAKE_MORSEL:
      this.morselArgs = new MorselArg();
      morselArgs.setArgs(argList, STATE_MORSEL.equals(command));
      break;
    }

    argList.enforceNoRemaining();
  }
  
  private void loadConfig(ArgList argList) {
    File configFile = argList.removeExistingFile();
    if (configFile == null)
      throw new IllegalArgumentException("no config file found in arguments");
    this.config = new Config(configFile);
  }

  @Override
  protected void start() throws InterruptedException, Exception {
    switch (command) {
    case SETUP:
      setup();
      break;
    case STATUS:
      status();
      break;
    case CREATE:
      create();
      break;
    case LIST:
      list();
      break;
    case HISTORY:
      history();
      break;
    case UPDATE:
      update();
      break;
    case WITNESS:
      witness();
      break;
    case VALIDATE:
      validate();
      break;
    case ROLLBACK:
      rollback();
      break;
    case STATE_MORSEL:
    case MAKE_MORSEL:
      morsel();
      break;
      
    }
  }
  
  
  
  void validate() {
    initLedger();
    long rows = ledger.lesserSize();
    if (rows == 0) {
      printf("%nNothing to validate.%n");
      status();
      return;
    }
    
    printf("%nValidating %d %s%n", rows, pluralize("row", rows));
    
    // FIXME: this needlessly overflows at about 45B rows
    ledger.setProgressTicker(Progress.newStdOut(rows));
    
    State state = ledger.validateState(false);
    if (state.needsMending()) {
      printf("WARNING (!):%n");
      needsMendingMessage();;
    } else {
      printf("%nOK. All ledgered rows verified.%n");
    }
    
  }
  
  
  void morsel() throws IOException {
    initLedger();
    if (this.ledger.getState().needsMending())
      throw new IllegalStateException(
          "source ledger conflicts with hash ledger: " + ledger.getState());
    
    File file = ledger.writeMorselFile(
        morselArgs.morselFile,
        morselArgs.rowNums,
        null,   // no note
        morselArgs.redactCols,
        morselArgs.sourceInfo);
    int entries = morselArgs.rowNums.size();
    if (entries == 0)
      System.out.println("state path written to morsel: " + file);
    else
      System.out.println(entries + pluralize(" row", entries) + " written to morsel: " + file);
  }
  
  
  private final static String NULL_SYMBOL = "<NULL>";
  private final static String COLUMN_DIVISOR = " | ";
  
  void list() {
    initLedger();
    TablePrint table;
    {
      long lastRn = listArgs.rowNums.get(listArgs.rowNums.size() - 1);
      int decimalWidth = Math.max(3,  Long.toString(lastRn).length());
      table = new TablePrint(decimalWidth + 3, 77 - decimalWidth);
    }
    
    SourceLedger src = this.ledger.getSourceLedger();
    
    final long max = src.size();
    
    StringBuilder s = new StringBuilder(256);
    
    int count = 0;
    for (long rn : listArgs.rowNums) {
      ++count;
      if (rn > max) 
        break;
      var srcRow = src.getSourceRow(rn);
      List<ColumnValue> columns = srcRow.getColumns();
      s.setLength(0);
      for (var col : columns) {
        if (col.getType().isNull())
          s.append(NULL_SYMBOL);
        else {
          col.appendValue(s);
        }
        s.append(COLUMN_DIVISOR);
      }
      // remove the last divisor
      s.setLength(s.length() - COLUMN_DIVISOR.length());
      table.printRow("[" + rn + "]", s);
    }
    table.println();
    table.println(count + pluralize(" row", count) + " listed");
    if (count < listArgs.rowNums.size()) {
      int skipped = listArgs.rowNums.size() - count;
      table.println("(" + skipped + pluralize(" row argument", skipped) + " beyond last row)");
    }
    table.println();
  }

  void create() {
    printf("creating backing hash ledger tables (3)%n");
    this.ledger = SqlLedger.declareNewInstance(config);
    printf("%nDone.%n");
    status();
  }
  
  
  void update() {
    initLedger();
    var state = ledger.getState();
    switch (state) {
    case FORKED:
    case TRIMMED:
      status();
      break;
    case PENDING:
      long rowsAdded = ledger.update(updateCount);
      printf("%n%d %s added%n%n", rowsAdded, pluralize("source row", rowsAdded));
    case COMPLETE:
      
      witness();
    }
  }
  
  
  
  void witness() {
    initLedger();
    try {
      var witReport = ledger.witness();
      if (witReport.nothingDone()) {
        if (ledger.getState().isComplete() && UPDATE.equals(command))
          printf("%nUp to date.%n");
        else {
          printf("All rows recorded in hash ledger already witnessed.%n");
        }
      } else {
        int crums = witReport.getRecords().size();
        int crumtrails = witReport.getStored().size();
        printf(
            "%n%d %s submitted; %d %s (%s) stored%n%n",
            crums, pluralize("crum", crums),
            crumtrails, pluralize("crumtrail", crumtrails),
            pluralize("witness record", crumtrails));
        if (crumtrails == 0)
          printf("Run '%s' in a few minutes", WITNESS);
      }
    } catch (ClientException cx) {
      System.out.printf(
          "%nEncountered a netword error while attempting to have the ledger witnessed%n" +
          "[%d] is the last witnessed row%n%n", ledger.lastWitnessedRowNumber());
      throw cx;
    }
    printf("%n");
  }
  
  
  void rollback() {
    initLedger();
    initConsole();
    
    assert ledger.getState().needsMending();
    
    final long postSize;
    final long rowsToLose;
    if (conflictRow == 0) {
      assert ledger.getState().isTrimmed();
      postSize = ledger.sourceLedgerSize();
      rowsToLose = ledger.hashLedgerSize() - postSize;
    } else {
      postSize = conflictRow - 1;
      rowsToLose = ledger.hashLedgerSize() - postSize;
    }
    
    final int preTrailCount = ledger.getHashLedger().getTrailCount();
    var trail = ledger.getHashLedger().nearestTrail(postSize + 1);
    
    printf(
        "WARNING: You are about to erase the last %d %s from the hash ledger!%n",
        rowsToLose, pluralize("row", rowsToLose));
    
    if (trail != null) {
      printf(
        "         Crumtrails (witness records) dating back to row [%d] will also be lost:%n", trail.rowNumber());
      printf(
        "         (witnessed on %s)%n", new Date(trail.utc()));
    }
    
    printf("%nConfirm rollback [y]:%n");

    String ack = readLine();
    boolean yes = "y".equalsIgnoreCase(ack) || "yes".equalsIgnoreCase(ack);
    if (!yes) {
      printf("%nOK. Rollback aborted.%n");
      return;
    }
    
    if (conflictRow == 0)
      ledger.rollback();
    else
      ledger.rollback(conflictRow);
    
    final int trailsLost = preTrailCount - ledger.getHashLedger().getTrailCount();
    
    printf(
        "%n%d %s rolled back; %d %s lost.%n",
        rowsToLose, pluralize("row", rowsToLose),
        trailsLost, pluralize("crumtrail", trailsLost));
        
  }
  
  
  void history() {
    initLedger();
    
    var table = trailTablePrint();
    
    int witnessedRows = this.ledger.getHashLedger().getTrailCount();
    
    for (int index = 0; index < witnessedRows; ++index) {
      table.println();
      printTrailDetail(index, table);
    }
    table.println();
    long ledgeredRows = ledger.hashLedgerSize();
    table.println(ledgeredRows + pluralize(" ledgered row", ledgeredRows));
    table.println(
        "state witnessed and recorded at " +  witnessedRows + pluralize(" row", witnessedRows));
    table.println();
  }



  private final static int RM = 80;
  private final static int LEFT_STATUS_COL_WIDTH = 16;
  private final static int RIGHT_STATUS_COL_WIDTH = 18;
  private final static int MID_STATUS_COL_WIDTH = RM - LEFT_STATUS_COL_WIDTH - RIGHT_STATUS_COL_WIDTH;
  
  
  private TablePrint trailTablePrint() {
    return new TablePrint(
        LEFT_STATUS_COL_WIDTH,
        MID_STATUS_COL_WIDTH,
        RIGHT_STATUS_COL_WIDTH);
  }
  

  void status() {
    initLedger();
    
    var out = System.out;
    
    final long rowsRecorded = ledger.hashLedgerSize();
    final int trails = ledger.getHashLedger().getTrailCount();
    out.printf("%n%d %s recorded in hash ledger%n", rowsRecorded, pluralize("row", rowsRecorded));
    
    out.printf("%d %s%n", trails, pluralize("crumtrail", trails));
    
    State state = ledger.getState();
    if (state.needsMending()) {
      
      needsMendingMessage();
      printf("%nRun with the '" + VALIDATE + "' command to determine the first modified source row.%n");
    
    } else {
      final long unwitnessRows = ledger.getHashLedger().unwitnessedRowCount();
      if (state.isPending()) {
        long pending = ledger.rowsPending();
        out.printf("%n%d %s in source ledger not yet recorded%n", pending, pluralize("row", pending));
        
      } else {
        // assert state.isComplete();
        out.printf("%nhash ledger is up to date with source ledger%n");
        if (unwitnessRows == 0) {
          out.printf("all row hashes witnessed%n");
        } else {
          out.printf("%d remaining row %s to witness%n", unwitnessRows, pluralize("hash", unwitnessRows));
        }
      }
      if (trails != 0) {
        TablePrint table = trailTablePrint();
        table.println();
        String heading = "First";
        boolean single = trails == 1;
        if (single)
          heading += " (and only)";
        heading += " crumtrail";
        table.printRow(null, heading);
        printTrailDetail(0, table);
        
        if (!single) {
          table.println();
          table.printRow(null, "Last crumtrail");
          printTrailDetail(trails - 1, table);
        }
        table.println();
      }
      
      System.out.printf(
          "%nledger state hash (row [%d]):%n%s%n",
          rowsRecorded,
          IntegralStrings.toHex(ledger.getHashLedger().getSkipLedger().stateHash()));
    }
  }
  
  
  private void needsMendingMessage() {
    if (ledger.getState().isForked()) {
      long fc = ledger.getFirstConflict();
      printf("%nrow [%d] has been modified%n", fc);
      printf("%nTo fix, either%n");
      printf("  a) restore the source row [%d] to its previous state, or%n", fc);
      printf("  b) rollback to before conflict row [%d] in the hash ledger]%n", fc);
      
    } else {
      // assert state.isTrimmed();
      long trimmed = ledger.sourceLedgerSize();
      long missing = ledger.hashLedgerSize() - trimmed;
      printf("%nsource ledger has been trimmed to %d %s%n", trimmed, pluralize("row", trimmed));
      printf("%nTo fix, either%n");
      printf("  a) restore the last %d missing source %s, or%n", missing, pluralize("row", missing));
      printf("  b) trim the hash ledger (to row [%d])%n", trimmed);
      
    }
  }
  
  
  private void printTrailDetail(int index, TablePrint table) {
    TrailedRow trailedRow = ledger.getHashLedger().getTrailByIndex(index);
    long utc = trailedRow.utc();
    table.printRow("row #: ", ledger.getHashLedger().getTrailedRowNumbers().get(index));
    table.printRow("witnessed:", new Date(utc), "UTC: " + utc);
    table.printRow("trail root:", IntegralStrings.toHex(trailedRow.trail().rootHash()));
    table.printRow("ref URL:", trailedRow.trail().getRefUrl());
  }
  



  private void initLedger() {
    if (ledger == null)
      this.ledger = SqlLedger.loadInstance(config);
  }


  private ConfigFileBuilder builder;
  private Console console;
  
  private void initConsole() {
    this.console = System.console();
    if (console == null) {
      System.err.println("[ERROR] No console. Aborting.");
      StdExit.GENERAL_ERROR.exit();
    }
  }

  void setup() {
    initConsole();
    console.printf("%nEntering interactive mode..%n");
    printf("Enter <CTRL-c> to abort at any time.%n%n");
    PrintSupport printer = new PrintSupport();
    var paragraph =
        "The steps to create a valid (or at least well-formed) configuration file for this program consist " +
        "of configuring a connection to the source ledger (the append-only table), optionally " +
        "(recommended) a separate database connection for the hash-ledger, and then declaring which " +
        "columns (and in what order) define a source-row's data to be tracked.";
    printer.printParagraph(paragraph);
    printer.println();
    paragraph =
        "2 connection URLs are recommended, instead of just the one, because that way " +
        "it's possible to configure a read-only connection for the source-table while allowing a " +
        "read/write connection dedicated to the hash-ledger tables that track it.";
    printer.printParagraph(paragraph);
    printer.println();
    printer.println("Here's an outline of what we'll be doing..");
    printer.println();
    printer.setIndentation(2);
    printer.println("1. Configure DB connection to source table (the ledger source)");
    printer.println("  1.a JDBC URL configuration (Required)");
    printer.println("  1.b Connection name/value paramaters (Optional if already in URL)");
    printer.println("  1.c JDBC Driver class (Optional if already registered at boot time)");
    printer.println("  1.d JDBC Driver classpath (Optional if already defined at boot time)");
    printer.println();
    printer.println("2. Configure DB connection for hash ledger tables as above (Optional)");
    printer.println();
    printer.println("3. Configure source- and hash-ledger schemas");
    printer.println("  3.a Source table (or view) name");
    printer.println("  3.b Source table PRIMARY_KEY column");
    printer.println("  3.c Other source table columns");
    printer.println();
    printer.println("4. Save the file. Default schema definitions for the hash tables are saved");
    printer.println("   Depending on the SQL flavor of your DB vendor, you may need to modify these.");
    printer.println();
    
    File targetConfigFile = getTargetConfigFile();
    this.builder = new ConfigFileBuilder(targetConfigFile);
    setSrcConUrl();
    setSrcConInfo();
    if (setSrcDriverClass())
      setSrcDriverClasspath();
    testSrcCon();
    if (setHashConUrl()) {
      setHashConInfo();
      if (setHashDriverClass())
        setHashDriverClasspath();
    } 
    setSourceTablename();
    setSourceColumns();
    printf("%n");
    printf("Generating random salt%n");
    printf("%n");
    builder.seedSourceSalt();
    printf("%n");
    paragraph =
        "Generating default schema definitions (CREATE TABLE statements) for the hash-ledger. " +
        "These are simple enough to work on most any database engine. Still, if you need to define " +
        "these schemas some other way you can edit the configuration file (or create the " +
        "tables on the database yourself).";
    printer.setIndentation(0);
    printer.printParagraph(paragraph);
    builder.setDefaultHashSchemas();
    saveConfigFile();
  }





  private void saveConfigFile() {
    
    var path = builder.getConfigFile().getPath();
    printf("Saving configuration to%n  <%s>", path);
    try {
      builder.save();
      
    } catch (Exception x) {
      printf("%nERROR: on attempt to save <%s>%n%s%n", path, x);
    }
    printf("%n");
    printf("%nDone.%n");
    printf("%n");
    String note =
        "Review the contents of the config file (adjust per SQL dialect) and then invoke the '" + CREATE + "' command. " +
        "You can modify the rows and columns pulled in on the SELECT \"by row number\" statement there. For example, " +
        "you may denormalize the data (pull in other column values related to FOREIGN KEYs). " +
        "Note you can redact any column when outputing a morsel, and that the storage overhead in the hash ledger is small and fixed per row, regardless how " +
        "many columns are returned in the SELECT statement.";
    new PrintSupport().printParagraph(note);
    printf("%n");
  }




  private void setSourceColumns() {
    printf("%nGood.");
    printf(" Now name the column names in table%n   <%s>%n", builder.getHashTablePrefix());
    
    var paragraph =
        "that are to be tracked. " +
        "The first of these should be the table's PRIMARY KEY column " +
        "(or the equivalent of). The values in this column must be monotonically " +
        "increasing, whenever new rows are appended to the table. There are no restrictions " +
        "on the other columns (most any SQL data type, or NULL value should work).";
    
    PrintSupport printer = new PrintSupport();
    printer.printParagraph(paragraph);
    printer.println();
    
    for (int count = MAX_TRIALS; count-- > 0; ) {
      printf("Enter the column names (separate with spaces):%n");
      String line = readLine();
      if (line.isEmpty())
        continue;
      var tokenizer = new StringTokenizer(line);
      final int cc = tokenizer.countTokens();
      if (cc < 2) {
        printf("Need at least 2 column names.. Try Again.%n");
        continue;
      }
      
      ArrayList<String> columnNames = new ArrayList<>(cc);
      while (tokenizer.hasMoreTokens()) {
        String colName = tokenizer.nextToken();
        if (malformedColumnname(colName)) {
          printf("Woops, with '%s' (quoted column-name).. Try Again.%n", colName);
          break;
        }
        columnNames.add(colName);
      }
      if (columnNames.size() != cc)
        continue; // (user input error already notified)
      
      var queryBuilder = new SqlSourceQuery.DefaultBuilder(
          builder.getHashTablePrefix(),
          columnNames);
      
      builder.setSourceSizeQuery(queryBuilder.getPreparedSizeQuery());
      builder.setSourceRowQuery(queryBuilder.getPreparedRowByNumberQuery());
      printf("%nExcellent.");
      return;
    }
    
    giveupAbort();
  }

  
  /**
   * {@code printf} with delay to draw attention.
   */
  private void printf(String format, Object... args) {
    try {
      ThreadUtils.ensureSleepMillis(100);
    } catch (InterruptedException ix) {
      Thread.currentThread().interrupt();
    }
    if (console == null)
      System.out.printf(format, args);
    else
      console.printf(format, args);
  }


  private void setSourceTablename() {
    for (int count = MAX_TRIALS; count-- > 0; ) {
      printf("%nEnter the name of the table that is to be tracked:%n");
      String table = readLine();
      if (table.isEmpty())
        continue;
      if (malformedTablename(table)) {
        printf("%nThat can't be right. Try Again.");
        continue;
      }
      builder.setHashTablePrefix(table);
      return;
    }
    giveupAbort();
  }



  private boolean malformedTablename(String table) {
    // stub
    return table.length() < 2 || malformedSqlThing(table);
  }
  
  
  private boolean malformedColumnname(String column) {
    // stub
    return malformedSqlThing(column);
  }
  
  private boolean malformedSqlThing(String thing) {
    if (!Strings.isAlphabet(thing.charAt(0)))
      return true;
    for (int index = 1; index < thing.length(); ++index) {
      char c = thing.charAt(index);
      boolean ok = Strings.isAlphabet(c) || Strings.isDigit(c) || c == '_';
      if (!ok)
        return true;
    }
    return false;
  }



  private final static int MAX_TRIALS = 3;


  private File getTargetConfigFile() {
    File target = null;
    int count = MAX_TRIALS;
    int blanks = 0;
    while (target == null && count-- > 0) {
      printf("Enter the destination path:%n");
      String path = readLine();
      
      if (path.isEmpty()) {
        if (++blanks < 2)
          ++count;
        continue;
      } else
        blanks = 0;
      
      try {
        target = new File(path);
        if (target.exists()) {
          target = null;
          if (count > 0)
            printf("%nWoops.. that file path already exists. Try again.%n");
        }
      } catch (Exception x) {
        if (count > 0)
          printf(
              "%nThat doesn't seem to work. (Error msg: %s)%n" +
              "Try again.%n", x.getMessage());
      }
      if (target == null)
        giveupAbort();
    }
    return target;
  }
  
  
  private String readLine() {
    String line = console.readLine();
    if (line == null)
      giveupAbort();
    return line.trim();
  }
  
  
  private void giveupAbort() {
    System.err.println();
    System.err.println("OK, giving up. Aborting.");
    System.err.println();
    StdExit.ILLEGAL_ARG.exit();
  }
  
  
  private void setSrcConUrl() {
    printf("%nGood.");
    printf(
        " We need a connection URL to the database the source table lives in.%n" +
        "For e.g. jdbc:postgres.. (We'll add parameters and credentials after.)%n%n");
    int blanks = 0;
    for (int count = MAX_TRIALS; count-- > 0; ) {
      printf("Enter the connection URL for the source table:%n");
      String url = readLine();
      if (url.isEmpty()) {
        if (++blanks < 2)
          ++count;
        continue;
      } else
        blanks = 0;
      if (malformedUrl(url)) {
        console.printf("Not a valid URL. Try Again.%n");
        continue;
      }
      
      builder.setSourceUrl(url);
      
      printf("%nNice. ");
      break;
    }
  }
  
  
  private boolean malformedUrl(String url) {
    return url.length() < 10 || ! url.toLowerCase().startsWith("jdbc:");
  }
  
  
  private void setSrcConInfo() {
    printf("How about credentials and other parameters for this database?%n");
    printf(
        "These are passed in as name/value pairs. For example%n" +
        " user=admin%n pwd=123%n" +
        "If your driver supports it, you can set this connection to read-only (e.g.%n" +
        "readonly=true).%n%n");
    printf("Enter as many name/value pairs as needed followed by an empty line:%n");
    
    doAddConInfoLoop(
        (name,value) -> { builder.setSourceConProperty(name, value); return Boolean.TRUE;},
        "source");
    
  }
  
  
  
  private void doAddConInfoLoop(BiFunction<String,String,?> func, String conName) {

    int count = MAX_TRIALS;
    var tally = new HashMap<>();
    while (true) {
      String line = readLine();
      if (line.isEmpty()) {
        int props = tally.size();
        if (props < 2) {
          // confirm
          String msg = props == 0 ? "Confirm no parameters" : "Confirm just 1 paramater";
          msg += " for " + conName + " connection [y]:%n";
          printf(msg);
          String ack = readLine();
          boolean yes = "y".equalsIgnoreCase(ack) || "yes".equalsIgnoreCase(ack);
          
          if (!yes) {
            printf("%nOK, continue adding name/value pairs, followed by an empty line:%n");
            continue;
          }
        }
        break;
      }
      int delimiterIndex = delimiterIndex(line);
      if (delimiterIndex == -1) {
        warnInvalidProp(--count);
        continue;
      }
      String name = line.substring(0, delimiterIndex).trim();
      String value = line.substring(delimiterIndex + 1, line.length()).trim();
      if (name.isEmpty() || value.isEmpty()) {
        warnInvalidProp(--count);
        continue;
      }
      func.apply(name, value);
//      builder.setSourceConProperty(name, value);
      Object oldValue = tally.put(name, value);
      if (oldValue != null)
        console.printf("(Overwrote old value '%s')%n", oldValue);
      count = MAX_TRIALS;
    }
    int props = tally.size();
    String msg = props < 2 ? "%nOK, " : "%nGreat. ";
    msg += props + pluralize(" name/value pair", props) + " added.%n%n";
    printf(msg);
  }
  




  private void setHashConInfo() {
    printf(
        "Enter credentials and other parameters for this DB connection as you did for the%n" +
        "connection to the source-table's DB.%n%n");
    
    printf("Enter as many name/value pairs as needed followed by an empty line:%n");
    
    doAddConInfoLoop(
        (name,value) -> { builder.setHashConProperty(name, value); return Boolean.TRUE;},
        "hash-ledger");
  }


  private boolean setDriverClass(String url, Consumer<String> func) {
    printf(
        "%nDo you need to set the JDBC Driver class associated with the connection URL%n <%s> ?%n%n",
        url);
    printf(
        "You can specify the Driver class (and then its classpath) here, if it's not%n" +
        "pre-registered with the Runtime's DriverManager.%n%n");
    
    while (true) {
      printf("Enter the driver class name (enter blank to skip):%n");
      String line = readLine();
      if (line.isEmpty()) {
        console.printf("%nNot setting any driver class / classpath.%n%n");
        return false;
      }
      if (Strings.isPermissableJavaName(line)) {
        func.accept(line);
//        builder.setSourceDriverClass(line);
        return true;
      }
      console.printf("%nNot a valid Java class name. Try again:%n");
    }
  }


  private boolean setHashDriverClass() {
    return setDriverClass(
        builder.getHashUrl(),
        s -> builder.setHashDriverClass(s));
  }
  
  
  


  private void setHashDriverClasspath() {
    setDriverClasspath(
        builder.getHashDriverClass(),
        s -> builder.setHashDriverClasspath(s));
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  private int delimiterIndex(String line) {
    int equalIndex = supIndexOf(line, '=');
    int colonIndex = supIndexOf(line, ':');
    int supIndex = Math.min(equalIndex, colonIndex);
    return supIndex == line.length() ? -1 : supIndex;
  }
  
  private int supIndexOf(String line, char c) {
    int index = line.indexOf(c);
    return index == -1 ? line.length() : index;
  }
  
  private void warnInvalidProp(int countdown) {
    warn("That's not a valid name/value pair. Try again:%n", countdown);
  }

  private void warn(String message, int countdown) {
    if (countdown > 0)
      console.printf(message);
    else
      giveupAbort();
  }

  private boolean setSrcDriverClass() {
    return setDriverClass(
        builder.getSourceUrl(),
        s -> builder.setSourceDriverClass(s));
  }


  private void setSrcDriverClasspath() {
    setDriverClasspath(
        builder.getSourceDriverClass(),
        s -> builder.setSourceDriverClasspath(s));
  }
  
  
  private void setDriverClasspath(String driverClass, Consumer<String> func) {
    console.printf(
        "%nGreat. Do you need to set the .jar file from which%n     <%s>%nwill be loaded?%n%n" +
        "You can set the path to its .jar file either absolutely, or relative to the%n" +
        "(parent directory of the) configuration file.%n%n" +
        
        "Enter the location of the .jar file (enter blank to skip):%n",
        driverClass);
    
    
    while (true) {
      String path = readLine();
      if (path.isEmpty())
        break;
      if (!path.endsWith(".jar")) {
        console.printf("Error. Must be a .jar file. Try again:%n");
        continue;
      }
      
      try {
        File resolvedPath = new File(path);
        boolean relative = !resolvedPath.isAbsolute();
        if (relative)
          resolvedPath = new File(builder.getBaseDir(), path);
        
        if (resolvedPath.isFile()) {
          console.printf("%nGot it.%n%n");
          func.accept(path);
          break;
        } else {
          String msg = "Error. " + path;
          if (relative)
            msg += " which resolves to " + resolvedPath;
          msg += " does not exist. Try again:%n";
          console.printf(msg);
          continue;
        }
      } catch (Exception x) {
        console.printf("Woops. That didn't work (" + x.getMessage() + "). Try again:%n");
      }
    }
    
  }
  
  private void testSrcCon() {
    printf("%nWould you like to test this DB connection? [y]:");
    if ("y".equalsIgnoreCase(readLine())) {
      printf("%n%nAttempting connnection.. ");
      if (srcConWorks())
        printf(" Works!%n%n");
      else {
        printf(" Woops, that didn't work.");
        printf("%nYou can edit these configuration properties later.%n%n");
      }
    } else {
      printf("%n%nOK, skipping DB connnection test%n");
    }
  }
  
  
  private boolean srcConWorks() {
    ConnectionInfo conInfo =
        new ConnectionInfo(
            builder.getSourceUrl(),
            builder.getSourceDriverClass(),
            builder.getSourceDriverClasspath());
    try {
      conInfo.open(builder.getBaseDir(), builder.getSourceConProperties()).close();
      return true;
    } catch (Exception x) {
      return false;
    }
  }
  
  
  private boolean setHashConUrl() {
    PrintSupport printer = new PrintSupport();
    var paragraph =
        "The hash-ledger (the data structure that tracks the source-table) reads and " +
        "writes to its own tables. The information in this ledger is almost entirely " +
        "opaque and consists of undecipherable hashes: it does reveal the number of " +
        "rows in the source-table and their history (when the hashes were witnessed), " +
        "however.";
    printer.printParagraph(paragraph);
    printf("%n");
    paragraph =
        "So the hash-ledger needs a (read/write) DB connection--which can either be one " +
        "dedicated to itself, or the same one used to access the source-table.";
    printer.printParagraph(paragraph);
    printf("%n");
    while (true) {
      printf(
          "Enter the connection URL for the hash ledger (enter blank to skip):%n");
      String url = readLine();
      if (url.isEmpty())
        return false;
      if (malformedUrl(url)) {
        console.printf("%nNot a valid URL. Try Again.%n");
        continue;
      }
      
      builder.setHashUrl(url);
      printf("%nGot it.");
      return true;
    }
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
        "Command line tool for managing a tamper proof historical ledger formed from a relational database " +
        "table or view that is operationally append-only.";
    printer.printParagraph(paragraph);
    printer.println();
  }

  private final static int LEFT_TBL_COL_WIDTH = 15;
  private final static int RIGHT_TBAL_COL_WIDTH = RM - INDENT - LEFT_TBL_COL_WIDTH;

  @Override
  protected void printUsage(PrintStream out) {
    PrintSupport printer = new PrintSupport(out);
    printer.setMargins(INDENT, RM);
    printer.println();
    out.println("USAGE:");
    printer.println();
    String paragraph =
        "Excepting '" + SETUP + "', each command takes the path to a " +
        "configuration file (specifying DB connections, the table or view etc.) as well as " +
        "any other arguments indicated below. Command and argument order do not matter.";
    printer.printParagraph(paragraph);
    printer.println();

    TablePrint table = new TablePrint(out, LEFT_TBL_COL_WIDTH, RIGHT_TBAL_COL_WIDTH);
    table.setIndentation(INDENT);
    
    table.printRow(SETUP, "interactive config file setup");
    table.println();
    
    table.printRow(CREATE, "creates the hash ledger schema using the given config");
    table.printRow(null,   "file. (See " + SETUP + ")");
    table.println();
    
    table.printRow(STATUS, "prints the status of the ledger");
    table.println();
    
    table.printRow(LIST,   "lists (prints) the given source rows");
    table.printRow(null,   "Args:");
    table.println();
    table.printRow(null,   "<row-numbers>         (required)");
    table.println();
    table.printRow(null,   "Strictly ascending row numbers separated by commas (no spaces)");
    table.printRow(null,   "Ranges may be substituted for numbers. For example:");
    table.printRow(null,   "250,692-717");
    table.println();

    table.printRow(HISTORY, "lists the crumtrails (witness records) in the ledger");
    table.println();
    
    table.printRow(UPDATE, "appends the hashes of new source rows to the hash-ledger;");
    table.printRow(null,   "unwitnessed rows are timestamped.");
    table.printRow(null,   "Args:");
    table.println();
    table.printRow(null,   "<maximum number of rows to append>            (optional)");
    table.printRow(null,   "DEFAULT: no maximum");
    table.println();

    table.printRow(WITNESS, "retrieves crumtrails (witness records) for the remaining rows");
    table.printRow(null,   "in the hash ledger. Equivalent to '" + UPDATE + " 0'");
    table.println();

    table.printRow(VALIDATE, "validates the source rows hashes haven't changed");
    table.println();

    table.printRow(ROLLBACK, "rolls back the ledger to before given row number. The source");
    table.printRow(null,   "table or view is supposed to be append-only; in the real world");
    table.printRow(null,   "mistakes happen. Requires interactive user confirmation.");
    table.printRow(null,   "Args:");
    table.println();
    table.printRow(null,   "<conflict-number>     (required, unless source is trimmed)");
    table.println();
    table.printRow(null,   "Note if the source row's hash does not conflict at the given");
    table.printRow(null,   "row number then the command will fail. If the source table has");
    table.printRow(null,   "been trimmed (truncated) and no <conflict-number> is supplied,");
    table.printRow(null,   "then the hash ledger is rolled back to the size of the source.");
    table.println();

    table.printRow(MAKE_MORSEL, "creates a morsel file containing the contents of the given");
    table.printRow(null,   "row numbers");
    table.printRow(null,   "Args:");
    table.println();
    table.printRow(null,   "<row-numbers>         (required)");
    table.println();
    table.printRow(null,   "Strictly ascending row numbers separated by commas (no spaces)");
    table.printRow(null,   "Ranges may be substituted for numbers. For example:");
    table.printRow(null,   "308,466,592-598,717");
    table.println();
    table.printRow(null,   "<path/to/morselfile>  (optional)");
    table.println();
    table.printRow(null,   "If provided, then <path/to/morselfile> is the destination path.");
    table.printRow(null,   "The given path should not be an existing file; however, if it's");
    table.printRow(null,   "an existing directory, then a filename is generated for the");
    table.printRow(null,   "file in the chosen directory.");
    table.printRow(null,   "DEFAULT: '.'          (current directory)");
    table.println();
    table.printRow(null,   REDACT + "=<column-numbers>  (optional)");
    table.println();
    table.printRow(null,   "If provided, then the given comma-separated column numbers will");
    table.printRow(null,   "be redacted. The first column is numbered 1.");
    table.println();
    table.printRow(null,   META + "=<path/to/meta_file>  (optional)");
    table.println();
    table.printRow(null,   "If provided, then the given JSON meta file is used. Overrides");
    table.printRow(null,   "any value set for this property in the ledger config file.");
    table.println();
    
    table.printRow(STATE_MORSEL, "creates an empty morsel file containing only the ledger's");
    table.printRow(null,   "opaque state-path: the shortest list of rows connecting the last");
    table.printRow(null,   "row to the first. This is a verifiably evolving fingerprint.");
    table.println();
    table.printRow(null,   "<path/to/morselfile>  (optional)");
    table.println();
    table.printRow(null,   "(Same semantics as with '" + MAKE_MORSEL + "' above)");
    table.println();
    
  }

  @Override
  protected void printLegend(PrintStream out) {
    out.println();
    out.println("For additional info on morsels try");
    out.println("  " + Mrsl.class.getSimpleName().toLowerCase() + " -help");
    out.println("from the console.");
    out.println();
  }
  
  
  private final static String SETUP = "setup";
  private final static String CREATE = "create";
  private final static String STATUS = "status";
  private final static String LIST = "list";
  private final static String HISTORY = "history";
  private final static String UPDATE = "update";
  private final static String WITNESS = "witness";
  private final static String VALIDATE = "validate";
  private final static String ROLLBACK = "rollback";

  private final static String MAKE_MORSEL = "make-morsel";
  private final static String STATE_MORSEL = "state-morsel";

}





