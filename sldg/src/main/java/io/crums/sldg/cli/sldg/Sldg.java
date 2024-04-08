/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.cli.sldg;


import static io.crums.util.Strings.nOf;
import static io.crums.util.Strings.pluralize;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import io.crums.client.ClientException;
import io.crums.sldg.ledgers.Ledger;
import io.crums.sldg.mrsl.MorselFile;
import io.crums.sldg.ledgers.Ledger.State;
import io.crums.sldg.ledgers.SourceLedger;
import io.crums.sldg.json.SourceInfoParser;
import io.crums.sldg.reports.pdf.ReportAssets;
import io.crums.sldg.sql.Config;
import io.crums.sldg.sql.SqlLedger;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.SourceInfo;
import io.crums.sldg.time.TrailedRow;
import io.crums.util.IntegralStrings;
import io.crums.util.Lists;
import io.crums.util.json.JsonParsingException;
import io.crums.util.main.NumbersArg;
import io.crums.util.main.TablePrint;
import io.crums.util.ticker.Progress;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

/**
 * Manages a ledger formed from a relational database table or view that is
 * operationally append-only.
 */
@Command(
    name = "sldg",
    mixinStandardHelpOptions = true,
    version = "sldg 0.5",
    synopsisHeading = "",
    customSynopsis = {
        "",
        "Ledger monitoring, tracking and reporting tool.",
        "",
        "Usage: @|bold sldg|@ @|fg(yellow) FILE|@ COMMAND",
        "       @|bold sldg help|@ COMMAND",
        "       @|bold sldg " + Setup.NAME + "|@",
        "       @|bold sldg|@ [@|fg(yellow) -hV|@]",
        "",
    },
    subcommands = {
        HelpCommand.class,
        Setup.class,
        Create.class,
        Status.class,
        ListCmd.class,
        History.class,
        Update.class,
        Witness.class,
        Validate.class,
        Rollback.class,
        Morsel.class,
    })
public class Sldg implements Closeable {
  
  
  public static void main(String[] args) {
    int exitCode;
    
    try (var sldg = new Sldg()) {
      
      exitCode = new CommandLine(new Sldg()).execute(args);
    
    } catch (Exception x) {
      if (Thread.interrupted())
        exitCode = INTERRUPT;
      else {
        System.err.printf("Unhandled exception: %s%n", x.toString());
        x.printStackTrace();
        System.err.printf(
            "%nPlease report this bug at%n    https://github.com/crums-io/skipledger%n%n");
        exitCode = ERR_SOFT;
      }
    }
    
    System.exit(exitCode);
  }
  

  final static int ERR_SOFT = 1;
  final static int ERR_USER = 2;
  final static int INTERRUPT = 3;
  final static int ERR_IO = 4;
  /** Network error. */
  final static int ERR_NET = 5;
  

  
  
  
  
  @Spec
  private CommandSpec spec;
  

  private File configFile;
  
  @Parameters(
      arity = "0..1",
      paramLabel = "FILE",
      description = {
          "Ledger configuration file",
          "@|bold Required|@ with any command except @|bold " + Setup.NAME + "|@ and @|bold help|@"
      })
  public void setConfig(File configFile) {
    this.configFile = configFile;
    if (!configFile.isFile())
      throw new ParameterException(spec.commandLine(), "not a file: " + configFile);
    else if (!configFile.canRead())
      throw new ParameterException(spec.commandLine(), "need read permission: " + configFile);
  }
  
  
  /**  Returns the configuration file. (Driver method for overrides.) */
  public File getConfigFile() {
    return configFile;
  }
  
  
  
  
  private ConfigR config;
  
  
  public ConfigR getConfig() {
    if (config == null)
      config = new ConfigR(getConfigFile());
    return config;
  }
  
  

  private SqlLedger ledger;
  
  public SqlLedger getLedger() {
    if (ledger == null)
      ledger = SqlLedger.loadInstance(getConfig());
    return ledger;
  }
  
  
  public void createLedger() {
    if (ledger != null)
      throw new IllegalStateException("cannot recreate already loaded ledger");
    ledger = SqlLedger.declareNewInstance(getConfig());
  }
  
  
  /** Closes the backing database connection(s). */
  @Override
  public void close() {
    if (ledger != null) {
      ledger.close();
      ledger = null;
    }
    config = null;
  }
  
  
  /**
   * Invokes {@code System.out.printf(format, args)} after pre-processing
   * any Jansi-encoded strings.
   * 
   * @param format
   * @param args Jansi-encoded string arguments are pre-processed
   * 
   * @see Ansi#string(String)
   * @see Ansi#AUTO
   */
  public void printf(String format, Object... args) {
    System.out.printf(format, jansify(args));
  }
  
  
  private Object[] jansify(Object[] args) {
    for (int index = args.length; index-- > 0; ) {
      if (args[index] instanceof String arg)
        args[index] = Ansi.AUTO.string(arg);
    }
    return args;
  }
  
  
  /**
   * Prints a line of error message in red.
   * 
   * @param format
   * @param args this <em>might</em> also work with Jansi-encoded arguments
   */
  public void printfError(String format, Object... args) {
    var formatted = format.formatted(jansify(args));
    var inRed = Ansi.AUTO.string("@|red " + formatted + "|@");
    System.err.println(inRed);
  }
  
}

@Command(
    name = Create.NAME,
    description = {
        "Create empty hash tables (3) for tracking the ledger",
        "(as defined in the preceding config @|yellow FILE|@ argument)"})
class Create implements Runnable {
  
  public final static String NAME = "create";
  
  @ParentCommand
  private Sldg sldg;
  
  @Spec
  private CommandSpec spec;

  @Override
  public void run() {
    final var out = System.out;
    out.println();
    boolean overwriteAttempt;
    try {
      sldg.getLedger();
      overwriteAttempt = true;
    } catch (Exception expected) {
      overwriteAttempt = false;
    }
    if (overwriteAttempt)
      throw new ParameterException(spec.commandLine(), "Ledger already exists.");
    out.printf("%ncreating backing hash ledger tables (3)%n");
    sldg.createLedger();
    out.printf("%nDone.%n");
    sldg.printf(
        "Use the %s command to verify the source-row query works as intended.%n",
        "@|bold " + ListCmd.NAME + "|@");
  }
  
}


abstract class CommandBase {

  private final static int RM = 80;
  private final static int LEFT_STATUS_COL_WIDTH = 16;
  private final static int RIGHT_STATUS_COL_WIDTH = 18;
  private final static int MID_STATUS_COL_WIDTH = RM - LEFT_STATUS_COL_WIDTH - RIGHT_STATUS_COL_WIDTH;
  
  
  public TablePrint trailTablePrint() {
    return new TablePrint(
        LEFT_STATUS_COL_WIDTH,
        MID_STATUS_COL_WIDTH,
        RIGHT_STATUS_COL_WIDTH);
  }
  
  abstract Ledger getLedger();
  
  public void printTrailDetail(int index, TablePrint table) {
    var ledger = getLedger();
    TrailedRow trailedRow = ledger.getHashLedger().getTrailByIndex(index);
    long utc = trailedRow.utc();
    table.printRow("row #: ", trailedRow.rowNumber());
    table.printRow("witnessed:", new Date(utc), "UTC: " + utc);
    table.printRow("trail root:", IntegralStrings.toHex(trailedRow.trail().rootHash()));
    table.printRow("ref URL:", trailedRow.trail().getRefUrl());
  }
  
  
  void needsMendingMessage() {
    var ledger = getLedger();
    var out = System.out;
    if (ledger.getState().isForked()) {
      long fc = ledger.getFirstConflict();
      out.printf("%nrow [%d] has been modified%n", fc);
      out.printf("%nTo fix, either%n");
      out.printf("  a) restore the source row [%d] to its previous state, or%n", fc);
      out.printf("  b) %s to before conflict row [%d] in the hash ledger]%n",
          Ansi.AUTO.string("@|bold " + Rollback.NAME + " |@"), fc);
      
    } else if (ledger.getState().isTrimmed()){
      long trimmed = ledger.sourceLedgerSize();
      long missing = ledger.hashLedgerSize() - trimmed;
      out.printf("%nsource ledger has been trimmed to %s%n", nOf(trimmed, "row"));
      out.printf("%nTo fix, either%n");
      out.printf("  a) restore the last %s, or%n", nOf(missing, "missing source row"));
      out.printf("  b) %s the hash ledger (to row [%d])%n",
          Ansi.AUTO.string("@|bold " + Rollback.NAME + " |@"), trimmed);
    } else
      throw new RuntimeException("needsMendingMessage() assertion failed: " + ledger.getState());
    
  }
  
}


@Command(
    name = "status",
    description = "Display ledger status")
class Status extends CommandBase implements Runnable {
  
  
  
  public static void run(Sldg sldg) {
    var instance = new Status();
    instance.sldg = sldg;
    instance.run();
  }
  
  @ParentCommand
  private Sldg sldg;
  
  @Override
  SqlLedger getLedger() {
    return sldg.getLedger();
  }
  
  
  @Override
  public void run() {
    final var ledger = getLedger();
    
    var out = System.out;
    
    final long rowsRecorded = ledger.hashLedgerSize();
    final int trails = ledger.getHashLedger().getTrailCount();
    out.printf("%n%s recorded in hash ledger%n", nOf(rowsRecorded, "row"));
    
    out.printf("%s%n", nOf(trails, "crumtrail"));
    
    State state = ledger.getState();
    if (state.needsMending()) {
      
      needsMendingMessage();
      sldg.printf(
          "%nUse the %s command to determine the first modified source row.%n",
          "@|bold " + Validate.NAME + "@|");
    
    } else {
      
      if (state.isPending()) {
        long pending = ledger.rowsPending();
        out.printf("%n%s in source ledger not yet recorded%n", nOf(pending, "row"));
        
      } else {
        // assert state.isComplete();
        out.printf("%nhash ledger is up to date with source ledger%n");
        long unwitnessRows = ledger.getHashLedger().unwitnessedRowCount();
        if (unwitnessRows == 0) {
          out.printf("all row hashes witnessed%n");
        } else {
          out.printf(
              "%s remaining row %s to witness%n",
              nOf(unwitnessRows, "remaining row hash"));
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
  
}

@Command(
    name = ListCmd.NAME,
    description = {
        "List source rows matching row numbers",
        "Pending (yet-to-be-tracked) rows can also be listed.",
        "",
    })
class ListCmd implements Runnable {
  
  public final static String NAME = "list";
  
  private final static String DEFAULT_COL_DIVISOR = " | ";
  private final static String NULL_SYMBOL = "<NULL>";
  private final static String SEP_S = "%s";
  private final static String SEP_T = "%t";
  

  
  @ParentCommand
  private Sldg sldg;
  
  @Spec
  private CommandSpec spec;
  
  
  private List<Long> rowNums;
  
  
  @Parameters(
      arity = "1",
      paramLabel = "NUMS",
      description = {
          "Comma-separated row numbers to match",
          "Use dashes for ranges. For example: 466,308,592-598,717"
          
      }
      )
  public void setRowNumbers(String commaSepRns) {
    this.rowNums = NumbersArg.parseSorted(commaSepRns);
    if (rowNums == null)
      throw new ParameterException(
          spec.commandLine(), "cannot grok '" + commaSepRns + "'");
    long first = rowNums.get(0);
    if (first < 1)
      throw new ParameterException(
          spec.commandLine(),
          "row number (" + first + ") must be > 0: '" + commaSepRns + "'");
  }
  
  
  @Option(
      names = "--sep",
      paramLabel = "DIV",
      description = {
          "Set column separator. Use quotes for white space",
          "or the following character aliases:",
          "  %" + SEP_S + "  -> ' '   (space)", // (extra % in order to esc %)
          "  %" + SEP_T + "  -> '\\t'  (tab)",
          "Default: \"" + DEFAULT_COL_DIVISOR + "\""
        }
      )
  public void setColumnDivisor(String sep) {
    if (sep.length() > 1 && sep.startsWith("\"") && sep.endsWith("\""))
      columnDivisor = sep.substring(1, sep.length() - 1);
    else
      columnDivisor = sep.replace(SEP_S, " ").replace(SEP_T, "\t");
  }
  private String columnDivisor = DEFAULT_COL_DIVISOR;
  
  
  
  
  
  
  @Override
  public void run() {

    TablePrint table;
    {
      long lastRn = rowNums.get(rowNums.size() - 1);
      int decimalWidth = Math.max(3,  Long.toString(lastRn).length());
      table = new TablePrint(decimalWidth + 3, 77 - decimalWidth);
    }
    
    SourceLedger src = sldg.getLedger().getSourceLedger();
    
    final long max = src.size();
    
    StringBuilder s = new StringBuilder(256);
    
    int count = 0;
    for (long rn : rowNums) {
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
        s.append(columnDivisor);
      }
      // remove the last divisor
      s.setLength(s.length() - columnDivisor.length());
      table.printRow("[" + rn + "]", s);
    } // for
    
    table.println();
    table.println(nOf(count, " row") + " listed");
    int skipped = rowNums.size() - count;
    if (skipped > 0) {
      table.println();
      table.println("(" + nOf(skipped, " row argument") + " beyond last row)");
    }
    table.println();
  }
  
}


@Command(
    name = "history",
    description = "List crumtrails (witness records) in hash ledger"
    )
class History extends CommandBase implements Runnable {
  
  @ParentCommand
  private Sldg sldg;
  

  @Override
  Ledger getLedger() {
    return sldg.getLedger();
  }

  @Override
  public void run() {
    var ledger = getLedger();
    
    int witnessedRows = ledger.getHashLedger().getTrailCount();
    
    var table = trailTablePrint();
    
    for (int index = 0; index < witnessedRows; ++index) {
      table.println();
      printTrailDetail(index, table);
    }
    table.println();
    long ledgeredRows = ledger.hashLedgerSize();
    table.println(nOf(ledgeredRows, " ledgered row"));
    table.println(
        "state witnessed and recorded at " +  nOf(witnessedRows, "row"));
    table.println();
  }
}




@Command(
    name = Update.NAME,
    description = {
        "Append untracked source rows to hash ledger",
        "",
        "Unwitnessed row hashes are timestamped (see @|bold help " + Witness.NAME + "|@)",
        "",
      }
    )
class Update implements Callable<Integer> {
  
  public final static String NAME = "update";
  
  private final static String COUNT_LABEL = "COUNT";
  
  @ParentCommand
  private Sldg sldg;
  
  @Spec
  private CommandSpec spec;
  
  
  private long count = Long.MAX_VALUE;
  
  
  @Parameters(
      paramLabel = COUNT_LABEL,
      arity = "0..1",
      description = {
          "Maximum number of source rows to add to the tracking hash ledger",
          "Default: no limit",
      }
      )
  public void setUpdateCount(long count) {
    this.count = count;
    if (count < 0)
      throw new ParameterException(
          spec.commandLine(),
          // Note so'll I'll remember..
          // This works, but formatting exception messages a bad idea..
          // (cuz you'll discover formatting bugs at the worst fucking time)
          "negative %s: %d".formatted(COUNT_LABEL, count));
  }
  
  

  @Override
  public Integer call() {
    var ledger = sldg.getLedger();
    switch (ledger.getState()) {
    case FORKED:
    case TRIMMED:
      Status.run(sldg);
      return 0;
    case PENDING:
      long rowsAdded = ledger.update(count);
      System.out.printf("%n%s added%n%n", nOf(rowsAdded, "source row"));
    case COMPLETE:
      return Witness.callAfterUpdate(sldg);
    default:
      throw new AssertionError("ledger state: " + ledger.getState());
    }
  }
  
}


@Command(
    name = Witness.NAME,
    description = {
        "Witness remaining rows in hash ledger",
        "",
        "Retrieve and store crumtrails (witness records) for remaining unwitnessed rows",
        "in hash ledger. Same as '@|bold " + Update.NAME + "|@ 0'",
        "",
    }
    )
class Witness implements Callable<Integer> {
  
  public final static String NAME = "witness";
  
  
  /**
   * Prolly more elegant (picocli) ways to do this..
   * This gets the job done.
   */
  public static int callAfterUpdate(Sldg sldg) {
    var instance = new Witness();
    instance.sldg = sldg;
    instance.afterUpdate = true;
    return instance.call();
  }

  
  @ParentCommand
  private Sldg sldg;
  
  /** switches message output only */
  private boolean afterUpdate;
  
  
  
  @Override
  public Integer call() {
    var ledger = sldg.getLedger();
    var out = System.out;
    try {
      var witReport = ledger.witness();
      if (witReport.nothingDone()) {
        if (ledger.getState().isComplete() && afterUpdate)
          out.printf("%nUp to date.%n");
        else {
          out.printf("All rows recorded in hash ledger already witnessed.%n");
        }
      } else {
        int crums = witReport.getRecords().size();
        int crumtrails = witReport.getStored().size();
        out.printf(
            "%s (%s) submitted; %s (%s) stored%n",
            nOf(crums, "crum"),
            pluralize("row hash", crums),
            nOf(crumtrails, "crumtrail"),
            pluralize("witness record", crumtrails));
        long remaining = ledger.unwitnessedRowCount();
        if (remaining == 0)
          out.printf("All rows in hash ledger are now witnessed.%n");
        else {
          out.printf(
              "%s for %s pending retrieval%n",
              pluralize("Crumtrail", remaining),
              nOf(remaining, "row"));
          sldg.printf(
              "Run '%s' in a few minutes to retrieve the remaining %s.%n",
              "@|bold " + Witness.NAME + "|@",
              pluralize("crumtrail", remaining));
        }
      }
    } catch (ClientException cx) {
      
      sldg.printfError(
          """
          %nNetwork error encountered while attempting to have the ledger witnessed%n
          [%d] is the last witnessed row%n
          Error detail: %s%n%n
          """,
          ledger.lastWitnessedRowNumber(),
          cx.getMessage());
      
      return Sldg.ERR_NET;
    }
    
    return 0;
  }
}


@Command(
    name = Validate.NAME,
    description = {
        "Validate source rows tracked in hash ledger have not changed",
        "",
        "Failure exit code: negative the first 31 bits of the failed row number",
        "",
    }
    )
class Validate extends CommandBase implements Callable<Integer> {
  
  public final static String NAME = "validate";

  
  @ParentCommand
  private Sldg sldg;
  
  @Option(
      names = "--failCode",
      paramLabel = "CODE",
      description = {
          "Use this exit code if validation fails",
          "Default: negative the failed row number (mod 0x7fffffff)",
        }
      )
  private int failExitCode;
  

  @Override
  public Integer call() throws Exception {
    var ledger = sldg.getLedger();
    var out = System.out;
    long rows = ledger.lesserSize();
    if (rows == 0) {
      out.printf("%nNothing to validate.%n");
      Status.run(sldg);
      return 0;
    }
    
    out.printf("%nValidating %s%n", nOf(rows, "row"));
    
    // FIXME: this needlessly overflows at about 45B rows
    ledger.setProgressTicker(Progress.newStdOut(rows));

    State state = ledger.validateState(false);
    if (state.needsMending()) {
      
      String warning = "WARNING (!):%n";
      if (failExitCode == 0)
        sldg.printf(warning);
      else
        sldg.printfError(warning);
        
      needsMendingMessage();
      return failExitCode();
      
    } else {
      out.printf("%nOK. All ledgered rows verified.%n");
    }
    return 0;
  }
  
  
  private int failExitCode() {
    if (failExitCode != 0)
      return failExitCode;
    var ledger = getLedger();
    var state = ledger.getState();
    if (!state.needsMending())
      throw new RuntimeException("failExitCode() assertion failed. State: " + state);
    long conflict = state.isForked() ? ledger.getFirstConflict() : ledger.sourceLedgerSize() + 1;
    return - (int) (conflict & Integer.MAX_VALUE);
  }


  @Override
  Ledger getLedger() {
    return sldg.getLedger();
  }
  
}


@Command(
    name = Rollback.NAME,
    description = {
        "Roll back (trim) the hash ledger to before the conflict number",
        "",
        "Existing rows in a ledger are never supposed to change. The only way to fix a",
        "permanently edited (or deleted) source row is to @|italic trim|@ the hash ledger.",
        "",
        "@|italic History will be lost; any morsel reported after the conflict-row will conflict|@",
        "@|italic with future morsels from the edited ledger.|@",
        "",
        "Requires interactive user confirmation.",
        "",
    }
    )
class Rollback implements Runnable {
  
  public final static String NAME = "rollback";
  
  
  

  @ParentCommand
  private Sldg sldg;
  
  @Spec
  private CommandSpec spec;
  
  
  private long conflictRow;
  
  @Parameters(
      paramLabel = "NUM",
      description = {
          "Conflict row number (typically discovered via the @|bold " + Validate.NAME + "|@ command)",
          "Required, unless the ledger state is TRIMMED",
        }
      )
  public void setConflictRow(long conflict) {
    if (conflict > 1)
      this.conflictRow = conflict;
    else {
      String msg;
      if (conflict == 1L)
        msg = "Attempt to empty ledger";
      else
        msg = "Illegal conflict row number: " + conflict;
      throw new ParameterException(spec.commandLine(), msg);
    }
  }


  @Override
  public void run() {
    
    if (System.console() == null)
      throw new ParameterException(
          spec.commandLine(),
          Rollback.NAME + " command requires console for confirmation");
    
    var ledger = sldg.getLedger();
    final boolean trim = conflictRow == 0;
    
    // check arguments
    if (trim) {
      // coulda lumped this check in the *if* stmt above:
      // it's clearer this way
      if (!ledger.getState().isTrimmed())
        throw new ParameterException(spec.commandLine(), "missing conflict-number");
    } else {
      long hashLdgrSz = ledger.hashLedgerSize();
      if (conflictRow > hashLdgrSz)
        throw new ParameterException(
            spec.commandLine(), 
            "conflict row number (%d) > hash ledger size (%d)".formatted(
                conflictRow, hashLdgrSz));
      
      if (ledger.checkRow(conflictRow) && ledger.lastValidRow() > conflictRow)
        throw new ParameterException(
            spec.commandLine(), 
            "no conflict found at/before row [" + conflictRow + "]");
    }
    

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
    
    var out = System.out;
    
    out.printf(
        "WARNING: You are about to erase the last %s from the hash ledger!%n",
        nOf(rowsToLose, "row"));
    
    if (trail != null) {
      out.printf(
        "   Crumtrails (witness records) dating back to row [%d] will also be lost%n",
        trail.rowNumber());
      out.printf(
        "   (witnessed on %s)%n", new Date(trail.utc()));
    }
    
    out.printf("%nConfirm rollback [y]:%n");

    String ack = System.console().readLine();
    boolean yes = "y".equalsIgnoreCase(ack) || "yes".equalsIgnoreCase(ack);
    if (!yes) {
      out.printf("%nOK. Rollback aborted.%n");
      return;
    }
    
    if (conflictRow == 0)
      ledger.rollback();
    else
      ledger.rollback(conflictRow);
    
    final int trailsLost = preTrailCount - ledger.getHashLedger().getTrailCount();
    
    out.printf(
        "%n%s rolled back; %s lost.%n",
        nOf(rowsToLose, "row"),
        nOf(trailsLost, "crumtrail"));
    
  }
  
  
}



@Command(
    name = "morsel",
    description = {
        "Create morsel (report) file",
        "Includes source data for given row numbers with proofs of membership and age.",
        "Creates a @|italic state|@-morsel (ledger fingerprint file) if no row numbers given.",
        "",
        "See also @|bold mrsl -h|@ (morsel reader/manipulation program).",
        "",
      }
    )
class Morsel implements Callable<Integer> {
  

  private final static String REDACT = "--redact";
  
  @ParentCommand
  private Sldg sldg;
  @Spec
  private CommandSpec spec;
  
  private File morselFile = new File(".");

  @Option(
      names = {"-r", "--report"},
      description = "Include report template provided in config (Alpha)")
  private boolean report;
  
  
  // TODO: add default file extension?
  @Option(
      names = {"-s", "--save"},
      paramLabel = "PATH",
      description = {
          "Path to save new morsel file",
          "If it's a directory, then the file name is generated",
          "with the subdir name prefixed;",
          "otherwise, a path to @|italic new|@ file.",
          "Default: \".\"   (current directory)",
      })
  public void setMorselFile(File file) {
    this.morselFile = file;
    if (file.exists()) {
      if (!file.isDirectory())
        throw new ParameterException(spec.commandLine(), file + " already exists");
    
    } else {
      File parent = file.getParentFile();
      if (parent != null && !parent.exists()) {
        throw new ParameterException(
            spec.commandLine(),
            "parent directory does not exist: " + parent);
      }
    }
  }
  
  
  @Option(
      names = "--meta",
      paramLabel = "META_FILE",
      description = {
          "Path to ledger meta file injected into morsel",
          "Default: path specified in ledger config file's",
          "@|italic " + Config.META_PATH +  "|@ property, if any.",
          }
      )
  public void setMeta(File file) {
    if (!file.isFile())
      throw new ParameterException(spec.commandLine(), file + " does not exist");
    try {
      this.sourceInfo = SourceInfoParser.INSTANCE.toEntity(file);
    
    } catch (JsonParsingException jpx) {
      throw new ParameterException(
          spec.commandLine(),
          file + " is not well-formed. Parsing error: " + jpx.getMessage());
    }
  }

  private SourceInfo sourceInfo;
  
  /**
   * Returns the user-supplied JSON meta file, or the one set in the configuration
   * file, if any.
   */
  private SourceInfo getMeta() {
    return sourceInfo == null ?
        sldg.getConfig().getSourceInfo().orElse(null) : sourceInfo;
  }
  

  private List<Long> srcRns = List.of();
  private List<Integer> redactCols = List.of();
  

  
  @Parameters(
      paramLabel = "ROWS",
      arity = "0..1",
      description = {
          "Comma-separated list of source rows to include",
          "Use dash for ranges. Eg: 466,308,592-598,717",
          "Default: None  (a @|italic state|@-morsel)"
          }
      )
  public void setSourceRows(String rowSpec) {
    this.srcRns = NumbersArg.parse(rowSpec);
    if (srcRns == null || srcRns.isEmpty())
      throw new ParameterException(spec.commandLine(),
          "invalid row numbers argument: " + rowSpec);
    srcRns = Lists.sortRemoveDups(srcRns);
    var first = srcRns.get(0);
    if (first < 1)
      throw new ParameterException(spec.commandLine(),
          "invalid row number (%d): %s".formatted(first, rowSpec));
  }
  
  
  
  
  @Option(
      names = {"-d", REDACT},
      paramLabel = "COLS",
      description = {
          "Comma-separated column numbers to redact from sources",
          "Use dash for ranges. Eg: 8,12-17,23"
          }
      )
  public void setRedactColumns(String redactColumns) {
    this.redactCols = NumbersArg.parseInts(redactColumns);
    if (this.redactCols == null)
      throw new ParameterException(spec.commandLine(),
          "invalid " + REDACT + " argument: " + redactColumns);
    assert !redactCols.isEmpty();
    redactCols = Lists.sortRemoveDups(redactCols);
    var first = redactCols.get(0);
    if (first < 1)
      throw new ParameterException(spec.commandLine(),
          "invalid column number (%d): %s".formatted(first, redactColumns));
  }
  
  
  @Override
  public Integer call() {
    var ledger = sldg.getLedger();
    if (ledger.getState().needsMending())
      throw new ParameterException(spec.commandLine(),
          "source ledger conflicts with hash ledger: " + ledger.getState());
    
    var builder = ledger.loadBuilder(
        srcRns,
        null, // no comment (for now)
        redactCols,
        getMeta());
    
    if (report) try {
      sldg.getConfig().getReportPath().ifPresentOrElse(
          f -> ReportAssets.setReport(builder.assetsBuilder(), f),
          () -> System.err.println(
              "[WARNING] no valid report path setting '" + ConfigR.REPORT_PATH_PROPERTY +
              "' found in config file. (Ignored.)") );
    } catch (Exception x) {
      // bail
      System.err.println("Report template misconfiguration: " + x.getMessage());
      return Sldg.ERR_USER;
    }
    
    File file;
    try {
      
      file = MorselFile.createMorselFile(morselFile, builder);
    
    } catch (IOException iox) {
      // bail
      System.err.println("Failed to write morsel file");
      System.err.println("  Target path: " + morselFile);
      System.err.println("  Error detail: " + iox.getMessage());
      return Sldg.ERR_IO;
    }
    
    int entries = srcRns.size();
    if (entries == 0)
      System.out.println("State morsel written to " + file);
    else
      System.out.printf("Source morsel (%s) written to %s%n", nOf(entries, "row"), file.getPath());

    return 0;
  }
  
}












































