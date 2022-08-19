/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.cli.mrsl;


import static io.crums.util.Strings.nOf;
import static io.crums.util.Strings.pluralize;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import io.crums.io.IoBridges;
import io.crums.model.CrumTrail;
import io.crums.sldg.ByteFormatException;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.MorselFile;
import io.crums.sldg.json.MorselDumpWriter;
import io.crums.sldg.json.SourceInfoParser;
import io.crums.sldg.json.SourceRowParser;
import io.crums.sldg.json.TrailedRowWriter;
import io.crums.sldg.packs.MorselPack;
import io.crums.sldg.packs.MorselPackBuilder;
import io.crums.sldg.packs.MorselPackBuilder.MergeResult;
import io.crums.sldg.reports.pdf.ReportAssets;
import io.crums.sldg.reports.pdf.ReportTemplate;
import io.crums.sldg.reports.pdf.input.NumberArg;
import io.crums.sldg.src.BytesValue;
import io.crums.sldg.src.DateValue;
import io.crums.sldg.src.SourceInfo;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.time.TrailedRow;
import io.crums.util.IntegralStrings;
import io.crums.util.Lists;
import io.crums.util.Sets;
import io.crums.util.TaskStack;
import io.crums.util.json.JsonPrinter;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.main.ArgList;
import io.crums.util.main.NumbersArg;
import io.crums.util.main.PrintSupport;
import io.crums.util.main.TablePrint;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;



/**
 * CLI for morsel files. Main launch class.
 */
@Command(
    name="mrsl",
    mixinStandardHelpOptions = true,
    version = "mrsl 0.5",
    description = {
        "Morsel file (.mrsl) report and manipulation tool.%n",
    },
    subcommands = {
        HelpCommand.class,
        Summary.class,
        Info.class,
        State.class,
        ListCmd.class,
        History.class,
        Entry.class,
        Merge.class,
        Submerge.class,
        Report.class,
        Dump.class
    })
public class Mrsl {
  
  final static int ERR_SOFT = 1;
  final static int ERR_USER = 2;
  final static int ERR_IO = 3;
  
  final static int RM = 80;
  final static int RN_PAD = 1;
  
  final static String RED_TOKEN = "[X]";
  
  static String stateString(MorselPack pack) {
    long hi = pack.hi();
    return hi + "-" + IntegralStrings.toHex(pack.rowHash(hi));
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Mrsl()).execute(args);
    System.exit(exitCode);
  }
  
  
  
  @Spec
  private CommandSpec spec;
  
  private File morselFile;
  
  @Parameters(arity = "1", paramLabel = "FILE", description = "Morsel file (.mrsl)")
  public void setMorselFile(File file) {
    morselFile = file;
    if (!morselFile.isFile())
      throw new ParameterException(spec.commandLine(), "not a file: " + morselFile);
  }
  
  
  MorselFile getMorselFile() {
    return getMorselFile(morselFile);
  }
  

  MorselFile getMorselFile(File file) {
    try {
      return new MorselFile(file);
    } catch (HashConflictException hcx) {
      throw new ParameterException(spec.commandLine(), file + " is corrupted: " + hcx.getMessage());
    } catch (ByteFormatException bfx) {
      throw new ParameterException(spec.commandLine(), file + " is not a morsel file: " + bfx.getMessage());
    } catch (RuntimeException x) {
      throw x;
    }
  }
  
  
}


class PackOption {
  
  @Option(names = {"-p", "--pack"}, description = "Output JSON sans whitespace padding")
  private boolean pack;
  
  boolean isPack() {
    return pack;
  }
  
}


class JsonOptions {
  
  @Option(names = {"-j", "--json"}, description = "Output JSON")
  private boolean json;
  
  @Mixin
  private PackOption packOpt;

  boolean isJson() {
    return json || isPack();
  }
  
  boolean isPack() {
    return packOpt != null && packOpt.isPack();
  }
}


class SaveBase {
  
  private File saveFile;
  
  void setSave(String file, CommandSpec spec) {
    saveFile = new File(file);
    File parent = saveFile.getParentFile();
    if (parent != null && !parent.isDirectory())
      throw new ParameterException(
          spec.commandLine(),
          "parent dir for given path does not exist: " + saveFile);
    if (saveFile.exists())
      throw new ParameterException(
          spec.commandLine(),
          "cannot overwrite existing path: " + saveFile);
  }
  
  public File getSaveFile() {
    return saveFile;
  }
  
  
  public boolean isSaveEnabled() {
    return getSaveFile() != null;
  }
  
  
  public int onIoError(String command, IOException iox) {
    System.err.println(command + " failed on I/O error: " + iox.getMessage());
    System.err.println("Target file: " + saveFile);
    return Mrsl.ERR_IO;
  }
  
}

class SaveOption extends SaveBase {
  
  @Spec
  CommandSpec spec;
  
  @Option(names = {"-s", "--save"}, paramLabel = "FILE", description = "Save to (new) file")
  public void setSave(String file) {
    setSave(file, spec);
  }
  
}


class ReqSaveOption extends SaveBase {
  
  @Spec
  CommandSpec spec;

  @Option(names = {"-s", "--save"}, paramLabel = "FILE", description = "Save to (new) file", required = true)
  public void setSave(String file) {
    setSave(file, spec);
  }
}




@Command(
    name = "sum",
    description = "Print morsel summary"
    )
class Summary implements Runnable {

  private final static int LEFT_SUM_COL_WIDTH = 17;
  private final static int RIGHT_SUM_COL_WIDTH = 25;
  private final static int MID_SUM_COL_WIDTH = Mrsl.RM - LEFT_SUM_COL_WIDTH - RIGHT_SUM_COL_WIDTH;
  
  @ParentCommand
  private Mrsl mrsl;

  

  @Override
  public void run() {
    var morsel = mrsl.getMorselFile();
    var pack = morsel.getMorselPack();
    PrintStream out = System.out;
    out.println();
    out.println("<" + morsel.getFile().getName() + ">");
    
    if (pack.getSourceInfo().isPresent()) {
      var info = pack.getSourceInfo().get();
      out.println(" -- " + info.getName() + " -- ");
    }
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
    
    if (!trails.isEmpty()) {
      out.println("History:");
      long witRn = trails.get(trails.size() - 1);
      var trail = pack.crumTrail(witRn);
      table.printRow(" witnessed:", new Date(trail.crum().utc()), "(row " + witRn + ")");
      out.println();
    }
    
    out.println("<" + Mrsl.stateString(pack) + ">");
  }
  
}




@Command(
    name = "info",
    description = "Print ledger meta info"
    )
class Info implements Runnable {
  
  @ParentCommand
  private Mrsl mrsl;

  @Mixin
  private JsonOptions jsonOpt = new JsonOptions();
  
  @Mixin
  private SaveOption saveOpt = new SaveOption();


  @Override
  public void run() {
    var pack = mrsl.getMorselFile().getMorselPack();
    var info = pack.getMetaPack().getSourceInfo();
    if (info.isEmpty()) {
      System.out.println();
      System.out.println("Morsel contains no meta information.");
    }
    
    else if (jsonOpt.isJson())
      infoJson(info.get());
    else
      infoText(info.get());
  }
  
  void infoJson(SourceInfo info) {
    var jObj = SourceInfoParser.INSTANCE.toJsonObject(info);
    boolean saving = saveOpt.isSaveEnabled();
    boolean compact = jsonOpt.isPack();
    
    try (var closer = new TaskStack()) {
      Appendable out;
      if (saving) {
        var writer = new FileWriter(saveOpt.getSaveFile());
        closer.pushClose(writer);
        out = writer;
      } else
        out = System.out;
      
      if (compact)
        jObj.writeJSONString(out);
      else
        new JsonPrinter(out).print(jObj);
        
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on writing " + saveOpt.getSaveFile() + ": " + iox.getMessage(), iox);
    }
    
    if (saving) {
      String msg =
          "Meta info for ledger named <" + info.getName() + "> with " +
          nOf(info.getColumnInfoCount(), "named column") + " written to file";
      System.out.println(msg);
    }
    System.out.println();
  }
  
  
  
  private final static int FIRST_TXT_INDENT = 4;
  private final static int INF_TXT_COL = FIRST_TXT_INDENT + 1;
  
  void infoText(SourceInfo info) {
    var table = new TablePrint(INF_TXT_COL, INF_TXT_COL);
    table.println();
    table.printRow("Name:");
    table.printRow(null, info.getName());
    
    if (isPresent(info.getDescription())) {
      table.println();
      table.printRow("Description:");
      table.setIndentation(FIRST_TXT_INDENT);
      table.printParagraph(info.getDescription());
      table.setIndentation(0);
    }
    
    var columnInfos = info.getColumnInfos();
    if (!columnInfos.isEmpty()) {
      table.println();
      table.println("Named Columns:");
      table.setIndentation(FIRST_TXT_INDENT);
      for (var colInfo : columnInfos) {
        table.println();
        table.printRow("[" + colInfo.getColumnNumber() + "]", colInfo.getName());
        table.incrIndentation(INF_TXT_COL);
        if (colInfo.getDescription() != null) {
          table.printRow("Description:");
          table.incrIndentation(INF_TXT_COL);
          table.printParagraph(colInfo.getDescription());
          table.decrIndentation(INF_TXT_COL);
        }
        if (isPresent(colInfo.getUnits())) {
          table.printRow("Units:", colInfo.getUnits());
        }
        table.decrIndentation(INF_TXT_COL);
      }
      table.setIndentation(0);
    }
    
    if (info.getDateFormat().isPresent()) {
      table.println();
      table.println("Date Format:");
      table.setIndentation(FIRST_TXT_INDENT);
      table.printRow("Pattern:", info.getDateFormatPattern());
      table.printRow("Example:", info.getDateFormat().get().format(new Date()));
      table.setIndentation(0);
    }
    table.println();
  }
  
  private boolean isPresent(String value) {
    return value != null && !value.isBlank();
  }
}




/**
 * Subcommand.
 */
@Command(
    name = "state",
    description = "Print last row hash and number"
    )
class State implements Runnable {
  
  @ParentCommand
  private Mrsl mrsl;
  
  @Override
  public void run() {
    System.out.println( Mrsl.stateString( mrsl.getMorselFile().getMorselPack()) );
  }
}




@Command(
    name = "list",
    description = "List known rows and entries"
    )
class ListCmd implements Runnable {

  private final static String TRL_TAG = "W";
  private final static String ENT_TAG = "S";
  
  private final static int RN_PAD = Mrsl.RN_PAD;
  private final static int FLAG_PAD = 2;
  private final static int DATE_COL = 32;
  
  @ParentCommand
  private Mrsl mrsl;

  @Override
  public void run() {
    MorselPack pack = mrsl.getMorselFile().getMorselPack();
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
          Mrsl.RM - c0w - 3 - FLAG_PAD - DATE_COL);
    }

    final int postFlagsSpace = Mrsl.RM - table.getColStart(4);
    final int postDateSpace = postFlagsSpace - DATE_COL;

    // there are more efficient ways to do below
    // (if it should become an issue)..

    for (long rn : allRns) {
      int ei = Collections.binarySearch(entryRns, rn);
      int ti = Collections.binarySearch(trailRns, rn);

      final String e, t;
      e = ei < 0 ? null : ENT_TAG;
      t = ti < 0 ? null : TRL_TAG;

      String date;
      if (t != null)
        date = new Date(pack.crumTrail(rn).crum().utc()).toString();
      else
        date = null;

      if (e != null) {

        SourceRow srcRow = pack.getSourceByRowNumber(rn);
        
        String srcDesc = srcRow.toString(" ", Mrsl.RED_TOKEN);

        if (date == null) {

          int spaceAvail = postFlagsSpace;
          if (srcDesc.length() > spaceAvail)
            srcDesc = srcDesc.substring(0, spaceAvail - 2) + "..";
          table.printRow("[" + rn + "]", null, e, t, srcDesc);

        } else  {

          int spaceAvail = postDateSpace - date.length();
          if (srcDesc.length() > spaceAvail)
            srcDesc = srcDesc.substring(0, spaceAvail - 2) + "..";
          
          table.printRow("[" + rn + "]", null, e, t, date);

        }


      } else  // e == null
        table.printRow("[" + rn + "]", null, e, t, date);
    }

    table.println();
    int rows = allRns.size();
    int ents = entryRns.size();
    int trails = trailRns.size();
    table.println(
        nOf(rows, "row") + ", " + nOf(trails, "crumtrail") + ", " + nOf(ents, "source-row") + ".");
    table.println();
  }
}




@Command(
    name = "history",
    description = "List rows witnessed"
    )
class History implements Runnable {
  
  @ParentCommand
  private Mrsl mrsl;

  @Override
  public void run() {
    var morselFile = mrsl.getMorselFile();
    MorselPack pack = morselFile.getMorselPack();

    PrintStream out = System.out;
    out.println();
    
    String prettyName = "<" + morselFile.getFile().getName() + ">";
    
    List<Long> trailedRns = pack.trailedRowNumbers();
    if (trailedRns.isEmpty()) {
      out.println("No crumtrails in " + prettyName);
      return;
    }

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
}




/**
 * Subcommand. About 100 lines of duplicated code could be removing using a
 * more functional style--avoided since deduping would add its own lines +
 * increase cognitive load.
 */
@Command(
    name = Entry.NAME,
    description = "Output selected source rows (text or JSON)"
    )
class Entry implements Callable<Integer> {
  
  final static String NAME = "entry";

  private final static String SEP_S = "%s";
  private final static String SEP_T = "%t";
  
  @ParentCommand
  private Mrsl mrsl;
  
  @Spec
  private CommandSpec spec;
  
  @Mixin
  private JsonOptions jsonOpt;
  @Mixin
  private SaveOption saveOpt;
  
  @Option(names = { "--slim" }, description = "Output skinny JSON")
  private boolean slim;
  
  @Option(names = { "-t", "--time"}, description = "Include crumtrails (witness records)")
  private boolean time;
  
  
  @Option(names = { "--col-widths"}, paramLabel = "WIDTHS", description = "Column widths for table text output")
  private String columnWidthSpec;
  
  @Option(names = { "--col-sep"}, paramLabel = "SEP",
      description = {
          "Column separator for table text output",
          "Character aliases:",
          "  %" + SEP_S + "  -> ' '  (space)",
          "  %" + SEP_T + " -> '\\t'  (tab)",
          "Default: ' ' (single space)"})
  private String columnSep;
  
  
  @Option(names = {"-n", "--no-rownum"}, description = "Omit row numbers for table text output")
  private boolean noRowNum;
  
  
  @Parameters(
      paramLabel = "NUMS",
      arity = "1..*",
      description = {
          "List of (candidate) row numbers",
          "express ranges using dash, e.g. 1-1000",
          
      })
  public void setRowNums(String[] rowNums) {
    var argList = new ArgList(rowNums);
    rowNumbers = argList.removeNumbers(true);
    if (!argList.isEmpty())
      throw new ParameterException(spec.commandLine(), "row number args not resolved: " + argList);
  }
  private List<Long> rowNumbers;
  
  /** If it's <em>slim</em>, then it's also JSON. */
  
  boolean isJson() {
    return slim || jsonOpt.isJson();
  }

  @Override
  public Integer call() {
    if (saveOpt == null)
      saveOpt = new SaveOption();
    if (jsonOpt == null)
      jsonOpt = new JsonOptions();
    MorselPack pack = mrsl.getMorselFile().getMorselPack();
    var selectedRns = new TreeSet<>(Sets.sortedSetView(pack.sourceRowNumbers()));
    selectedRns.retainAll(rowNumbers);
    if (selectedRns.isEmpty()) {
      System.out.println("No entries match the given row " + pluralize("number", rowNumbers.size()));
      return 0;
    }
    int trailIndex = time ? pack.indexOfNearestTrail(selectedRns.first()) : -1;
    
    try {
      return isJson() ?
          json(pack, selectedRns, trailIndex) :
            text(pack, selectedRns, trailIndex);
    } catch (IOException iox) {
      return saveOpt.onIoError(NAME, iox);
    }
  }
  
  
  private int json(MorselPack pack, SortedSet<Long> entryRns, int trailIndex) throws IOException {

    List<TrailedRow> trails;
    TrailedRow nextTrail;
    long nextTrailedRn;
    
    if (trailIndex == -1) {
      trails = null;
      nextTrail = null;
      nextTrailedRn = Long.MAX_VALUE;
    } else {
      trails = pack.getTrailedRows();
      nextTrail = trails.get(trailIndex);
      nextTrailedRn = nextTrail.rowNumber();
      trails = pack.getTrailedRows();
    }
    
    DateFormat dateFormat = getDateFormat(pack);
    
    var rowParser = new SourceRowParser(dateFormat);
    var trailParser = new TrailedRowWriter(dateFormat);
    
    var rowWriter = slim ? rowParser.toSlim() : rowParser;
    var trailWriter = slim ? trailParser.toSlim() : trailParser;
    
    var jArray = new JSONArray();
    
    boolean inject = false;
    int trailCount = 0;
    for (long rn : entryRns) {
      var srcRow = pack.getSourceByRowNumber(rn);
      final boolean advanceTrail = rn >= nextTrailedRn;
      inject = rn == nextTrailedRn;
      if (advanceTrail && !inject)
        jArray.add(trailWriter.toJsonObject(nextTrail));
      
      var jObj = inject ?
          rowParser.toSlimJsonObject(srcRow, nextTrail) :
            rowWriter.toJsonObject(srcRow);
      jArray.add(jObj);
      
      if (advanceTrail) {
        ++trailCount;
        if (++trailIndex >= trails.size()) {
          nextTrail = null;
          nextTrailedRn = Long.MAX_VALUE;
        } else {
          nextTrail = trails.get(trailIndex);
          nextTrailedRn = nextTrail.rowNumber();
        }
      }
    }
    
    if (nextTrail != null && !inject) {
      ++trailCount;
      jArray.add(trailWriter.toJsonObject(nextTrail));
    }
    
    boolean indent = !jsonOpt.isPack();
    boolean save = saveOpt.isSaveEnabled();
    
    try (var closer = new TaskStack()) {
      if (indent) {
        JsonPrinter printer;
        if (save) {
          var out = new FileWriter(saveOpt.getSaveFile());
          closer.pushClose(out);
          printer = new JsonPrinter(out);
        } else
          printer = new JsonPrinter(System.out);
        
        printer.print(jArray);
      } else {  // not indenting
        if (save) {
          var out = new FileWriter(saveOpt.getSaveFile());
          closer.pushClose(out);
          jArray.writeJSONString(out);
        } else {
          jArray.writeJSONString(IoBridges.toWriter(System.out));
        }
      }
      
      if (save)
        System.out.println(
            nOf(entryRns.size(), "row") + ", " + nOf(trailCount, "trail") + " saved to file.");
      else
        System.out.println();

      return 0;
    }
  }
  
  
  private int text(MorselPack pack, SortedSet<Long> entryRns, int trailIndex) throws IOException {
    
    final boolean includeRns = !noRowNum;
    final boolean saving = saveOpt.isSaveEnabled();
    
    final List<Integer> colWidths;
    if (columnWidthSpec == null)
      colWidths = null;
    else {
      colWidths = NumbersArg.parseInts(columnWidthSpec);
      if (colWidths == null)
        throw new ParameterException(spec.commandLine(), "Bad --col-widths parameter: " + columnWidthSpec);
    }

    // set the column separator
    final String sep;
    if (columnSep == null) {
      sep = colWidths == null ? " " : ""; // if column widths are set, default to no separator
    } else {
      sep = columnSep.replace(SEP_S, " ").replace(SEP_T, "\t");
    }
    
    try (var closer = new TaskStack()) {

      TablePrint table;
      {
        PrintStream out;
        if (saving) {
          out = new PrintStream(saveOpt.getSaveFile());
          closer.pushClose(out);
        } else
          out = System.out;
        
        if (colWidths == null) {
          // w/ possible exception of the first column,
          // columns are simply appended
          int fcw = includeRns ?
                entryRns.last().toString().length() + 2 + Mrsl.RN_PAD : 2;
          
          table = new TablePrint(out, fcw);
          
        } else
          table = new TablePrint(out, colWidths);  // user-defined
      }
      table.setColSeparator(sep);
      
      
      
      List<TrailedRow> trails;
      TrailedRow nextTrail;
      long nextTrailedRn;
      
      if (trailIndex == -1) {
        trails = null;
        nextTrail = null;
        nextTrailedRn = Long.MAX_VALUE;
      } else {
        trails = pack.getTrailedRows();
        nextTrail = trails.get(trailIndex);
        nextTrailedRn = nextTrail.rowNumber();
        trails = pack.getTrailedRows();
      }
      
      DateFormat dateFormat = getDateFormat(pack);
      final int fsi = includeRns ? 1 : 0;
      
      for (long rn : entryRns) {
        var srcRow = pack.getSourceByRowNumber(rn);
        var cols = srcRow.getColumns();
        Object[] colValues = new Object[cols.size() + fsi];
        for (int index = cols.size(); index-- > 0; ) {
          var col = cols.get(index);
          
          Object value;
          
          switch (col.getType()) {
          case BYTES:
            value = "(" + nOf(((BytesValue) col).size(), "byte") + ")";
            break;
          case HASH:
            value = Mrsl.RED_TOKEN;
            break;
          case DATE:
            {
              Date date = new Date(((DateValue) col).getUtc());
              value = dateFormat == null ? date : dateFormat.format(date);
            }
            break;
          case NULL:
            value = "NULL";
            break;
          default:
            value = col.getValue();
          }
          
          colValues[index + fsi] = value;
        }
        
        if (includeRns)
          colValues[0] = saving ? rn : "[" + rn + "]";
        
        final boolean advanceTrail;
        if (rn > nextTrailedRn) {
          advanceTrail = true;
          printNextTrail(nextTrail, table, dateFormat, includeRns);
        } else
          advanceTrail = rn == nextTrailedRn;
        
        table.printRow(colValues);
        
        if (rn == nextTrailedRn)
          printNextTrail(nextTrail, table, dateFormat, includeRns);
        
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
      
      if (nextTrail != null)
        printNextTrail(nextTrail, table, dateFormat, includeRns);
      
      if (!saving)
        table.println();

      return 0;
    }
  }
  

  private void printNextTrail(TrailedRow nextTrail, TablePrint table, DateFormat dateFormat, boolean includeRn) {
    Date date = new Date(nextTrail.utc());
    String msg = "<< Witnessed " + (dateFormat == null ? date : dateFormat.format(date)) + " >>";
    if (includeRn)
      table.printRow("[" + nextTrail.rowNumber() + "]", msg);
    else
      table.println(msg);
  }
  
  private DateFormat getDateFormat(MorselPack pack) {
    var info = pack.getMetaPack().getSourceInfo();
    return info.isEmpty() ? null : info.get().getDateFormat().orElse(null);
  }
  
}




@Command(
    name = Merge.NAME,
    description = "Merge morsels from the same ledger"
    )
class Merge implements Callable<Integer> {
  
  final static String NAME = "merge";
  
  @ParentCommand
  private Mrsl mrsl;
  
  @Spec
  private CommandSpec spec;
  
  
  private List<File> morselFiles;
  
  @Parameters(
      paramLabel = "FILES",
      arity = "1..*",
      description = "Morsel file[s] to merge with first")
  public void setMergeFiles(String[] files) {
    var argList = new ArgList(files);
    morselFiles = argList.removeExistingFiles();
    if (!argList.isEmpty())
      throw new ParameterException(
          spec.commandLine(),
          pluralize("non-existent file", argList.size()) + ": " + argList);
  }
  
  @Mixin
  private ReqSaveOption saveOpt;
  
  
  // TODO: not implemented yet
  
  @Option(
      names = "--ig-source-misses",
      description = {
          "Ignore (drop) source rows that can't be merged",
          "(Happens if the source row number is very near",
          "the morsel's last row number.)"
      })
  private boolean igSourceMisses;

  @Override
  public Integer call() {
    final int count = this.morselFiles.size() + 1;
    
    var out = System.out;
    
    out.println();
    out.println("Loading " + count + " morsels for merge..");
    
    ArrayList<MorselFile> sources = new ArrayList<>(count);
    sources.add(mrsl.getMorselFile());
    out.println(" " + sources.get(0).getFile());
    MorselFile auth;
    {
      for (File f : morselFiles) {
        out.print(" " + f);
        sources.add(mrsl.getMorselFile(f));
        out.println();
      }
      
      Collections.sort(sources, (a, b) -> compareMorsels(a, b));
      
      auth = sources.remove(sources.size() - 1);
    }

    var authPack = auth.getMorselPack();
    out.println();
    out.println(" authority: " + auth.getFile());
    out.println("  hi: " + authPack.hi());
    out.print("  " + nOf(authPack.sourceRowNumbers().size(), "source row"));
    out.println(", " + nOf(authPack.trailedRowNumbers().size(), "crumtrail"));
    
    
    MorselPackBuilder builder = new MorselPackBuilder();
    builder.init(auth.getMorselPack());

    int srcsAdded = auth.getMorselPack().sourceRowNumbers().size();
    int trailsAdded = auth.getMorselPack().trailedRowNumbers().size();
    
    // TODO: not printed ATM
    int missedSrcs = 0;
    int missedTrails = 0;
    
    for (var morsel : Lists.reverse(sources)) {
      
      out.println(" " + morsel.getFile());
      
      MergeResult result;
      try {
        result = builder.mergeSources(morsel.getMorselPack());
      } catch (HashConflictException hcx) {
        throw new ParameterException(
            spec.commandLine(),
            "<%s> not from same ledger: %s".formatted(morsel.getFile().toString(), hcx.getMessage()));
      }
      
      srcsAdded += result.srcRowsAdded();
      trailsAdded += result.trailsAdded();
      if (result.success()) {
        if (result.nothingDone())
          out.println("  0 objects added");
        else {
          out.print("  " + nOf(result.srcRowsAdded(), "source row"));
          out.println(", " + nOf(result.trailsAdded(), "crumtrail") + " added");
        }
      } else {
        // failed
        int failedSrcCount = result.failedSrcRns().size();
        int failedTrailCount = result.failedTrailRns().size();
        
        missedSrcs += failedSrcCount;
        missedTrails += failedTrailCount;
        
        out.print("  [WARNING] ");
        if (result.nothingDone())
          out.println("didn't merge any source rows or crumtrails");
        else
          out.println("some source rows or crumtrails were missed");
        
        out.println("  %s missed; %s added".formatted(
            nOf(failedSrcCount, "source row"),
            nOf(result.srcRowsAdded(), "source row")));
        out.println("  %s missed; %s added".formatted(
            nOf(failedTrailCount, "crumtrail"),
            nOf(result.trailsAdded(), "crumtrail")));
      }
    }
    try {
      File dest = MorselFile.createMorselFile(saveOpt.getSaveFile(), builder);
      
      out.println();
      out.println(nOf(srcsAdded, "source row") + " and " + nOf(trailsAdded, "crumtrail") + " merged to " + dest);
    } catch (IOException iox) {
      return saveOpt.onIoError(NAME, iox);
    }
    return 0;
  }
  

  
  private int compareMorsels(MorselFile a, MorselFile b) {
    MorselPack packA = a.getMorselPack();
    MorselPack packB = b.getMorselPack();
    int comp = Long.compare(packA.hi(), packB.hi());
    if (comp == 0) {
      var rowsA = packA.sourceRowNumbers();
      var rowsB = packB.sourceRowNumbers();
      if (rowsA.isEmpty())
        comp = rowsB.isEmpty() ? 0 : -1;
      else if (rowsB.isEmpty())
        comp = 1;
      else
        comp = rowsA.get(rowsA.size() - 1).compareTo(rowsB.get(rowsB.size() - 1));
    }
    return comp;
  }
  
}



@Command(
    name = Submerge.NAME,
    description = "Write rows, redact columns, to new morsel"
    )
class Submerge implements Callable<Integer> {

  final static String NAME = "submerge";
  
  private final static String REDACT_OPT = "--redact-cols";
  
  @ParentCommand
  private Mrsl mrsl;
  
  @Spec
  private CommandSpec spec;
  
  @Mixin
  private ReqSaveOption saveOpt;
  
  
  private String rowSpec;
  private List<Long> srcRns;
  
  
  private List<Integer> redactCols = List.of();
  
  
  @Parameters(
      paramLabel = "ROWS",
      arity = "1",
      description = {
          "Comma-separated row numbers to match",
          "Use dash for ranges. Eg: 466,308,592-598,717"})
  public void setSourceRows(String rowSpec) {
    this.srcRns = NumbersArg.parse(rowSpec);
    if (srcRns == null || srcRns.isEmpty())
      throw new ParameterException(
          spec.commandLine(), "invalid row numbers argument: " + rowSpec);
    this.rowSpec = rowSpec;
    srcRns = Lists.sortRemoveDups(srcRns);
    var first = srcRns.get(0);
    if (first < 1)
      throw new ParameterException(
          spec.commandLine(), "invalid row number (%d): %s".formatted(first, rowSpec));
  }
  
  @Option(
      names = REDACT_OPT,
      paramLabel = "COLS",
      arity = "0..1",
      description = {
          "Comma-separated column numbers to redact",
          "Use dash for ranges. Eg: 8,12-17,23"})
  public void setRedactColumns(String redactColumns) {
    this.redactCols = NumbersArg.parseInts(redactColumns);
    if (this.redactCols == null)
      throw new ParameterException(
          spec.commandLine(), "invalid " + REDACT_OPT + " argument: " + redactColumns);
    assert !redactCols.isEmpty();
    redactCols = Lists.sortRemoveDups(redactCols);
    var first = redactCols.get(0);
    if (first < 1)
      throw new ParameterException(
          spec.commandLine(), "invalid row number (%d): %s".formatted(first, redactColumns));
  }
  


  @Override
  public Integer call() {
    var out = System.out;
    out.println();
    
    var morsel = mrsl.getMorselFile();
    var pack = morsel.getMorselPack();
    
    srcRns = srcRns.stream().filter(pack::containsSourceRow).toList();
    if (srcRns.isEmpty())
      throw new ParameterException(
          spec.commandLine(), "no source rows match '" + rowSpec + "'");

    
    var builder = new MorselPackBuilder();
    
    int trails = builder.initWithSources(pack, srcRns, redactCols, null);
    
    builder.setMetaPack(pack.getMetaPack());
    
    File dest;
    try {
      dest = MorselFile.createMorselFile(saveOpt.getSaveFile(), builder);
    } catch (IOException iox) {
      return saveOpt.onIoError(NAME, iox);
    }
    System.out.println(
        nOf(srcRns.size(), "source row") + ", " + nOf(trails, "crumtrail") +
        " written to " + dest);
    return 0;
  }
  
}



@Command(
    name = Dump.NAME,
    description = {
        "Dump entire morsel as JSON (excluding meta data)",
        }
    )
class Dump implements Callable<Integer> {
  
  final static String NAME = "dump";
  
  @ParentCommand
  private Mrsl mrsl;
  
  @Mixin
  private PackOption packOpt;

  @Mixin
  private SaveOption saveOpt;

  @Override
  public Integer call() {
    MorselFile morsel = mrsl.getMorselFile();
    
    final boolean compact = packOpt.isPack();
    
    var morselObj = MorselDumpWriter.INSTANCE.toJsonObject(morsel.getMorselPack());
    
    try (var closer = new TaskStack()) {
      if (saveOpt.isSaveEnabled()) {
        var writer = new FileWriter(saveOpt.getSaveFile());
        closer.pushClose(writer);
        if (compact)
          morselObj.writeJSONString(writer);
        else
          new JsonPrinter(writer).print(morselObj);
      }
      else if (compact)               // not saving
        morselObj.writeJSONString(IoBridges.toWriter(System.out));
      else
        new JsonPrinter(System.out).print(morselObj);
    } catch (IOException iox) {
      return saveOpt.onIoError(NAME, iox);
    }
    return 0;
  }
  
}

// TODO
class Verify {
  
}












@Command(
    name = Report.NAME,
    description = "Generate PDF report from template if present. (Alpha)"
    )
class Report implements Callable<Integer> {
  
  final static String NAME = "report";
  final static String ARGS = "ARGS";
  final static String A_OPT = "-a";
  final static String ABOUT_OPT = "--about";
  
  @ParentCommand
  private Mrsl mrsl;
  
  @Spec
  private CommandSpec spec;
  
  
  @Option(
      names = {A_OPT, ABOUT_OPT},
      description = {
          "Show the named arguments (@|fg(yellow) " + ARGS + "|@) template takes and exit",
          }
      )
  private boolean showInfo;
  

  /**
   * Note this isn't quite right: it's required (!) if --about is not set.
   */
  @Mixin
  private SaveOption saveOpt;
  
  @Parameters(
      paramLabel = ARGS,
      arity = "0..*",
      description = {
          "List of required or optional name=@|italic value|@ arguments template",
          "  takes (see option @|fg(yellow) " + A_OPT + "|@). If the template only takes a",
          "  single argument, then @|italic value|@ needn't be named.",
      })
  public void setQueryArgs(String[] args) {
    if (args == null || args.length == 0) {
      queryArgs = Map.of();
      return;
    }
    // parse the numbers
    var parser = NumberFormat.getInstance();
    Map<String, Number> input;
    if (args.length == 1) {
      var arg = args[0];
      int eqIndex = arg.indexOf('=');
      String name;
      Number value;
      try {
        if (eqIndex == -1) {
          name = "";
          value = parser.parse(arg);
        } else {
          name = arg.substring(0, eqIndex);
          value = parser.parse(arg.substring(eqIndex + 1));
        }
        input = Map.of(name, value);
      } catch (ParseException px) {
        throw new ParameterException(spec.commandLine(), "illegal argument: '" + arg + "'");
      }
    } else {
      var map = new HashMap<String, Number>();
      for (var arg : args) try {
        int eqIndex = arg.indexOf('=');
        if (eqIndex == -1)
          throw new IllegalArgumentException(
              "'" + arg + "' must be in name=<value> format");
        var name = arg.substring(0, eqIndex);
        var value = parser.parse(arg.substring(eqIndex + 1));
        map.put(name, value);
      } catch (ParseException px) {
        throw new ParameterException(spec.commandLine(), "illegal argument: '" + arg + "'");
      }
      input = Collections.unmodifiableMap(map);
    }
    
    queryArgs = input;
  }
  
  private Map<String, Number> queryArgs = Map.of();

  @Override
  public Integer call() throws Exception {
    if (showInfo) {
      showInfo();
      return 0;
    } else {
      return makeReport();
    }
  }
  
  
  private int makeReport() {
    if (template().isEmpty()) {
      System.err.println("[ERROR] Morsel does not contain report template");
      return Mrsl.ERR_USER;
    }
    var sources = mrsl.getMorselFile().getMorselPack().sources();
    if (sources.isEmpty())
      throw new ParameterException(
          spec.commandLine(), "no source rows found in morsel file");
    
    var report = template().get();
    
    int requiredArgs = report.getRequiredNumberArgs().size();
    if (requiredArgs > queryArgs.size()) {
      throw new ParameterException(
          spec.commandLine(),
          "report template requires " + nOf(requiredArgs, "number arg"));
    }
    
    Map<String, Number> inputs;
    
    if (queryArgs.size() == 1 && queryArgs.containsKey("")) {
      Number value = queryArgs.get("");
      if (requiredArgs != 1) {
        throw new ParameterException(
            spec.commandLine(),
            "unnamed value (" + value + "); report template requires " +
            nOf(requiredArgs, "number arg"));
      }
      String name = report.getRequiredNumberArgs().get(0).name();
      inputs = Map.of(name, value);
    } else
      inputs = queryArgs;
    
    if (saveOpt == null || !saveOpt.isSaveEnabled())
      throw new ParameterException(
          spec.commandLine(), "no path specified to save PDF file");
    
    File target = saveOpt.getSaveFile();
    
    report.bindNumberArgs(inputs);
    
    try {
      report.writePdf(target, sources);
      
      System.out.println("PDF report written to " + target);
      System.out.println();
    
    } catch (Exception x) {
      System.err.println("[ERROR] Failed to generate PDF report: " + x.getMessage());
      System.err.println("        Since this is a new feature, this is likely a bug.");
      System.err.println("        Call stack:");
      System.err.println();
      x.printStackTrace();
      System.err.println();
      System.err.println("        If not already reported, please open an \"issue\" at");
      System.err.println("        <https://github.com/crums-io/skipledger/issues>");
      return Mrsl.ERR_SOFT;
    }
    
    return 0;
  }
  
  
  private void showInfo() {
    var printer = new PrintSupport();
    if (template().isEmpty()) {
      printer.println("No report template found.");
      printer.println();
      return;
    }
    var report = template().get();
    printer.println("Report template found.");
    printer.println();
    report.getDescription().ifPresent(
        desc -> {
          printer.setIndentation(1);
          printer.println("Description:");
          printer.println();
          printer.printParagraph(desc);
          printer.println();
          printer.setIndentation(0);
        });
    
    printer.println("Arguments:");
    printer.println();
    printer.setIndentation(1);
    
    var args = report.getNumberArgs();
    final int argCount = args.size();
    if (argCount == 0) {
      printer.println("NONE");
      printer.println();
      return;
    }
    for (var arg : args) {
      var param = arg.param();
      printer.println("Name:");
      printer.incrIndentation(1);
      printer.println(param.name());
      printer.decrIndentation(1);
      param.getDescription().ifPresent(
          desc -> {
            printer.println("Description:");
            printer.incrIndentation(1);
            printer.printParagraph(desc);
            printer.decrIndentation(1);
          });
      param.getDefaultValue().ifPresent(
          value -> {
            printer.println("Default: " + value);
          });
      printer.println();
    }
    
    printer.setIndentation(0);
    var requiredArgs = requiredArgs(report);
    
    final int reqArgCount = requiredArgs.size();
    switch (reqArgCount) {
    case 0:
      {
        var msg = argCount > 1 ?
            "All template arguments have defaults: " :
              "Template argument has default: ";
        printer.println(msg + "no user input necessary.");
      }
      break;
    case 1:
      {
        var msg = argCount > 1 ?
            "Only one argument requires a value (not defaulted):" :
              "The following value is required (not defaulted):";
        printer.println(msg);
        printer.println("  " + requiredArgs.get(0));
        printer.println("It may be supplied either as a name=value pair");
        printer.println("  " + requiredArgs.get(0) + "=<value>");
        printer.println("or without the name (just <value>).");
        printer.println();
      }
      break;
    default:
      printer.println("Report template takes " + reqArgCount + " required arguments named");
      requiredArgs.forEach(name -> printer.println("  " + name));
      printer.println("supplied as name=<value> pairs.");
    }
  }
  
  
  
  
  private List<String> requiredArgs(ReportTemplate report) {
    return Lists.map(report.getRequiredNumberArgs(), NumberArg::name);
  }
  
  
  private Optional<ReportTemplate> template() {
    if (template == null)
      template =  ReportAssets.getReport(
          mrsl.getMorselFile().getMorselPack().getAssets());
    return template;
  }
  private Optional<ReportTemplate> template;
  
}









































