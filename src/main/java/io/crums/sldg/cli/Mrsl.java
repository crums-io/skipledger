/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cli;


import static io.crums.sldg.SldgConstants.MRSL_EXT;
import static io.crums.util.Strings.nOf;
import static io.crums.util.Strings.pluralize;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.io.IoBridges;
import io.crums.model.CrumTrail;
import io.crums.sldg.ByteFormatException;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.MorselFile;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SldgException;
import io.crums.sldg.json.MorselDumpWriter;
import io.crums.sldg.json.SourceInfoParser;
import io.crums.sldg.json.SourceRowParser;
import io.crums.sldg.json.TrailedRowWriter;
import io.crums.sldg.packs.MorselPack;
import io.crums.sldg.packs.MorselPackBuilder;
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
import io.crums.util.main.Option;
import io.crums.util.main.OptionGroup;
import io.crums.util.main.PrintSupport;
import io.crums.util.main.StdExit;
import io.crums.util.main.TablePrint;

/**
 * CLI for morsel files.
 * 
 * <h3>TODO</h3>
 * <pre>
 *  - merge/submerge option: meta=path/to/meta_file
 *  - morsel user-comment string
 * </pre>
 */
public class Mrsl extends BaseMain {
  
  public final static String PROGNAME = Mrsl.class.getSimpleName().toLowerCase();
  
  private String command;
  private File morselFile;
  
  private List<Long> rowNumbers;
  private File saveFile;
  private String sep = " ";
  private List<Integer> colWidths;
  
  private List<Integer> redactCols = Collections.emptyList();
  
  private List<File> morselFiles;
  
  private Set<Option> options = Collections.emptySet();
  
  /**
   * morsel comment.
   */
  private String comment;
  
  
  private Mrsl() {
  }
  
  
  public static void main(String[] args) {
    new Mrsl().doMain(args);
  }

  @Override
  protected void init(String[] args) throws IllegalArgumentException, IOException {
    ArgList argList = newArgList(args);
    
    this.command = argList.removeCommand(SUM, INFO, STATE, LIST, HISTORY, ENTRY, MERGE, SUBMERGE, DUMP);
    
    if (!MERGE.equals(command))
      this.morselFile = argList.removeExistingFile();
      
    
    switch (command) {
    case SUM:
    case STATE:
    case LIST:
    case HISTORY:
      break;
      
    case INFO:
      options = OptionGroup.remove(argList, JSON_OPT, PACK_OPT);
      setSaveFile(argList);
      if (saveFile != null && options.isEmpty())
        options = Collections.singleton(JSON_OPT);
      break;
      
    case ENTRY:
      this.rowNumbers = argList.removeNumbers(true);
      if (rowNumbers.isEmpty())
        throw new IllegalArgumentException("missing entry row numbers");
      
      options = OptionGroup.remove(argList, JSON_OPT, SLIM_OPT, TIME_OPT, PACK_OPT, NO_ROWNUM_OPT);
      setSaveFile(argList);
      if (isJson()) {
        if (options.contains(NO_ROWNUM_OPT))
          throw new IllegalArgumentException(
              NO_ROWNUM_OPT + " option cannot be used with JSON: " + argList.getArgString());
      
      } else {
        // text output (not json)
        if (options.contains(PACK_OPT))
          throw new IllegalArgumentException(
              PACK_OPT + " option can only be used with JSON: " + argList.getArgString());
        
        // set the column display widths
        String columnWidths = argList.removeValue(COL_SIZES);
        if (columnWidths != null) {
          this.colWidths = NumbersArg.parseInts(columnWidths);
          if (colWidths == null || colWidths.isEmpty() || colWidths.size() > 0xffff)
            throw new IllegalArgumentException(columnWidths);
          for (int index = colWidths.size(); index-- > 0; ) {
            int w = colWidths.get(index);
            if (w < MIN_COL_DISPLAY_WIDTH)
              throw new IllegalArgumentException(
                  "col [" + (index + 1) + "] display size (" + w + ") in '" +
                  COL_SIZES + "=" + columnWidths + "' is less than minimum (" +
                  MIN_COL_DISPLAY_WIDTH + ")");
            if (w > MAX_COL_DISPLAY_WIDTH)
              throw new IllegalArgumentException(
                  "col [" + (index + 1) + "] display size (" + w + ") in '" +
                  COL_SIZES + "=" + columnWidths + "' is greater than maximum (" +
                  MIN_COL_DISPLAY_WIDTH + ")");
          }
        }
        
        // set column separator
        String separator = argList.removeValue(SEP);
        if (separator != null) {
          separator = separator.replace(SEP_S, " ");
          separator = separator.replace(SEP_T, "\t");
          this.sep = separator;
        } else if (this.colWidths != null)
          this.sep = "";  // if column widths are set, default to no separator
        
      }
      break;
      
    case MERGE:
      this.morselFiles = argList.removeExistingFiles();
      
      if (morselFiles.size() < 2)
        throw new IllegalArgumentException(
            morselFiles.isEmpty() ?
                "missing morsel files for merge" : "missing 2nd morsel file for merge");
      

      setSaveFile(argList);
      break;
      
    case SUBMERGE:
      this.rowNumbers = argList.removeNumbers(true);
      if (rowNumbers.isEmpty())
        throw new IllegalArgumentException("missing row numbers");
      {
        var set = new TreeSet<>(rowNumbers);
        if (set.size() < rowNumbers.size())
          throw new IllegalArgumentException(
              "duplicate row numbers specified: " + argList.getArgString());
        this.rowNumbers = new ArrayList<>(set.size());
        rowNumbers.addAll(set);
        
      }
      setSaveFile(argList);
      this.redactCols = getRedactColumns(argList);
      break;
      
    case DUMP:
      setSaveFile(argList);
      this.options = PACK_OPT.removeOption(argList);
      break;
      
    default:
      throw new RuntimeException("assertion failed. command=" + command);
    }
    
    
    argList.enforceNoRemaining();
  }
  
  private final static int MAX_COL_DISPLAY_WIDTH = 512;
  private final static int MIN_COL_DISPLAY_WIDTH = 3;
  
  
  private boolean isJson() {
    return options.contains(JSON_OPT) || options.contains(SLIM_OPT);
  }
  
  
  private void setSaveFile(ArgList argList) {
    String filepath = argList.removeValue(SAVE);
    if (filepath != null) {
      saveFile = new File(filepath);
      checkSaveParentDir();
    }
  }
  
  
  private void checkSaveParentDir() {
    File parent = saveFile.getParentFile();
    if (parent != null && !parent.isDirectory())
      throw new IllegalArgumentException(
          "parent directory of given path does not exist: " + saveFile);
    
  }
  

  @Override
  protected void start() throws InterruptedException, IOException {
    try {
      
      startImpl();
      
    } catch (SldgException sx) {
      
      if (sx instanceof HashConflictException) {
        String msg;
        if (morselFile != null)
          msg = "Hash conflict in morsel file " + morselFile;
        else
          msg = "Hash conflict across morsel files " + morselFiles;
        
        if (sx.getMessage() != null)
          msg += ": " + sx.getMessage();
        
        throw new HashConflictException(msg, sx);
      
      } else if (sx instanceof ByteFormatException) {
        throw new ByteFormatException(
            "Illegal byte format in morsel file " + morselFile + ": " + sx.getMessage(), sx);
      
      } else
        throw sx;
    }
  }
  
  
  private void startImpl() throws InterruptedException, IOException, HashConflictException {
    switch (command) {
    case SUM:
      sum();
      break;
    case INFO:
      info();
      break;
    case STATE:
      state();
      break;
    case LIST:
      list();
      break;
    case HISTORY:
      history();
      break;
      
    case ENTRY:
      entry();
      break;
    case MERGE:
      merge();
      break;
    case SUBMERGE:
      submerge();
      break;
    case DUMP:
      dump();
      break;
    default:
      throw new RuntimeException("assertion failure. command=" + command);
    }
  }
  
  
  void state() {
    System.out.println(stateString(new MorselFile(morselFile).getMorselPack()));
  }

  
  void entry() throws IOException {
    if (options.contains(JSON_OPT) || options.contains(SLIM_OPT)) {
      entryJson();
    } else {
      entryText();
    }
  }

  
  void history() {
    MorselPack pack = new MorselFile(morselFile).getMorselPack();

    PrintStream out = System.out;
    out.println();
    
    String prettyName = "<" + morselFile.getName() + ">";
    
    List<Long> trailedRns = pack.trailedRowNumbers();
    if (trailedRns.isEmpty()) {
      out.println("No crumtrails in " + prettyName);
      return;
    }

    out.println();
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
  
  
  void submerge() throws IOException {
    MorselPack pack = new MorselFile(morselFile).getMorselPack();
    
    // check we have the source rows
    if (!Sets.sortedSetView(pack.sourceRowNumbers()).containsAll(rowNumbers)) {
      // identify *which rows are not present
      // Dirty hack: I know that this.rowNumbers is mutable
      this.rowNumbers.removeAll(pack.sourceRowNumbers());
      exitInputError("missing source rows: " + rowNumbers);
      return; // never reached
    }
    
    var builder = new MorselPackBuilder();
    
    int trails = builder.initWithSources(pack, rowNumbers, redactCols, comment);
    
    builder.setMetaPack(pack.getMetaPack());

    File dest = MorselFile.createMorselFile(saveFile, builder);
    
    System.out.println();
    System.out.println(
        nOf(rowNumbers.size(), "source row") + ", " + nOf(trails, "crumtrail") +
        " written to " + dest);
  }


  void merge() throws IOException {
    
    final int count = this.morselFiles.size();
    
    var out = System.out;
    
    out.println();
    out.println("Loading " + count + " morsels for merge..");
    
    ArrayList<MorselFile> sources = new ArrayList<>(count);
    MorselFile auth;
    {
      for (File f : morselFiles) {
        out.print(" " + f);
        sources.add(new MorselFile(f));
        out.println();
      }
      
      Collections.sort(sources, (a, b) -> compareMorsels(a, b));
      
      auth = sources.remove(sources.size() - 1);
    }

    out.println();
    out.println(" authority: " + auth.getFile());
    
    
    MorselPackBuilder builder = new MorselPackBuilder();
    int objects = builder.init(auth.getMorselPack());
    out.println();
    out.println("init " + nOf(objects, "object") + " (hi row " + auth.getMorselPack().hi() + ")");
    
    
    for (var morsel : sources) {
      
      out.print(" " + morsel.getFile() + " ..");
      int added = builder.addAll(morsel.getMorselPack());
      objects += added;
      out.println(". " + nOf(added, "object") + " added");
    }
    
    File dest = MorselFile.createMorselFile(saveFile, builder);
    
    out.println();
    out.println(objects + " objects merged to " + dest);
  }
  
  private int compareMorsels(MorselFile a, MorselFile b) {
    MorselPack packA = a.getMorselPack();
    MorselPack packB = b.getMorselPack();
    int comp = Long.compare(packA.hi(), packB.hi());
    if (comp == 0)
      comp = packA.getFullRowNumbers().size() - packB.getFullRowNumbers().size();
    return comp;
  }
  
  
  
  
  
  
  
  
  private SortedSet<Long> sourceRowNumSet(MorselPack pack) {
    return Sets.sortedSetView(pack.sourceRowNumbers());
  }
  
  
  void entryJson() throws IOException {
    MorselPack pack = new MorselFile(morselFile).getMorselPack();
    
    var entryRns = sourceRowsSelected(pack);
    
    int trailIndex = this.options.contains(TIME_OPT) ?
        pack.indexOfNearestTrail(entryRns.first()) : -1;
    
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
    
    boolean slim = options.contains(SLIM_OPT);
    
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
    
    boolean indent = !options.contains(PACK_OPT);
    boolean save = saveFile != null;
    

    try (var closer = new TaskStack()) {
      if (indent) {
        JsonPrinter printer;
        if (save) {
          var out = new FileWriter(saveFile);
          closer.pushClose(out);
          printer = new JsonPrinter(out);
        } else
          printer = new JsonPrinter(System.out);
        
        printer.print(jArray);
      } else {  // not indenting
        if (save) {
          var out = new FileWriter(saveFile);
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
    }
  }
  
  
  

  private DateFormat getDateFormat(MorselPack pack) {
    var info = pack.getMetaPack().getSourceInfo();
    return info.isEmpty() ? null : info.get().getDateFormat().orElse(null);
  }
  
  
  
  private TreeSet<Long> sourceRowsSelected(MorselPack pack) {

    TreeSet<Long> entryRns = new TreeSet<>(this.rowNumbers);
    entryRns.retainAll(sourceRowNumSet(pack));
    
    if (entryRns.isEmpty()) {
      System.err.println("[ERROR] No entries match the row numbers given.");
      StdExit.ILLEGAL_ARG.exit();
      return null; // never reached
    
    } else
      return entryRns;
  }
  
  
  void entryText() {
    try (var closer = new TaskStack()) {
      
      var pack = new MorselFile(this.morselFile).getMorselPack();
      
      TreeSet<Long> entryRns = sourceRowsSelected(pack);
      
      final boolean includeRns = !this.options.contains(NO_ROWNUM_OPT);
      final boolean saving = saveFile != null;
      
      TablePrint table;
      {
        var out = saving ? new PrintStream(saveFile) : System.out;
        if (saving)
          closer.pushClose(out);
        if (this.colWidths == null) {
          // w/ possible exception of the first column,
          // columns are simply appended
          int fcw = includeRns ?
                entryRns.last().toString().length() + 2 + RN_PAD : 2;
          
          table = new TablePrint(out, fcw);
          
        } else
          table = new TablePrint(out, colWidths);  // user-defined
        
      }
      
      table.setColSeparator(this.sep);
      
      int trailIndex = this.options.contains(TIME_OPT) ?
          pack.indexOfNearestTrail(entryRns.first()) : -1;
  
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
            value = RED_TOKEN;
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
    
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on writing to " + saveFile + " :: " + iox.getMessage(), iox);
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
  
  
  

  private final static String RED_TOKEN = "[X]";
  
  



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
        "Command line tool for reading and combining (merging) morsel files (" + MRSL_EXT + " ext). " +
        "Morsel files are tamper-proof tear-outs of rows from an opaque hash ledger optionally paired with the " +
        "source of those rows (i.e. the content whose hash is the input-hash in its corresponding " +
        " skip ledger row).";
    printer.printParagraph(paragraph);
    printer.println();
    printer.println("State morsels:");
    printer.println();
    paragraph =
        "Morsel files that contain no source rows are called \"state-morsels\". These represent the state of " +
        "the ledger (how many rows and maybe some history). Because they're small (measured in kB's) they're " +
        "used as a richer fingerprint of state than just a single SHA-256 hash: their internal structure allows " +
        "an evolving ledger's fingerprint to be verified for consistency with its previous version.";
    printer.printParagraph(paragraph);
    printer.println();
    printer.println("Merge operations:");
    printer.println();
    paragraph =
        "There are typically 2 reasons to \"" + MERGE + "\" morsel files into one. The first, is to just " +
        "gather source-row information from morsels from the same ledger into one morsel. The second, is to create an updated version " +
        "of a morsel that aligns with the ledger's current state hash using a state-morsel.";
    printer.printParagraph(paragraph);
    printer.println();
  }


  private final static int LEFT_TBL_COL_WIDTH = 11;
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
        "Commands in the table below each take a morsel file as argument. The order in which " +
        "arguments are entered doesn't matter. (Reminder: if a required command collides with a " +
        "local filename, prefix the filename with '." + File.separatorChar + "' in order to " +
        "disambiguate.)";
    printer.printParagraph(paragraph);
    printer.println();

    TablePrint table = new TablePrint(out, LEFT_TBL_COL_WIDTH, RIGHT_TBAL_COL_WIDTH);
    table.setIndentation(INDENT);
    
    table.printRow(SUM, "prints a summary of the morsel's contents");
    table.println();
    
    table.printRow(INFO, "prints meta information about the ledger, if any.");
    table.println();
    table.printRow(null, JSON_OPT + "  (or -" + JSON_OPT.getSym() + ")");
    table.printRow(null,  "If set, then the output is in JSON format");
    table.println();
    table.printRow(null, PACK_OPT + "  (or -" + PACK_OPT.getSym() + ")");
    table.printRow(null,  "If set, then JSON whitespace is removed");
    table.println();
    
    table.printRow(STATE, "prints the highest row number and that row's hash");
    table.println();
    
    table.printRow(LIST, "lists the morsel's rows and entries (source-rows)");
    table.println();
    
    table.printRow(HISTORY, "lists the witnessed rows and their evidence");
    table.println();
    
    table.printRow(ENTRY, "lists in detail the given source-rows or saves them to a file");
    table.printRow(null,  "Arguments:");
    table.println();
    table.printRow(null,   "<row-numbers>       (required)");
    table.println();
    table.printRow(null,   "Strictly ascending row numbers separated by commas or spaces");
    table.printRow(null,   "Ranges (incl.) may be substituted for numbers. For example:");
    table.printRow(null,   "308,466,592-598,717");
    table.println();
    table.printRow(null, SAVE + "=<path/to/file> (optional)");
    table.println();
    table.printRow(null,  "If provided, then source-rows are saved to the given file path");
    table.printRow(null,  "in the order given; otherwise they are printed to the console.");
    table.println();
    table.printRow(null, NO_ROWNUM_OPT + "  (or -" + NO_ROWNUM_OPT.getSym() + ")");
    table.printRow(null,  "Excludes row numbers when saving (" + SAVE + "=<path/to/file>)");
    table.printRow(null,  "in text format (i.e. not JSON).");
    table.println();
    table.printRow(null, SEP + "=<separator>     (optional)");
    table.println();
    table.printRow(null,  "In text format (i.e. not JSON) column values are separated with");
    table.printRow(null,  "with the given string. Character aliases:");
    table.printRow(null,  "  " + SEP_S + " -> ' '  (space)");
    table.printRow(null,  "  " + SEP_T + " -> '\\t' (tab)");
    table.printRow(null,  "To set with no separator, leave its value empty.");
    table.printRow(null,  "DEFAULT: ' '        (single whitespace)");
    table.println();
    table.printRow(null, COL_SIZES + "=<comma-separated-numbers>     (optional)");
    table.println();
    table.printRow(null,  "In text format (i.e. not JSON) column display widths are the");
    table.printRow(null,  "given comma-separated list of numbers.");
    table.println();
    table.printRow(null, JSON_OPT + "  (or -" + JSON_OPT.getSym() + ")");
    table.printRow(null,  "If set, then the output is in JSON format");
    table.println();
    table.printRow(null, SLIM_OPT + "  (or -" + SLIM_OPT.getSym() + ")");
    table.printRow(null,  "If set, then the JSON is in skinny form");
    table.println();
    table.printRow(null, TIME_OPT + "  (or -" + TIME_OPT.getSym() + ")");
    table.printRow(null,  "If set, then crumtrails (witness records) are included");
    table.println();
    table.printRow(null, PACK_OPT + "  (or -" + PACK_OPT.getSym() + ")");
    table.printRow(null,  "If set, then JSON whitespace is removed");
    table.println();

    table.printRow(MERGE, "merges the given morsel files to a new morsel file. The morsels");
    table.printRow(null,  "must come from the same ledger.");
    table.printRow(null,  "Arguments:");
    table.println();
    table.printRow(null,   "<path/to/morsel_1> <path/to/morsel_2> ..  (2 or more required)");
    table.println();
    table.printRow(null,   SAVE + "=<path/to/file> (optional)");
    table.printRow(null,  "DEFAULT: '.'        (current directory)");
    table.println();
    table.printRow(null,  "If path/to/file doesn't exist, then the merged morsel gets");
    table.printRow(null,  "created there; if the path is an existing directory, then a");
    table.printRow(null,  "file with a merge-generated name is created in that directory.");
    table.println();

    table.printRow(SUBMERGE, "creates a new morsel containing less information. Extracts a");
    table.printRow(null,  "the specified source rows, optionally redacting any of their");
    table.printRow(null,  "columns.");
    table.printRow(null,  "Arguments:");
    table.println();
    table.printRow(null,   "<row-numbers>       (required)");
    table.println();
    table.printRow(null,   "Strictly ascending row numbers separated by commas or spaces");
    table.printRow(null,   "Ranges (incl.) may be substituted for numbers. For example:");
    table.printRow(null,   "308,466,592-598,717");
    table.println();
    table.printRow(null,   SAVE + "=<path/to/file> (optional)");
    table.printRow(null,  "DEFAULT: '.'        (current directory)");
    table.printRow(null,  "(Same semantics as specified in '" + MERGE + "')");
    table.println();
    table.printRow(null, REDACT + "=<comma-separated-numbers>     (optional)");
    table.println();
    table.printRow(null,   "If provided, then values in the the given columns are redacted.");
    table.printRow(null,   "Column numbers are 1-based: the first is numbered 1.");
    table.println();

    table.printRow(DUMP,  "dumps a JSON representation of the entire morsel file");
    table.printRow(null,  "Arguments:");
    table.println();
    table.printRow(null, SAVE + "=<path/to/file> (optional)");
    table.println();
    table.printRow(null, PACK_OPT + "  (or -" + PACK_OPT.getSym() + ")");
    table.printRow(null,  "If set, then JSON whitespace is removed");
    table.println();
    
    
  }
  
  


  private final static int LEFT_SUM_COL_WIDTH = 17;
  private final static int RIGHT_SUM_COL_WIDTH = 25;
  private final static int MID_SUM_COL_WIDTH = RM - LEFT_SUM_COL_WIDTH - RIGHT_SUM_COL_WIDTH;
  

  void sum() {
    var morsel = new MorselFile(morselFile);
    MorselPack pack = morsel.getMorselPack();
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
    
    
    out.println("<" + stateString(pack) + ">");
  }
  
  
  
  private String stateString(MorselPack pack) {
    long hi = pack.hi();
    return hi + "-" + IntegralStrings.toHex(pack.rowHash(hi));
  }
  
  
  private final static String TRL_TAG = "W";
  private final static String ENT_TAG = "S";
  
  private final static int RN_PAD = 1;
  private final static int FLAG_PAD = 2;
  private final static int DATE_COL = 32;
  
  void list() {
    MorselPack pack = new MorselFile(morselFile).getMorselPack();
    
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
          RM - c0w - 3 - FLAG_PAD - DATE_COL);
    }

    final int postFlagsSpace = RM - table.getColStart(4);
    final int postDateSpace = postFlagsSpace - DATE_COL;

    // there are more efficient ways to do below
    // (if it should become an issue)..

    for (long rn : allRns) {
      int ei = Collections.binarySearch(entryRns, rn);
      int ti = Collections.binarySearch(trailRns, rn);

      String e, t;
      e = ei < 0 ? null : ENT_TAG;
      t = ti < 0 ? null : TRL_TAG;

      String date;
      if (t != null)
        date = new Date(pack.crumTrail(rn).crum().utc()).toString();
      else
        date = null;

      if (e != null) {

        SourceRow srcRow = pack.getSourceByRowNumber(rn);
        
        String srcDesc = srcRow.toString(" ", RED_TOKEN);

        if (date == null) {

          int spaceAvail = postFlagsSpace;
          if (srcDesc.length() > spaceAvail)
            srcDesc = srcDesc.substring(0, spaceAvail - 2) + "..";
          table.printRow("[" + rn + "]", null, e, t, srcDesc);

        } else  {

          int spaceAvail = postDateSpace - date.length();
          if (srcDesc.length() > spaceAvail)
            srcDesc = srcDesc.substring(0, spaceAvail - 2) + "..";
          
          table.printRow("[" + rn + "]", null, e, t, date, srcDesc);

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
  
  
  
  void info() {
    var pack = new MorselFile(morselFile).getMorselPack();
    var info = pack.getMetaPack().getSourceInfo();
    if (info.isEmpty()) {
      System.out.println();
      System.out.println("Morsel contains no meta information.");
      return;
    }
    if (isJson())
      infoJson(info.get());
    else
      infoText(info.get());
  }
  
  
  
  void infoJson(SourceInfo info) {
    var jObj = SourceInfoParser.INSTANCE.toJsonObject(info);
    boolean saving = saveFile != null;
    boolean compact = options.contains(PACK_OPT);
    
    try (var closer = new TaskStack()) {
      var out = saving ? new FileWriter(saveFile) : System.out;
      if (compact)
        jObj.writeJSONString(out);
      else
        new JsonPrinter(out).print(jObj);
        
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on writing " + saveFile + ": " + iox.getMessage(), iox);
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
  
  void dump() {
    var pack = new MorselFile(morselFile).getMorselPack();
    var morselObj = MorselDumpWriter.INSTANCE.toJsonObject(pack);
    
    final boolean saving = saveFile != null;
    final boolean compact = options.contains(PACK_OPT);
    
    
    try (var closer = new TaskStack()) {
      if (saving) {                     // saving
        var writer = new FileWriter(saveFile);
        closer.pushClose(writer);
        if (compact)
          morselObj.writeJSONString(writer);
        else
          new JsonPrinter(writer).print(morselObj);
        
      } else if (compact)               // not saving
        morselObj.writeJSONString(IoBridges.toWriter(System.out));
      else
        new JsonPrinter(System.out).print(morselObj);
      
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on saving to " + saveFile + ": " + iox.getMessage(), iox);
    }
    
    var out = System.out;
    out.println();
    
    if (saving) {
      boolean meta = pack.getMetaPack().getSourceInfo().isPresent();
      int srcRows = pack.sourceRowNumbers().size();
      int trails = pack.getTrailedRows().size();
      int fullRns = pack.getFullRowNumbers().size();
      int allRns = SkipLedger.coverage(pack.getFullRowNumbers()).tailSet(1L).size();
      
      out.print(allRns + " rows dumped to file; ");
      out.print(nOf(srcRows, "with source") + ", ");
      out.print(nOf(trails, "with crumtrail") + " (" + pluralize("witness record", trails) + "), ");
      out.print(fullRns + " fully linked");
      if (meta)
        out.print(", meta info included");
      out.println(".");
      out.println();
    }
      
  }
  
  
  private final static String SUM = "sum";
  private final static String INFO = "info";
  private final static String STATE = "state";
  private final static String LIST = "list";
  private final static String HISTORY = "history";
  private final static String ENTRY = "entry";
  private final static String MERGE = "merge";
  private final static String SUBMERGE = "submerge";
  private final static String DUMP = "dump";
  
  // named values (name=value)
  
  private final static String SAVE = "save";
  private final static String SEP = "sep";
  private final static String SEP_S = "%s";
  private final static String SEP_T = "%t";
  

  
  private final static String COL_SIZES = "col-sizes";
  
  
  // O P T I O N S
  
  private final static Option JSON_OPT = new Option("json");
  private final static Option SLIM_OPT = new Option("slim");
  private final static Option TIME_OPT = new Option("time");
  private final static Option PACK_OPT = new Option("pack");
  
  
  private final static Option NO_ROWNUM_OPT = new Option("no-rownum");

}













