/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.cli.rt;


import static io.crums.util.Strings.nOf;

import java.awt.Color;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import com.lowagie.text.Font;
import com.lowagie.text.Image;

import io.crums.io.FileUtils;
import io.crums.sldg.reports.pdf.Align;
import io.crums.sldg.reports.pdf.Align.H;
import io.crums.sldg.reports.pdf.BorderContent;
import io.crums.sldg.reports.pdf.CellData;
import io.crums.sldg.reports.pdf.CellData.TextCell;
import io.crums.sldg.reports.pdf.CellFormat;
import io.crums.sldg.reports.pdf.ColumnTemplate;
import io.crums.sldg.reports.pdf.FontSpec;
import io.crums.sldg.reports.pdf.Header;
import io.crums.sldg.reports.pdf.LineSpec;
import io.crums.sldg.reports.pdf.ReportTemplate;
import io.crums.sldg.reports.pdf.ReportTemplate.Components;
import io.crums.sldg.reports.pdf.TableTemplate;
import io.crums.sldg.reports.pdf.json.AutoRefContext;
import io.crums.sldg.reports.pdf.json.EditableRefContext;
import io.crums.sldg.reports.pdf.json.RefContext;
import io.crums.sldg.reports.pdf.json.RefContextParser;
import io.crums.sldg.reports.pdf.json.ReportTemplateParser;
import io.crums.util.Lists;
import io.crums.util.Strings;
import io.crums.util.TaskStack;
import io.crums.util.json.JsonPrinter;
import io.crums.util.main.StdExit;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * 
 */
@Command(
    name = "report-template",
    mixinStandardHelpOptions = true,
    version = {
        "report-template 0.5",
    },
    description = {
        "Utilities for creating ledger report templates.%n",
    },
    subcommands = {
        HelpCommand.class,
        Create.class,
        Pdf.class,
        Dedup.class,
        PinCell.class,
        
    })
public class ReportUtils {

  
  public static void main(String[] args) {
    int exitCode = new CommandLine(new ReportUtils()).execute(args);
    System.exit(exitCode);
  }
  
  /** Images subdirectory name. TODO: centralize. Other modules know about this location. */
  public final static String IMAGES = "images";
  


  
  
  public Map<String, ByteBuffer> makeImageMap(File imageDir) {
    if (imageDir == null || !imageDir.isDirectory())
      return Map.of();
    var imageMap = new TreeMap<String, ByteBuffer>();
    for (var f : imageDir.listFiles()) {
      if (f.isDirectory())
        continue;
      var buffer = FileUtils.loadFileToMemory(f);
      imageMap.put(f.getName(), buffer);
    }
    return imageMap;
  }
  
  @Spec
  private CommandSpec spec;
  
}


//             --- Helpers and mixins ---

/**
 * Mixin JSON file parameter.
 */
class JsonFile {

  @Parameters(
      paramLabel = "FILE",
      arity = "1",
      description = "Report template file (JSON)")
  File source;
  
  
  
  public File requireExists(CommandSpec spec) {
    if (!source.isFile())
      throw new ParameterException(spec.commandLine(), "file not found: " + source);
    return source;
  }
  
}




/**
 * Mixin --images option. Since mix-in members are always instantiated by picocli,
 * we tack in a few related member functions.
 */
class ImagesOption {

  @Option(
      names = {"-g", "--images"},
      paramLabel = "DIR",
      description = {
          "Directory referencerd images referenced are found",
          "(Images are referenced by simple filename.)",
          "Default: @|underline ./" + ReportUtils.IMAGES + "|@  (relative to JSON file)"
      })
  File images;
  
  
  
  public boolean isSet() {
    return images != null;
  }
  
  
  /** Returns the set {@linkplain #images} directory. */
  public File requireExists(CommandSpec spec) {
    if (!images.isDirectory())
      throw new ParameterException(spec.commandLine(), "directory not found: " + images);
    return images;
  }
  
  
  
  /**
   * Returns the images directory set on the command line; if not set, then
   * the default location (relative to the JSON file) is returned.
   * 
   * @param json  if not set, then the images directory is assumed to be a sibling of
   *              this file
   */
  public File getImagesDir(CommandSpec spec, File json) {
    return isSet() ?
        requireExists(spec) :
          new File(json.getParentFile(), ReportUtils.IMAGES);
  }
  
  
  /**
   * Returns the files in the given directory as a loaded image-map.
   * Make sure there are only images in this directory.
   * 
   * @param imageDir  existing images directory
   */
  public Map<String, ByteBuffer> makeImageMap(File imageDir) {
    if (!imageDir.isDirectory())
      throw new IllegalArgumentException("not a dir: " + imageDir);
    var imageMap = new TreeMap<String, ByteBuffer>();
    for (var f : imageDir.listFiles()) {
      if (f.isDirectory())
        continue;
      var buffer = FileUtils.loadFileToMemory(f);
      imageMap.put(f.getName(), buffer);
    }
    return imageMap;
  }
  
  /**
   * Returns the files in the resolved directory as a loaded image-map.
   * Make sure there are only images in this directory.
   * 
   * @param json  if not set, then the images directory is assumed to be a sibling of
   *              this file
   */
  public Map<String, ByteBuffer> makeImageMap(CommandSpec spec, File json) {
    File imageDir = getImagesDir(spec, json);
    if (!imageDir.exists())
      return Map.of();
    if (imageDir.isFile())
      throw new ParameterException(spec.commandLine(),
          "expected images directory is actually a file: " + imageDir);
    return makeImageMap(imageDir);
  }
  
}







//+   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +  //



@Command(
    name = "create",
    description = {
        "Creates a new report template.",
    }
    )
class Create implements Runnable {
  
  
  
  private final static float[] DEF_HEADER_COL_WIDTHS = { 25f, 50f, 25f };
  private final static float[] DEF_MAIN_COL_WIDTHS = { 15f, 45f, 20f, 20f };
  
  public final static String LOGO_ = "logo.";
  public final static String LOGO_PNG = LOGO_ + "png";

  @Spec
  private CommandSpec spec;
  
  
  private float[] headerWidths;
  
  @Option(
      names = {"--header-cw"},
      paramLabel = "PCNT",
      required = false,
      arity = "1..*",
      description = {
          "Sets the header table column widths as percentages.",
          "Widths are normalized (needn't sum to 1 or 100)",
          "Default: 25 50 25              (3-column table)",
      })
  public void setHeaderWidths(float[] widths) {
    this.headerWidths = normalizeWidths(widths);
  }
  
  
  private float[] mainWidths;
  
  @Option(
      names = {"--main-cw"},
      paramLabel = "PCNT",
      required = false,
      arity = "1..*",
      description = {
          "Sets the main table column widths as percentages.",
          "Widths are normalized (needn't sum to 1 or 100)",
          "Default: 15 45 20 20           (4-column table)",
      })
  public void setMainWidths(float[] widths) {
    this.mainWidths = normalizeWidths(widths);
  }
  
  
  private File target;
  
  @Parameters(
      paramLabel = "FILE",
      description = {
          "Path to @|italic new file|@. If a directory, then a new file",
          "named @|faint " + DEF_REPORT_FILENAME + "|@ is created there.",
      })
  public void setTarget(File file) {
    
    if (file.exists()) {
      
      if (file.isDirectory())
        this.target = new File(file, DEF_REPORT_FILENAME);
      else
        throw new ParameterException(spec.commandLine(), "file already exists: " + file);
      if (target.exists())
        throw new ParameterException(spec.commandLine(), "file already exists: " + target);
    
    } else {
      
      File dir = file.getParentFile();
      if (dir != null && !dir.isDirectory())
        throw new ParameterException(spec.commandLine(), "parent directory does not exist: " + dir);
      this.target = file;
    }
  }
  private final static String DEF_REPORT_FILENAME = "report.json";
  
  
  @Mixin
  private ImagesOption imagesOpt;
  
  

  @Override
  public void run() {
    
    
    CellData iconCell;
    {
      File images;  // images directory
      
      if (imagesOpt.isSet())
        images = imagesOpt.requireExists(spec);
      else {
        File parentDir = this.target.getParentFile();
        images = new File(parentDir, ReportUtils.IMAGES);
        if (!images.exists()) {
          if (!images.mkdir())
            throw new ParameterException(spec.commandLine(),
                "failed to create " + ReportUtils.IMAGES + " directory: " + images);
        
        } else if (images.isFile())
          throw new ParameterException(spec.commandLine(),
              "images directory is actually a file: " + images);
      }
      
      File logo = prepareLogo(images);
      
      Image icon;
      try {
        icon = Image.getInstance(logo.getPath());
      } catch (IOException iox) {
        throw new UncheckedIOException(
            // TODO: centralize / provide standard message for reporting bugs
            "I/O error. Please report this bug: " + iox.getMessage(), iox);
      }
      iconCell = CellData.forImage(logo.getName(), icon, 50, 50);
      
    }
    
    
    var references = new EditableRefContext();
    
    CellFormat cellFormat;
    {
      references.colorRefs().put("black", Color.BLACK);
      FontSpec font = new FontSpec("Helvetica", 10, Font.NORMAL, Color.BLACK);
      references.fontRefs().put("mainFont", font);
      cellFormat = new CellFormat(font);
      cellFormat.setPadding(4);
      cellFormat.setLeading(8);
      references.cellFormatRefs().put("mainFormat", cellFormat);
    }
    
    
    Header header;
    {
      if (headerWidths == null)
        headerWidths = normalizeWidths(DEF_HEADER_COL_WIDTHS);
      var headerTable = new TableTemplate(
          Lists.repeatedList(new ColumnTemplate(cellFormat, null), headerWidths.length));
      headerTable.setColumnWidths(Lists.floatList(headerWidths));
      headerTable.setFixedCell(0,  1, iconCell);
      headerTable.setFixedCell(0, 2, CellData.TextCell.BLANK);
      header = new Header(headerTable);
      references.cellDataRefs().put("blank", CellData.TextCell.BLANK);
    }
    
    TableTemplate mainTable;
    {
      if (mainWidths == null)
        mainWidths = normalizeWidths(DEF_MAIN_COL_WIDTHS);
      mainTable = new TableTemplate(
          Lists.repeatedList(new ColumnTemplate(cellFormat, null), mainWidths.length));

      
      // TODO: make color configurable
      var mainBorderColor = new Color(100, 183, 222);
      references.colorRefs().put("mainBorderColor", mainBorderColor);
      
      
      mainTable.setTableBorders(new LineSpec(3, mainBorderColor));
      
      
      // headings.. make configurable
      var headingStyle = new CellFormat(new FontSpec("Helvetica", 10, Font.BOLD, Color.WHITE));
      headingStyle.setBackgroundColor(mainBorderColor);
      headingStyle.setAlignH(H.CENTER);
      headingStyle.setPadding(8);
      references.cellFormatRefs().put("mainHeadingFormat", headingStyle);
      
      for (int index = 0; index < mainWidths.length; ++index)
        mainTable.setFixedCell(index, new TextCell("H_" + index, headingStyle));
    }
    
    var footer = new BorderContent(
        "© Example Footer Corp",
        new FontSpec("Helvetica", 8, Font.NORMAL, Color.GRAY), Align.H.RIGHT,
        false);
    
    
    var report = new ReportTemplate(new Components(header, null, mainTable, footer), null);
    
    var jObj = new ReportTemplateParser().setReferences(references).toJsonObject(report);
    JsonPrinter.write(jObj, target);
    System.out.printf("%d bytes written to %s%n", target.length(), target.getPath());
  }
  

  
  
  
  
  private File prepareLogo(File dir) {
    File logo = findLogo(dir);
    if (logo != null)
      return logo;
    
    // create a .png logo file
    logo = new File(dir, LOGO_PNG);
    try (TaskStack closer = new TaskStack()) {
      var out = new FileOutputStream(logo);
      closer.pushClose(out);
      var icon = getClass().getResourceAsStream("example_icon.png");
      closer.pushClose(icon);
      byte[] buffer = new byte[4096];
      while (true) {
        int bytes = icon.read(buffer);
        if (bytes == -1)
          break;
        out.write(buffer, 0, bytes);
      }
    } catch (IOException iox) {
      throw new ParameterException(spec.commandLine(),
          "failed to create image file " + logo + System.lineSeparator() +
          "Detail:" + iox.getMessage());
    }
    
    return logo;
  }
  
  private File findLogo(File dir) {
    File[] candidates = dir.listFiles(
        f -> { return f.isFile() && f.getName().startsWith(LOGO_); }
        );
    if (candidates == null || candidates.length == 0)
      return null;
    
    return candidates[0];
  }
  
  private float[] normalizeWidths(float[] widths) {
//    System.out.println(Lists.floatList(widths));
    // (never zero length, so we don't check)
    // normalize
    float sum = 0;
    for (int index = widths.length; index-- > 0; ) {
      float w = widths[index];
      if (w <= 0)
        throw new ParameterException(spec.commandLine(),"illegal header width: " + w);
      sum += w;
    }
    float[] norm = new float[widths.length];
    for (int index = widths.length; index-- > 0;)
      norm[index] = (float) (widths[index] / sum);
    return norm;
  }
  
}





//+   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +  //


@Command(
  name = "dedup",
  description = {
      "Deduplicates common JSON elements in the given report template file.",
      "Dedup'ed elements are centralized in the @|fg(cyan) " + ReportTemplateParser.OBJ_REFS + "|@ dictionary and are instead",
      "referenced wherever else they occur in the JSON."
  }
  )
class Dedup implements Runnable {

@Spec
private CommandSpec spec;

@Mixin
private JsonFile jsonFile;

@Mixin
private ImagesOption imagesOpt;




@Option(
    names = {"-t", "--target"},
    paramLabel = "FILE",
    description = "Target file. Otherwise, overwrite source"
    )
private File target;



private File getTarget() {
  var source = jsonFile.requireExists(spec);
  if (target == null)
    return source;
  
  if (target.exists() && !target.equals(source))
    throw new ParameterException(spec.commandLine(), "target file already exists: " + target);
  
  var dir = target.getParentFile();
  if (dir != null && !dir.exists())
    throw new ParameterException(spec.commandLine(), "target file parent directory does not exist: " + dir);
  return target;
}


@Override
public void run() {
  var targetCommit = new FileCommit(getTarget());
  try {
    var out = System.out; // for messages only
    
    var source = jsonFile.requireExists(spec);
    var parser = new ReportTemplateParser().setRefedImages(
        imagesOpt.makeImageMap(spec, source));
    
    // create the template instance 
    var reportTemplate = parser.toEntity(source);
    
    var originalRefs = parser.getReferences();

    // copy the gatherered references on the first read-pass into an auto context..
    var autoRefs = new AutoRefContext(originalRefs);
    
    // write out the JSON in a dry run, auto-populating the objRef dictionary
    // along the way -- a side effect, functional anti-pattern
    parser.setReferences(autoRefs).toJsonObject(reportTemplate);
    
    int dedupCount = autoRefs.countRefsCreated();

    // 
    if (dedupCount == 0) {
      out.println("No duplicate elements found.");
      out.printf("%s defined in the file's %s dictionary%n",
          nOf(originalRefs.sansImageSize(), "JSON element"),
          OBJ_REFS);
      return;
    }
    
    // done with discovery. Replace auto context (copy it).
    // this is just for good measure (currently works w/o copying)
    parser.setReferences(new EditableRefContext(autoRefs));
    
    // final product: regurgitated json
    var jReport = parser.toJsonObject(reportTemplate);
    
    targetCommit.moveToBackup();
    
    JsonPrinter.write(jReport, targetCommit.getFile());
    
    out.printf("%s deduplicated and added to %s:%n%n",
        nOf(autoRefs.countRefsCreated(), "JSON element"),
        OBJ_REFS);
    
    printDiscovered(autoRefs.colorRefsCreated());
    printDiscovered(autoRefs.fontRefsCreated());
    printDiscovered(autoRefs.formatRefsCreated());
    printDiscovered(autoRefs.cellRefsCreated());
    
    out.println();
    
  } catch (Exception jpx) {
    // TODO: add switches that control this section
    boolean restored = targetCommit.rollback();
    System.err.println("dedup failed: " + jpx.getMessage());
    if (restored)
      System.err.println("target file restored to original version: " + targetCommit.getFile());
    StdExit.GENERAL_ERROR.exit();
  }
}


private void printDiscovered(Map<String, ?> added) {
  for (var refName : added.keySet())
    System.out.println("  " + refName);
}

private final static String OBJ_REFS = ReportTemplateParser.OBJ_REFS + ":{..}";

}



// +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +  //


@Command(
    name = "pdf",
    description = {
        "Generates PDF from the JSON template.",
    }
    )
class Pdf implements Runnable {

  @Spec
  private CommandSpec spec;

  @Mixin
  private JsonFile jsonFile;
  
  @Mixin
  private ImagesOption imagesOpt;

  @Override
  public void run() {
    File source = jsonFile.requireExists(spec);
    
    var template = new ReportTemplateParser()
        .setRefedImages(imagesOpt.makeImageMap(spec, source))
        .toEntity(source);
    
    String filename;
    {
      String name = source.getName();
      if (name.toLowerCase().endsWith(".json"))
        name = name.substring(0, name.length() - 5);
      filename = name + ".pdf";
    }
    var fileCommit = new FileCommit(new File(source.getParentFile(), filename));
    fileCommit.moveToBackup();
    try {
      
      template.writePdf(fileCommit.getFile());
    
    } catch (Exception x) {
      fileCommit.rollback();
      throw new ParameterException(spec.commandLine(),
          "error on generating PDF from " + source + System.lineSeparator() +
          "Detail: " + x.getMessage());
    }
  }
  
}


//+   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +  //







@Command(
    name = "pin",
    description = {
        "Pins (fixes) content at the given table cell coordinates (column/row).",
        "",
    }
    )
class PinCell implements Runnable {
  
  static class RefOrText {
    
    @Option(names = "--cell-text", description = "Cell text value", required = true)
    String text;

    @Option(
        names = "--cell-ref",
        paramLabel = "cellREF",
        description = {
            "Reference to cell value. Must already exist in the",
            "JSON's @|fg(cyan) " + ReportTemplateParser.OBJ_REFS + "." + RefContextParser.CELLS + "{..}|@ dictionary."
        }, required = true)
    String cellRef;
    
    
    boolean isText() {
      return text != null;
    }
    
    
    CellData getRefed(CommandSpec spec, RefContext refs) {
      var refed = refs.cellDataRefs().get(cellRef);
      if (refed == null)
        throw new ParameterException(spec.commandLine(),
            "no such named cell found in " +
            ReportTemplateParser.OBJ_REFS + "." + RefContextParser.CELLS + " dictionary");
      return refed;
    }
    
  }
  


  /** Table setting option. Mutual-exclusive use for now. */
  static class TableOpts {
    
    @Option(names = "--main", description = "Main table")
    boolean main;
  
    @Option(names = "--header", description = "Header table")
    boolean header;
    
    @Option(names = "--subheader", description = "Sub-header table")
    boolean subHeader;
    
    
    TableTemplate getTable(CommandSpec spec, ReportTemplate report) {
      
      if (main)
        return report.getComponents().mainTable();
      if (header)
        return report.getComponents().header().headerTable();
      if (!subHeader)
        throw new IllegalStateException("not initialized");
      
      var subH = report.getComponents().subHeader();
      if (subH.isEmpty())
        throw new ParameterException(spec.commandLine(),
            "template JSON must contain the (optional) " + ReportTemplateParser.SUBHEAD_TABLE +
            " element for this option to work");
      
      return subH.get();
    }
    
    
    String getTableName() {
      return main ? "main" : (header ? "header" : "subheader");
    }
  }
  
  
  @ArgGroup(exclusive = true, multiplicity = "1")
  private TableOpts tableOpt;
  
  
  @Option(
      names = {"--cell-cr"},
      paramLabel = "IDX",
      required = true,
      arity = "2",
      description = {
          "Cell coordinates. 2 numbers: column row",
          "A @|italic negative|@ row index means @|italic after|@ the last dynamic",
          "row. Examples:",
          " --cell-cr  0  0    (top left corner)",
          " --cell-cr  1 -2    (2nd column, 2nd row after last",
          "                     dynamic row)"
      }
      )
  private int[] coordinates;
  
  


  @ArgGroup(exclusive = true, multiplicity = "1")
  private RefOrText refOrText;

  
  @Option(
      names = "--cell-text-format",
      paramLabel = "fmtREF",
      arity = "0..1",
      description = {
          "Cell text format reference (font, color, etc.)",
          "When set, override the default format the cell",
          "inherits from the table's column. The reference @|bold must|@",
          "already exist in the JSON's @|fg(cyan) " + ReportTemplateParser.OBJ_REFS + "." + RefContextParser.CELL_FORMATS + "{..}|@",
          "dictionary."
      }
      )
  String formatRef;
  
  
  private CellFormat getCellFormat(RefContext refs) {
    if (formatRef == null)
      return null;
    
    var format = refs.cellFormatRefs().get(formatRef);
    if (format == null)
      throw new ParameterException(spec.commandLine(),
          "no cell format named '" + formatRef + "' found in " +
          ReportTemplateParser.OBJ_REFS + "." + RefContextParser.CELL_FORMATS + " dictionary");
    
    return format;
  }
  
  
  private int[] coordinates() {
    if (coordinates.length != 2)
      throw new ParameterException(spec.commandLine(),
          "need exactly 2 coordinates (column, row)");
    if (coordinates[0] < 0)
      throw new ParameterException(spec.commandLine(),
          "negative column index: " + coordinates[0]);
    return coordinates;
  }
  
  
  
  @Spec
  private CommandSpec spec;
  
  @Mixin
  private JsonFile jsonFile;
  
  @Mixin
  private ImagesOption imagesOpt;
  
  
  
  

  @Override
  public void run() {
    File source = jsonFile.requireExists(spec);
    var parser = new ReportTemplateParser()
        .setRefedImages(imagesOpt.makeImageMap(spec, source));
    var report = parser.toEntity(source);
    
    var references = parser.getReferences();
    
    TableTemplate table = tableOpt.getTable(spec, report);
    
    CellData cell = refOrText.isText() ?
        new CellData.TextCell(refOrText.text, getCellFormat(references)) :
          refOrText.getRefed(spec, references);
    
    int col, row;
    {
      int[] cr = coordinates();
      col = cr[0];
      row = cr[1];
      if (col >= table.getColumnCount())
        throw new ParameterException(spec.commandLine(),
            "column index [" + col + "] out of bounds; table has " +
            Strings.nOf(table.getColumnCount(), "column"));
    }
    
    var prev = table.getFixedCells().get(table.toSerialIndex(col, row));
    if (cell.equals(prev)) {
      System.out.printf(
          "cell [%d,%d] in %s table is already set to same value, nothing done.%n",
          row, col, tableOpt.getTableName());
      return;
    }
    table.setFixedCell(col, row, cell);
    
    
    FileCommit fileCommit = new FileCommit(source);
    fileCommit.moveToBackup();
    try {
      
      var jReport = parser.toJsonObject(report);
      
      JsonPrinter.write(jReport, source);
      System.out.printf(
          "%s cell [%d,%d] in %s table%n",
          prev == null ? "created" : "updated",
          row, col, tableOpt.getTableName());
      
    } catch (Exception x) {
      boolean recovered = fileCommit.rollback();
      String msg = "failed to write to " + source;
      if (recovered)
        msg += " (backup restored)";
      msg += System.lineSeparator() + "Detail: " + x.getMessage();
      throw new RuntimeException(msg, x);
    }
  }
  
}













