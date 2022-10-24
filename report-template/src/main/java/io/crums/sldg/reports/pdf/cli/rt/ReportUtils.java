/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.cli.rt;


import static io.crums.util.Strings.nOf;

import java.awt.Color;
import java.awt.Desktop;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import com.lowagie.text.Font;
import com.lowagie.text.Image;
import com.lowagie.text.StandardFonts;

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
import io.crums.sldg.reports.pdf.ReportAssets;
import io.crums.sldg.reports.pdf.ReportTemplate;
import io.crums.sldg.reports.pdf.ReportTemplate.Components;
import io.crums.sldg.reports.pdf.TableTemplate;
import io.crums.sldg.reports.pdf.json.AutoRefContext;
import io.crums.sldg.reports.pdf.json.CellFormatParser;
import io.crums.sldg.reports.pdf.json.EditableRefContext;
import io.crums.sldg.reports.pdf.json.RefContext;
import io.crums.sldg.reports.pdf.json.RefContextParser;
import io.crums.sldg.reports.pdf.json.ReportTemplateParser;
import io.crums.util.Lists;
import io.crums.util.TaskStack;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonPrinter;
import io.crums.util.json.simple.JSONObject;
import io.crums.util.main.StdExit;
import io.crums.util.main.TablePrint;
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
        // TODO: copyright & license strings
    },
    description = {
        "Utilities for creating ledger report templates.%n",
        "Updates:",
        "Whenever this program updates a file, it stores the original version with a",
        "'@|faint ~|@' appended to its filename. See also @|bold rollback|@ command below.%n"
    },
    subcommands = {
        HelpCommand.class,
        Create.class,
        Pdf.class,
        Dedup.class,
        RefNames.class,
        RenameRef.class,
        Dsl.class,
        NewColorRef.class,
        NewFontRef.class,
        NewFormatRef.class,
        Pin.class,
        Unpin.class,
        Rollback.class,
    })
public class ReportUtils {

  
  public static void main(String[] args) {
    int exitCode = new CommandLine(new ReportUtils()).execute(args);
    System.exit(exitCode);
  }
  
  /** Images subdirectory name. */
  public final static String IMAGES = ReportAssets.IMAGES_SUBDIR_NAME;
  


  
  
  
  
  @Spec
  private CommandSpec spec;
  

  
  final static String OBJ_REFS = ReportTemplateParser.OBJ_REFS + ":{..}";
  final static String OBJ_REFS_CY = "@|fg(cyan) " + OBJ_REFS + "|@";
  
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
  
  
  public void write(JSONObject jObj) {
    var fileCommit = new FileCommit(source);
    fileCommit.moveToBackup();
    try {
      
      JsonPrinter.write(jObj, source);
      
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




/**
 * Mixin --images option. Since mix-in members are always instantiated by picocli,
 * we tack in a few related member functions.
 */
class ImagesOption {

  @Option(
      names = {"-g", "--images"},
      paramLabel = "DIR",
      description = {
          "Directory referenced images are found",
          "(Images are keyed by simple filename, w/o ext.)",
          "Default: @|underline " + ReportUtils.IMAGES + "|@  (sibling of JSON @|fg(yellow) FILE|@)"
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
      imageMap.put(ReportAssets.imageRef(f), buffer);
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


class OpenOpt {
  
  
  @Option(names = {"-o", "--open"}, description = "Open resultant file using system associated app")
  boolean open;
  
  
  boolean open(File file) {
    if (open) try {
      Desktop.getDesktop().open(file);
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on launching system associated app for " + file + System.lineSeparator() +
          "Detail: " + iox, iox);
    }
    return open;
  }
}



// Command class marker, going forward..

//+   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +  //

@Command(
    name = "create",
    description = {
        "Create a new report template.",
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
        // FIXME
        // acting as if it's necessarily a bug
        // (it's not: if the user input a crap image, it would throw here)
        throw new UncheckedIOException(
            "On loading image from " + logo + " : " + iox.getMessage(), iox);
      }
      iconCell = CellData.forImage(ReportAssets.imageRef(logo), icon, 50, 50);
      
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
      "Deduplicate common JSON elements using automatic references.",
      "Dedup'ed elements are defined in the " + ReportUtils.OBJ_REFS_CY + " dictionary and are",
      "referenced thereafter."
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
            ReportUtils.OBJ_REFS);
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
          ReportUtils.OBJ_REFS);
      
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
  

}



// +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +  //

@Command(
    name = "pdf",
    description = {
        "Generate PDF from the JSON template.",
    }
    )
class Pdf implements Runnable {

  @Spec
  private CommandSpec spec;

  @Mixin
  private JsonFile jsonFile;
  
  @Mixin
  private ImagesOption imagesOpt;
  
  @Mixin
  private OpenOpt openOpt;

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

    System.out.printf("%d bytes written to %s%n",
        fileCommit.getFile().length(),
        fileCommit.getFile().getPath());
    
    openOpt.open(fileCommit.getFile());
  }
  
}


// - - - - - More picocli Mixins for table cells


class CellPosition {
  
  
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
  private boolean checked;
  
  private int[] coordinates(CommandSpec spec) {
    if (!checked) {
      if (coordinates.length != 2)
        throw new ParameterException(spec.commandLine(),
            "need exactly 2 coordinates (column, row)");
      if (coordinates[0] < 0)
        throw new ParameterException(spec.commandLine(),
            "negative column index: " + coordinates[0]);
      checked = true;
    }
    return coordinates;
  }
  
  int col(CommandSpec spec) {
    return coordinates(spec)[0];
  }
  
  int row(CommandSpec spec) {
    return coordinates(spec)[1];
  }
}



class RefOrText {
  
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


/** Table templates defined in the report template model. */
enum ReportTable {
  HEADER("header"),
  SUB_HEADER("subheader"),
  MAIN("main");
  
  public final String table;
  
  private ReportTable(String table) { this.table = table; }
}


/** Table setting option. Mutual-exclusive use for now. */
class TableOpts {
  
  @Option(names = "--main", description = "Main table")
  boolean main;

  @Option(names = "--header", description = "Header table")
  boolean header;
  
  @Option(names = "--subheader", description = "Sub-header table")
  boolean subHeader;
  
  
  ReportTable asEnum() {
    if (main)
      return ReportTable.MAIN;
    if (header)
      return ReportTable.HEADER;
    if (subHeader)
      return ReportTable.SUB_HEADER;
    
    throw new RuntimeException("instance not initialized");
  }
  
  
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
    return asEnum().table;
  }
}





//+   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +  //

@Command(
    name = "pin",
    description = {
        "Pin content at table cell coordinates (column/row)",
        "",
    }
    )
class Pin implements Runnable {
  
  
  


  
  
  
  
  @ArgGroup(exclusive = true, multiplicity = "1")
  private TableOpts tableOpt;
  
  
  @Mixin
  private CellPosition cellPos;


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
    
    int col = cellPos.col(spec);
    int row = cellPos.row(spec);
    
    var prev = table.getFixedCells().get(table.toSerialIndex(col, row));
    if (cell.equals(prev)) {
      System.out.printf(
          "cell (%d,%d) in %s table is already set to same value, nothing done.%n",
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






//+   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +  //

@Command(
    name = "unpin",
    description = {
        "Unpin content at table cell coordinates (column/row)",
        "(Undoes the @|bold pin|@ command.)",
    }
    )
class Unpin implements Runnable {
  
  
  @Spec
  private CommandSpec spec;
  
  @Mixin
  private JsonFile jsonFile;
  
  @Mixin
  private ImagesOption imagesOpt;

  
  @ArgGroup(exclusive = true, multiplicity = "1")
  private TableOpts tableOpt;
  
  
  @Mixin
  private CellPosition cellPos;
  
  
  @Override
  public void run() {
    int col = cellPos.col(spec);
    int row = cellPos.row(spec);
    File source = jsonFile.requireExists(spec);
    var parser = new ReportTemplateParser()
        .setRefedImages(imagesOpt.makeImageMap(spec, source));
    var report = parser.toEntity(source);
    
    TableTemplate table = tableOpt.getTable(spec, report);
    
    // modify
    CellData cellData;
    try {
      cellData = table.removeFixedCell(col, row);
    
    } catch (IndexOutOfBoundsException iobx) {
      throw new ParameterException(spec.commandLine(),
          String.format(
              "illegal cell coordinates (%d,%d): %s",
              col, row, iobx.getMessage()));
    }
    
    var out = System.out;
    if (cellData == null) {
      out.printf("no cell pinned at (%d,%d) in % table, nothing done.%n",
          col, row, tableOpt.getTableName());
      return;
    }

    // reserialize the report
    var jObj = parser.toJsonObject(report);

    FileCommit fileCommit = new FileCommit(source);
    fileCommit.moveToBackup();
    try {
      JsonPrinter.write(jObj, source);
    } catch (RuntimeException x) {
      boolean recovered = fileCommit.rollback();
      System.err.printf("[ERROR] on writing to %s%nRecovered: %s%nError msg: %s%n",
          source.getName(),
          recovered ? "yes" : "no",
          x.getMessage());
      throw x; 
    }
    
    
    out.printf("removed pinned cell at (%d,%d) in %s table%n",
        col, row, tableOpt.getTableName());
    parser.getReferences().findRef(cellData).ifPresent(
        ref -> out.printf("referenced as: %s%n", ref));
  }
}






//+   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +  //

@Command(
    name = "rollback",
    description = {
        "Roll back the given file to its previous version.",
        "(Renames backup filename, removing leading '@|faint ~|@')",
        "",
    }
    )
class Rollback implements Runnable {

  
  @Spec
  private CommandSpec spec;
  
  
  @Parameters(
      arity = "1",
      paramLabel = "FILE",
      description = "file pathname to restore (not the backup file)"
      )
  private File file;

  
  @Override
  public void run() {
    var out = System.out;
    FileCommit fileCommit = new FileCommit(file);
    if (fileCommit.rollback()) {
      out.printf("%s %s%n",
          fileCommit.initExists() ? "Rolled back" : "Recovered",
          file.getName());
    } else if (!fileCommit.getBackupFile().exists()) {
      out.printf("No backup file (%s) found. Nothing updated.%n",
          fileCommit.getBackupFile().getName());
    } else {
      throw new ParameterException(spec.commandLine(),
          "failed to rollback using " + fileCommit.getBackupFile().getName() + System.lineSeparator() +
          "(a file permissions issue?)");
    }
      
  }
  
  
}







//+   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +  //

@Command(
    name = "dsl",
    description = {
        "List values JSON element in DSL can take and exit.",
    }
    )
class Dsl implements Runnable {
  
  
  static class Type {
    
    @Option(names = "--fonts", description = "Standard font (family) names")
    boolean stdFonts;

    @Option(names = "--cell-aligns", description = "Cell format alignments")
    boolean cellAligns;
    

    @Option(names = "--objRefs", description = ReportUtils.OBJ_REFS_CY + " subdictionary names")
    boolean objRefs;
    
    
  }
  

  @ArgGroup(exclusive = true, multiplicity = "1")
  private Type type;
  

  @Override
  public void run() {
    if (type.stdFonts)
      stdFonts();
    if (type.cellAligns)
      cellAligns();
    if (type.objRefs)
      objRefs();
    
    System.out.println();
  }
  
  
  
  
  
  private void objRefs() {
    String[] subs = {
        RefContextParser.COLORS,
        RefContextParser.FONTS,
        RefContextParser.CELL_FORMATS,
        RefContextParser.CELLS,
    };
    var out = System.out;
    out.printf(
        "%n%d presentation-related " + ReportUtils.OBJ_REFS + " sub dictionaries.%n" +
        "Values defined in successive dictionaries may reference values defined in%n" +
        "preceding dictionaries:%n%n",
        subs.length);
    
    for (var sub : subs)
      out.println("  " + sub);
    
  }


  private void cellAligns() {
    printAlign(CellFormatParser.ALIGN_H, Align.H.values());
    printAlign(CellFormatParser.ALIGN_V, Align.V.values());
  }
  
  private <T extends Enum<T> & Align.Defaulted> void printAlign(String tag, Enum<T>[] values) {
    var table = new TablePrint(30, 16);
    table.setIndentation(2);
    System.out.println(tag + ":");
    for (var a : values) {
      table.printRow(
          a.name().toLowerCase(),
          ((Align.Defaulted) a).isDefault() ? "(default)" : null);
    }
  }

  

  
  private final static String BOLD = "_BOLD";
  private final static String ITALIC = "_ITALIC";
  private final static String BOLDITALIC = "_BOLDITALIC";
  private void stdFonts() {
    for (var font : StandardFonts.values()) {
      var name = font.name();
      // the implementation doesn't distinguish these types..
      // use the style element instead
      if (name.endsWith(ITALIC) || name.endsWith(BOLD) || name.endsWith(BOLDITALIC))
        continue;
      System.out.println(font.name());
    }
  }
  
}


//   - - - Helpers and mixins - - -


enum RefType {
  COLORS,
  FONTS,
  FORMATS,
  CELLS;
}

/** Ref sub dictionary options. */
class RefOpts {

  
  @Option(names = "--colors", description = "Color sub dictionary")
  boolean colors;
  
  @Option(names = "--fonts", description = "Font sub dictionary")
  boolean fonts;
  
  @Option(names = "--formats", description = "Cell-format sub dictionary")
  boolean formats;
  
  @Option(names = "--cells", description = "Cell sub dictionary")
  boolean cells;
  
  
  RefType asEnum() {
    if (colors)
      return RefType.COLORS;
    if (fonts)
      return RefType.FONTS;
    if (formats)
      return RefType.FORMATS;
    if (cells)
      return RefType.CELLS;
    
    throw new RuntimeException("instance not initialized");
  }
  
  
  
  String jsonTag() {
    return switch (asEnum()) {
    case COLORS -> RefContextParser.COLORS;
    case FONTS -> RefContextParser.FONTS;
    case FORMATS -> RefContextParser.CELL_FORMATS;
    case CELLS -> RefContextParser.CELLS;
    };
  }
  
  
  
  
}



//+   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +  //


@Command(
    name = "refnames",
    description = {
        "List reference names defined in sub dictionary and exit.",
    }
    )
class RefNames implements Runnable {

  @Spec
  private CommandSpec spec;

  @ArgGroup(exclusive = true, multiplicity = "1")
  private RefOpts refOpts;
  
  @Mixin
  private JsonFile jsonFile;
  
  @Mixin
  private ImagesOption imagesOpt;
  
  static record Dictionary(String name, Set<String> keys) {
    Dictionary(String name, Map<String, ?> map) { this(name, map.keySet()); }
  }
  
  @Override
  public void run() {
    File source = jsonFile.requireExists(spec);
    var parser = new ReportTemplateParser()
        .setRefedImages(imagesOpt.makeImageMap(spec, source));
    
    // deserialize the source.. for its references actually
    parser.toEntity(source);
    
    var refs = parser.getReferences();
    
    Dictionary dict = switch (refOpts.asEnum()) {
    case COLORS -> new Dictionary(RefContextParser.COLORS, refs.colorRefs());
    case FONTS -> new Dictionary(RefContextParser.FONTS, refs.fontRefs());
    case FORMATS -> new Dictionary(RefContextParser.CELL_FORMATS, refs.cellFormatRefs());
    case CELLS -> new Dictionary(RefContextParser.CELLS, refs.cellDataRefs());
    };
    
    var out = System.out;
    dict.keys.forEach(k -> out.println("  " + k));
    out.printf("%s defined in sub dictionary '%s'%n",
        nOf(dict.keys.size(), "element"),
        dict.name);
  }
  
}





//+   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +  //


@Command(
  name = "rename",
  description = {
      "Rename object in " + ReportUtils.OBJ_REFS + " dictionary.",
      "References to the renamed object are updated using the new name."
  }
  )
class RenameRef implements Runnable {

  @Spec
  private CommandSpec spec;

  @ArgGroup(exclusive = true, multiplicity = "1")
  private RefOpts refOpts;
  
  @Mixin
  private JsonFile jsonFile;
  
  @Mixin
  private ImagesOption imagesOpt;
  
  
  @Option(names = "--ref", description = "reference name" )
  private String ref;
  
  @Option(names = "--ref-final", description = "final reference name")
  private String toRef;
  

  @Override
  public void run() {
    if (toRef.isEmpty())
      throw new ParameterException(spec.commandLine(), "empty --ref-final");
    
    File source = jsonFile.requireExists(spec);
    var parser = new ReportTemplateParser()
        .setRefedImages(imagesOpt.makeImageMap(spec, source));
    
    // deserialize the source..
    var report = parser.toEntity(source);
    
    var refs = new EditableRefContext(parser.getReferences());
    
    Map<String, ?> dict = switch (refOpts.asEnum()) {
    case COLORS -> refs.colorRefs();
    case FONTS  -> refs.fontRefs();
    case FORMATS  -> refs.cellFormatRefs();
    case CELLS    -> refs.cellDataRefs();
    };
    
    boolean replaced = replaceKey(dict, ref, toRef);
    
    var jObj = parser.setReferences(refs).toJsonObject(report);
    
    jsonFile.write(jObj);
    
    String msg = replaced ?
        "renamed reference '%s' in %s:{..} dictionary to '%s'%n" :
          "removed reference '%s' in %s:{..} dictionary%n" +
          "(reference '%s' with same value already exists)%n";
    
    System.out.printf(msg, this.ref, refOpts.jsonTag(), this.toRef);
  }
  
  
  private <T> boolean replaceKey(Map<String, T> dict, String oldKey, String newKey) {
    if (dict.containsKey(newKey))
      throw new ParameterException(spec.commandLine(),
          "reference '" + newKey + "' is already defined in '" +
          refOpts.jsonTag() + ":{..}' sub dictionary");
    T value = dict.remove(oldKey);
    if (value == null)
      throw new ParameterException(spec.commandLine(),
          "no reference named '" + oldKey + "' defined in '" +
          refOpts.jsonTag() + ":{..}' sub dictionary");
    
    T bootedValue = dict.put(newKey, value);
    if (bootedValue != null && !bootedValue.equals(value)) {
      throw new ParameterException(spec.commandLine(),
          "cannot overwrite existing reference '" + newKey +
          "' with the object referenced as '" + oldKey + "'");
    }
    return bootedValue == null;
  }
  
}


//- - - Helpers and mixins - - -

class Rgb {
  
  
  @Option(names = "--rgb", paramLabel = "0..255", arity = "3", required = true, description = "RGB color values")
  int[] rgb;
  
  private void check(CommandSpec spec) {
    
    for (int index = 0; index < 3; ++index) {
      int value = rgb[index];
      
      if (value < 0 || value > 255) {
        String colorMsg = switch (index) {
        case 0 -> "red";
        case 1 -> "green";
        case 2 -> "blue";
        default -> throw new RuntimeException();
        };
        throw new ParameterException(spec.commandLine(),
            String.format("out-of-bounds color (%s) value: %d", colorMsg, value));
      }
    }
  }
  
  
  Color color(CommandSpec spec) {
    check(spec);
    return new Color(rgb[0], rgb[1], rgb[2]);
  }
}


class RefNameOpt {
  
  @Option(names = "--ref", paramLabel = "NAME", required = true, description = "reference name")
  String name;
  
  String name(CommandSpec spec) {
    if (name.isEmpty())
      throw new ParameterException(spec.commandLine(), "empty reference name");
    return name;
  }
}


class ForceOpt {
  
  @Option(names = { "-f", "--force" }, required = false, description = "force overwrite")
  boolean force;
}




class NewRefBase<T> {
  
  /** @return false, if already set to this value (noop console msg already printed) */
  boolean put(String name, T value, Map<String, T> dict, Optional<String> existing, CommandSpec spec) {
    if (existing.isPresent()) {
      var existingName = existing.get();
      if (existingName.equals(name)) {
        System.out.printf("reference '%s' already defined as set, nothing to do.%n", name);
        return false;
      } else
        throw new ParameterException(spec.commandLine(),
            String.format("another reference ('%s') already has this value", existingName));
    }
    var prev = dict.put(name, value);
    if (prev != null)
      throw new ParameterException(spec.commandLine(),
          String.format(
              "reference '%s' already exists (with another value).%n" +
              "To re-define its value, edit the file directly.%n", name));
    return true;
  }
}



//+   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +  //


@Command(
name = "new-color",
description = {
    "Define a new color in the dictionary.",
}
)
class NewColorRef extends NewRefBase<Color> implements Runnable {

  @Spec
  private CommandSpec spec;
  
  @Mixin
  private JsonFile jsonFile;
  
  @Mixin
  private ImagesOption imagesOpt;
  
  @ArgGroup(exclusive = false, multiplicity = "1")
  private Rgb rgb;

  @Mixin
  private RefNameOpt ref;
  
  

  @Override
  public void run() {
    File source = jsonFile.requireExists(spec);
    
    var parser = new ReportTemplateParser()
        .setRefedImages(imagesOpt.makeImageMap(spec, source));
    
    // deserialize the source..
    var report = parser.toEntity(source);
    
    var color = rgb.color(spec);
    
    String refName = ref.name(spec);
    var ctx = new EditableRefContext(parser.getReferences());
    
    if (!put(refName, color, ctx.colorRefs(), ctx.findRef(color), spec))
      return; // (already exists)
    
    var jObj = parser.setReferences(ctx).toJsonObject(report);
    jsonFile.write(jObj);
    
    System.out.printf("ref '%s' added to color dictionary%n",
        refName);
  }
  
}








//+   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +  //


@Command(
name = "new-font",
description = {
  "Define a new font in the dictionary.",
}
)
class NewFontRef extends NewRefBase<FontSpec> implements Runnable {

  @Spec
  private CommandSpec spec;
  
  @Mixin
  private JsonFile jsonFile;
  
  @Mixin
  private ImagesOption imagesOpt;
  
  @Mixin
  private RefNameOpt ref;
  
  
  @Option(
      names = "--ff",
      description = {
          "Font family. For example, 'Times'",
          "Use @|bold dsl --fonts|@ to list supported fonts."
      },
      required = true)
  private String fontFamily;
  
  
  @Option(
      names = "--size",
      description = "Font size",
      required = true)
  private float size;
  
  
  @Option(
      names = "--color-ref",
      description = {
          "color already defined in dictionary",
          "Default: black"
      })
  private String colorRef;
  
  
  
  @Option(names = "--bold")
  private boolean bold;
  
  @Option(names = "--italic")
  private boolean italic;
  
  @Option(names = "--underline")
  private boolean underline;
  
  
  
  
  
  @Override
  public void run() {
    File source = jsonFile.requireExists(spec);
    
    var parser = new ReportTemplateParser()
        .setRefedImages(imagesOpt.makeImageMap(spec, source));
    
    // deserialize the source..
    var report = parser.toEntity(source);
    
    String refName = ref.name(spec);
    var ctx = new EditableRefContext(parser.getReferences());
    
    int fontStyle = FontSpec.styleInt(bold, italic, underline);
    
    if (size <= 0)
      throw new ParameterException(spec.commandLine(), "illegal --size: " + size);
    
    
    Color color;
    if (colorRef != null) try {
      color = parser.getReferences().getColor(colorRef);
    } catch (JsonParsingException notFound) {
      throw new ParameterException(spec.commandLine(),
          String.format("color ref '%s' is not defined", colorRef));
    } else
      color = null;
    
    
    {
      var upper = fontFamily.toUpperCase(Locale.ROOT);
      if (!FontSpec.standardFamilyNames().contains(upper)) {
        var tailSet = FontSpec.standardFamilyNames().tailSet(upper);
        int count = (int) tailSet.stream().filter(s -> s.startsWith(upper)).count();
        if (count == 1) {
          String ff = tailSet.first();
          System.out.printf("'%s' auto-completed to font family '%s'%n", fontFamily, ff);
          fontFamily = ff;
        } else if (count == 0) {
          throw new ParameterException(spec.commandLine(),
              "not a valid font family: " + fontFamily);
        } else if (fontFamily.isEmpty()) {
          throw new ParameterException(spec.commandLine(), "missing --ff value");
        } else {
          String msg = "'--ff " + fontFamily + "' is ambiguous. Possible matches are..";
          int index = 0;
          for (var fam : tailSet) {
            if (index == count)
              break;
            ++index;
            msg += System.lineSeparator() + "  " + fam;
          }
          throw new ParameterException(spec.commandLine(), msg);
        }
      }
    }
    
    var fontSpec = new FontSpec(fontFamily, size, fontStyle, color);
    
    
    if (!put(refName, fontSpec, ctx.fontRefs(), ctx.findRef(fontSpec), spec))
      return; // (already exists)
    
    var jObj = parser.setReferences(ctx).toJsonObject(report);
    jsonFile.write(jObj);
    
    System.out.printf("ref '%s' added to font dictionary%n",
        refName);
  }

}




class AlignH {
  
  @Option(names = "--left", description = "align left  (default)")
  boolean left;

  @Option(names = "--center", description = "align center")
  boolean center;

  @Option(names = "--right", description = "align right")
  boolean right;

  @Option(names = "--justified", description = "justified (paragraphs)")
  boolean justified;
  
  
  public Align.H toEnum() {
    if (center)
      return Align.H.CENTER;
    if (right)
      return Align.H.RIGHT;
    if (justified)
      return Align.H.JUSTIFIED;
    
    return Align.H.LEFT;
  }
  
}


class AlignV {

  @Option(names = "--bottom", description = "align bottom")
  boolean bottom;

  @Option(names = "--middle", description = "align middle  (default)")
  boolean middle;

  @Option(names = "--top", description = "align top")
  boolean top;
  
  
  public Align.V toEnum() {
    if (bottom)
      return Align.V.BOTTOM;
    if (top)
      return Align.V.TOP;
    
    return Align.V.MIDDLE;
  }
}



class AscDec {
  
  
  @Option(names = "--no-ascender")
  boolean noAscender;
  

  @Option(names = "--no-descender")
  boolean noDescender;
  
}



//+   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +  //


@Command(
name = "new-format",
description = {
  "Define a new cell-format in the dictionary.",
}
)
class NewFormatRef extends NewRefBase<CellFormat> implements Runnable {

  @Spec
  private CommandSpec spec;
  
  @Mixin
  private JsonFile jsonFile;
  
  @Mixin
  private ImagesOption imagesOpt;
  
  @Mixin
  private RefNameOpt ref;
  

  @Option(names = "--font-ref", paramLabel = "ref", required = true, description = "font reference")
  private String fontRef;

  @ArgGroup(exclusive = true, multiplicity = "0..1")
  private AlignH alignH;

  @ArgGroup(exclusive = true, multiplicity = "0..1")
  private AlignV alignV;
  
  
  @Option(names = "--bg-color-ref", paramLabel = "ref", description = "background color reference")
  private String bgColorRef;
  

  @Option(names = "--lead", paramLabel = "#.#", description = "leading (for text)")
  private float leading;

  @Option(names = "--pad", paramLabel = "#.#", description = "cell padding")
  private float padding;
  

  @ArgGroup(exclusive = true, multiplicity = "0..1")
  private AscDec ascDec;
  
  
  @Override
  public void run() {
    File source = jsonFile.requireExists(spec);
    
    var parser = new ReportTemplateParser()
        .setRefedImages(imagesOpt.makeImageMap(spec, source));
    
    // deserialize the source..
    var report = parser.toEntity(source);
    
  //  var color = rgb.color(spec);
    
    String refName = ref.name(spec);
    
    FontSpec font;
    try {
      font = parser.getReferences().getFont(fontRef);
    } catch (JsonParsingException notFound) {
      throw new ParameterException(spec.commandLine(),
          String.format("font ref '%s' is not defined", fontRef));
    }
    
    var format = new CellFormat(font);
    format.setLeading(leading);
    format.setPadding(padding);
    format.setAscender(!noAscender());
    format.setDescender(!noDescender());
    if (alignH != null)
      format.setAlignH(alignH.toEnum());
    if (alignV != null)
      format.setAlignV(alignV.toEnum());
    
    Color bgColor;
    if (bgColorRef != null) try {
      bgColor = parser.getReferences().getColor(bgColorRef);
    } catch (JsonParsingException notFound) {
      throw new ParameterException(spec.commandLine(),
          String.format("background color ref '%s' is not defined", bgColorRef));
    } else
      bgColor = null;
    
    format.setBackgroundColor(bgColor);

    var ctx = new EditableRefContext(parser.getReferences());
    
    if (!put(refName, format, ctx.cellFormatRefs(), ctx.findRef(format), spec))
      return; // (already exists)
    
    var jObj = parser.setReferences(ctx).toJsonObject(report);
    jsonFile.write(jObj);
    
    System.out.printf("ref '%s' added to cell-format dictionary%n",
        refName);
  }
  
  
  private boolean noAscender() {
    return ascDec != null && ascDec.noAscender;
  }
  
  private boolean noDescender() {
    return ascDec != null && ascDec.noDescender;
  }
  
  

}









/*

TODO:

- new-cell

Next steps:

- use morsel from ledger to define table cell auto-fills
  For this step, create a morsel for which _all_ its rows will be auto-filled
- next define a query that would continue to work if the morsel above is merged
  with other morsels from the ledger

*/

















