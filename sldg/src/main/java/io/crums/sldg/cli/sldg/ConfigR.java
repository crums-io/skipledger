/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.cli.sldg;

import java.io.File;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import io.crums.io.FileUtils;
import io.crums.sldg.packs.AssetsBuilder;
import io.crums.sldg.reports.pdf.ReportAssets;
import io.crums.sldg.sql.Config;
import io.crums.util.Lists;

/**
 * 
 */
public class ConfigR extends Config {
  
  public final static String REPORT_PATH_PROPERTY = "sldg.template.report.path";

  
  public final static List<String> PROP_NAMES_PLUS = Lists.concat(
      PROP_NAMES, REPORT_PATH_PROPERTY);
  
  
  
  

  private final Optional<File> reportPath;
  
  public ConfigR(File file) {
    this(loadProperties(file));
  }

  public ConfigR(Properties props) {
    super(props);
    var path = props.getProperty(REPORT_PATH_PROPERTY);
    if (path == null || path.isBlank())
      reportPath = Optional.empty();
    else {
      File f = FileUtils.getRelativeUnlessAbsolute(path, baseDir);
      String warning = null;
      if (f.exists()) {
        // verify
        Optional<File> opt;
        try {
          ReportAssets.setReport(new AssetsBuilder(), f);
          opt = Optional.of(f);
        } catch (Exception x) {
          x.printStackTrace();
          warning =
              "Error on parsing/loading report template assets from " + f +
              " Detail: " + x.getMessage() + ". No report template path set.";
          opt = Optional.empty();
        }
        reportPath = opt;
      } else {
        warning =
            "Ignoring improper value in property " + REPORT_PATH_PROPERTY + "=" + path +
            " Resolves to " + f + " (using base dir " + baseDir + " )" +
            " which does not exist. No report template path set.";
        
        reportPath = Optional.empty();
      }
      if (warning != null)
        System.getLogger(Sldg.class.getSimpleName()).log(Level.WARNING, warning);
    }
  }
  
  
  
  @Override
  protected List<String> propNames() {
    return PROP_NAMES_PLUS;
  }
  

  /**
   * Returns the path to the report template assets. This is the path
   * specified in {@linkplain ReportAssets#setReport(AssetsBuilder, File)}.
   * 
   * @return <em>validated</em> file path, if present; empty, o.w.
   */
  public Optional<File> getReportPath() {
    return reportPath;
  }
  
}
