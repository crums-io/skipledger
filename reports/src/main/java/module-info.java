module io.crums.sldg.reports.pdf {
  requires transitive io.crums.sldg.ledgers;
  
  requires java.desktop;
  requires com.github.librepdf.openpdf;
  
  exports io.crums.sldg.reports.pdf;
  exports io.crums.sldg.reports.pdf.func;
  exports io.crums.sldg.reports.pdf.input;
  exports io.crums.sldg.reports.pdf.json;
  exports io.crums.sldg.reports.pdf.pred;
}