module io.crums.sldg.mrsl_cli {
  requires io.crums.sldg.base;
  requires io.crums.core;
  requires io.crums.util;
  requires io.crums.sldg.reports.pdf;
  requires info.picocli;
  opens io.crums.sldg.cli.mrsl to info.picocli;
}