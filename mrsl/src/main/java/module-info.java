module io.crums.sldg.mrsl_cli {
  requires io.crums.sldg.ledgers;
  requires io.crums.legacy.tc1;
  requires io.crums.util;
  requires io.crums.sldg.reports.pdf;
  requires info.picocli;
  opens io.crums.sldg.cli.mrsl to info.picocli;
}