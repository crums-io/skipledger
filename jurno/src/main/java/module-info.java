module io.crums.sldg.jurno_cli {
  requires io.crums.sldg.logs;
  requires io.crums.util;
  requires info.picocli;
  opens io.crums.sldg.cli.jurno to info.picocli;
  
}