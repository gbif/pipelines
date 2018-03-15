package org.gbif.xml.occurrence.parser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.junit.Test;

public class ExtendedRecordParserTest {

  @Test
  public void multi() throws IOException {
    String inputPath = getClass().getResource("/responses/7ef15372-1387-11e2-bb2e-00145eb45e9a/61").getFile();
    String outputPath = "verbatim.avro";

    ExtendedRecordParser.convertFromXML(inputPath, outputPath);

    File verbtim = new File(outputPath);
    Files.deleteIfExists(verbtim.toPath());
  }

}