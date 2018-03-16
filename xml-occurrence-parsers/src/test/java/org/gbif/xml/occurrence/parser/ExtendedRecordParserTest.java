package org.gbif.xml.occurrence.parser;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExtendedRecordParserTest {

  private final String PATH = getClass().getResource("/responses/pages/7ef15372-1387-11e2-bb2e-00145eb45e9a/").getFile();

  @Test(expected = ParsingException.class)
  public void testInputPathIsAbsent() {
    ExtendedRecordParser.convertFromXML("", "test");
  }

  @Test(expected = ParsingException.class)
  public void testOutputPathIsAbsent() {
    ExtendedRecordParser.convertFromXML("test", "");
  }

  @Test(expected = ParsingException.class)
  public void testInputPathIsNull() {
    ExtendedRecordParser.convertFromXML(null, "test");
  }

  @Test(expected = ParsingException.class)
  public void testOutputPathIsNull() {
    ExtendedRecordParser.convertFromXML("test", null);
  }

  @Test(expected = ParsingException.class)
  public void testInputPathNotValid() {
    ExtendedRecordParser.convertFromXML("test", "test");
  }

  @Test(expected = ParsingException.class)
  public void testInputFileWrongExtension() {
    // State
    String inputPath = PATH + "61.zip";

    // When
    ExtendedRecordParser.convertFromXML(inputPath, "test");
  }

  @Test
  public void testParsingDirectory() throws IOException {
    // State
    String inputPath = PATH + "61";
    String outputPath = PATH + "verbatim.avro";

    // When
    ExtendedRecordParser.convertFromXML(inputPath, outputPath);

    // Should
    File verbtim = new File(outputPath);
    Assert.assertTrue(verbtim.exists());
    Files.deleteIfExists(verbtim.toPath());
  }

  @Test
  public void testParsingArchive() throws IOException {
    // State
    String inputPath = PATH + "61.tar.xz";
    String outputPath = PATH + "verbatim.avro";

    // When
    ExtendedRecordParser.convertFromXML(inputPath, outputPath);

    // Should
    File verbtim = new File(outputPath);
    Assert.assertTrue(verbtim.exists());
    Files.deleteIfExists(verbtim.toPath());
  }

  @Test
  public void testAvroDeserializing() throws IOException {
    // State
    String inputPath = PATH + "61";
    String outputPath = PATH + "verbatim.avro";

    // When
    ExtendedRecordParser.convertFromXML(inputPath, outputPath);

    // Should
    File verbtim = new File(outputPath);
    Assert.assertTrue(verbtim.exists());

    // Deserialize ExtendedRecord from disk
    DatumReader<ExtendedRecord> datumReader = new SpecificDatumReader<>(ExtendedRecord.class);
    DataFileReader<ExtendedRecord> dataFileReader = new DataFileReader<>(verbtim, datumReader);
    ExtendedRecord record = null;
    while (dataFileReader.hasNext()) {
      record = dataFileReader.next(record);
      Assert.assertNotNull(record);
      Assert.assertNotNull(record.getId());
    }

    Files.deleteIfExists(verbtim.toPath());
  }

}