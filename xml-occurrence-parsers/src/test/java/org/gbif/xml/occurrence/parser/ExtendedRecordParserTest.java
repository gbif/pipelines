package org.gbif.xml.occurrence.parser;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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

  private final String inpPath =
    getClass().getResource("/responses/pages/7ef15372-1387-11e2-bb2e-00145eb45e9a/").getFile();
  private final String outPath = inpPath + "verbatim.avro";

  @Test(expected = ParsingException.class)
  public void testInputPathIsAbsent() throws IOException {
    try (OutputStream output = new FileOutputStream(outPath)) {
      ExtendedRecordParser.convertFromXML("", output);
    }
  }

  @Test(expected = ParsingException.class)
  public void testOutputPathIsAbsent() throws IOException {
    try (OutputStream output = new FileOutputStream(outPath)) {
      ExtendedRecordParser.convertFromXML("test", output);
    }
  }

  @Test(expected = ParsingException.class)
  public void testInputPathIsNull() throws IOException {
    try (OutputStream output = new FileOutputStream(outPath)) {
      ExtendedRecordParser.convertFromXML(null, output);
    }
  }

  @Test(expected = ParsingException.class)
  public void testOutputPathIsNull() {
    ExtendedRecordParser.convertFromXML("test", null);
  }

  @Test(expected = ParsingException.class)
  public void testInputPathNotValid() throws IOException {
    try (OutputStream output = new FileOutputStream(outPath)) {
      ExtendedRecordParser.convertFromXML("test", output);
    }
  }

  @Test(expected = ParsingException.class)
  public void testInputFileWrongExtension() throws IOException {
    // State
    String inputPath = inpPath + "61.zip";

    // When
    try (OutputStream output = new FileOutputStream(outPath)) {
      ExtendedRecordParser.convertFromXML(inputPath, output);
    }
  }

  @Test
  public void testParsingDirectory() throws IOException {
    // State
    String inputPath = inpPath + "61";

    // When
    try (OutputStream output = new FileOutputStream(outPath)) {
      ExtendedRecordParser.convertFromXML(inputPath, output);
    }

    // Should
    File verbatim = new File(outPath);
    Assert.assertTrue(verbatim.exists());
    Files.deleteIfExists(verbatim.toPath());
  }

  @Test
  public void testParsingArchive() throws IOException {
    // State
    String inputPath = inpPath + "61.tar.xz";

    // When
    try (OutputStream output = new FileOutputStream(outPath)) {
      ExtendedRecordParser.convertFromXML(inputPath, output);
    }

    // Should
    File verbatim = new File(outPath);
    Assert.assertTrue(verbatim.exists());
    Files.deleteIfExists(verbatim.toPath());
  }

  @Test
  public void testAvroDeserializing() throws IOException {
    // State
    String inputPath = inpPath + "61";

    // When
    try (OutputStream output = new FileOutputStream(outPath)) {
      ExtendedRecordParser.convertFromXML(inputPath, output);
    }

    // Should
    File verbatim = new File(outPath);
    Assert.assertTrue(verbatim.exists());

    // Deserialize ExtendedRecord from disk
    DatumReader<ExtendedRecord> datumReader = new SpecificDatumReader<>(ExtendedRecord.class);
    try (DataFileReader<ExtendedRecord> dataFileReader = new DataFileReader<>(verbatim, datumReader)) {
      while (dataFileReader.hasNext()) {
        ExtendedRecord record = dataFileReader.next();
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.getId());
      }
    }

    Files.deleteIfExists(verbatim.toPath());
  }

}