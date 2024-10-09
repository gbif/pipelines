package org.gbif.converters;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DwcaToAvroConverterTest {

  @Test
  public void avroDeserializingNoramlIdTest() throws IOException {

    String inpPath = getClass().getResource("/dwca/plants_dwca").getFile();
    String outPath = inpPath + "/verbatim.avro";
    String metricPath = inpPath + "/metric.yaml";

    // When
    DwcaToAvroConverter.create()
        .inputPath(inpPath)
        .outputPath(outPath)
        .metaPath(metricPath)
        .convert();

    // Should
    File verbatim = new File(outPath);
    Assert.assertTrue(verbatim.exists());

    // Deserialize ExtendedRecord from disk
    DatumReader<ExtendedRecord> datumReader = new SpecificDatumReader<>(ExtendedRecord.class);
    try (DataFileReader<ExtendedRecord> dataFileReader =
        new DataFileReader<>(verbatim, datumReader)) {
      while (dataFileReader.hasNext()) {
        ExtendedRecord record = dataFileReader.next();
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.getId());
      }
    }

    Assert.assertTrue(Files.exists(Paths.get(metricPath)));
    Files.deleteIfExists(verbatim.toPath());
  }

  @Test
  public void csvConverterTest() throws Exception {

    String inpPath = getClass().getResource("/dwca/csv").getFile();
    String outPath = inpPath + "/verbatim.avro";

    // When
    DwcaToAvroConverter.create().inputPath(inpPath).outputPath(outPath).convert();

    // Should
    File verbatim = new File(outPath);
    Assert.assertTrue(verbatim.exists());

    int count = 0;
    // Deserialize ExtendedRecord from disk
    DatumReader<ExtendedRecord> datumReader = new SpecificDatumReader<>(ExtendedRecord.class);
    try (DataFileReader<ExtendedRecord> dataFileReader =
        new DataFileReader<>(verbatim, datumReader)) {
      while (dataFileReader.hasNext()) {
        count++;
        ExtendedRecord record = dataFileReader.next();
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.getId());
      }
    }
    Assert.assertEquals(13, count);

    Files.deleteIfExists(verbatim.toPath());
  }

  @Test
  public void xlsxConverterTest() throws Exception {

    String inpPath = getClass().getResource("/dwca/xlsx").getFile();
    String outPath = inpPath + "/verbatim.avro";

    // When
    DwcaToAvroConverter.create().inputPath(inpPath).outputPath(outPath).convert();

    // Should
    File verbatim = new File(outPath);
    Assert.assertTrue(verbatim.exists());

    int count = 0;
    // Deserialize ExtendedRecord from disk
    DatumReader<ExtendedRecord> datumReader = new SpecificDatumReader<>(ExtendedRecord.class);
    try (DataFileReader<ExtendedRecord> dataFileReader =
        new DataFileReader<>(verbatim, datumReader)) {
      while (dataFileReader.hasNext()) {
        count++;
        ExtendedRecord record = dataFileReader.next();
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.getId());
      }
    }
    Assert.assertEquals(54, count);

    Files.deleteIfExists(verbatim.toPath());
  }

  @Ignore
  @Test
  public void odsConverterTest() throws Exception {

    String inpPath = getClass().getResource("/dwca/ods").getFile();
    String outPath = inpPath + "/verbatim.avro";

    // When
    DwcaToAvroConverter.create().inputPath(inpPath).outputPath(outPath).convert();

    // Should
    File verbatim = new File(outPath);
    Assert.assertTrue(verbatim.exists());

    int count = 0;
    // Deserialize ExtendedRecord from disk
    DatumReader<ExtendedRecord> datumReader = new SpecificDatumReader<>(ExtendedRecord.class);
    try (DataFileReader<ExtendedRecord> dataFileReader =
        new DataFileReader<>(verbatim, datumReader)) {
      while (dataFileReader.hasNext()) {
        count++;
        ExtendedRecord record = dataFileReader.next();
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.getId());
      }
    }
    Assert.assertEquals(1, count);

    Files.deleteIfExists(verbatim.toPath());
  }
}
