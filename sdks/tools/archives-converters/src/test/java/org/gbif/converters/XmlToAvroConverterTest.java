package org.gbif.converters;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Function;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class XmlToAvroConverterTest {

  private String getTestInputPath() {
    return getClass()
        .getResource("/responses/pages/7ef15372-1387-11e2-bb2e-00145eb45e9a/")
        .getFile();
  }

  @Test
  public void avroDeserializingNoramlIdTest() throws IOException {

    // State
    String inputPath = getTestInputPath() + "61";

    String outPath = inputPath + "verbatim.avro";

    // When
    XmlToAvroConverter.create().inputPath(inputPath).outputPath(outPath).convert();

    // Should
    File verbatim = new File(outPath);
    Assert.assertTrue(verbatim.exists());

    // Deserialize ExtendedRecord from disk
    DatumReader<ExtendedRecord> datumReader = new SpecificDatumReader<>(ExtendedRecord.class);
    try (DataFileReader<ExtendedRecord> dataFileReader =
        new DataFileReader<>(verbatim, datumReader)) {
      while (dataFileReader.hasNext()) {
        ExtendedRecord record = dataFileReader.next();

        Function<Term, String> fn = term -> record.getCoreTerms().get(term.qualifiedName());

        Assert.assertNotNull(record);
        Assert.assertNotNull(record.getId());
        Assert.assertEquals(18, record.getCoreTerms().size());
        Assert.assertTrue(record.getId().contains("catalog"));

        Assert.assertEquals("PreservedSpecimen", fn.apply(DwcTerm.basisOfRecord));
        Assert.assertEquals("11.04.1958", fn.apply(DwcTerm.eventDate));
        Assert.assertEquals("6884", fn.apply(DwcTerm.catalogNumber));
        Assert.assertEquals("Greece", fn.apply(DwcTerm.country));
        Assert.assertEquals("GR", fn.apply(DwcTerm.countryCode));
        Assert.assertEquals("Collection Myriapoda", fn.apply(DwcTerm.collectionCode));
        Assert.assertEquals("Schizopetalidae", fn.apply(DwcTerm.family));
        Assert.assertEquals("SMF", fn.apply(DwcTerm.institutionCode));
        Assert.assertEquals("Acanthopetalum minotauri", fn.apply(DwcTerm.scientificName));
        Assert.assertEquals("GDA94", fn.apply(DwcTerm.geodeticDatum));
        Assert.assertEquals("FootprintWKT", fn.apply(DwcTerm.footprintWKT));
        Assert.assertTrue(fn.apply(DwcTerm.recordedBy).contains("|"));
        Assert.assertEquals(
            "near Ajil Deka, Gotys, SE-side of Akropolis", fn.apply(DwcTerm.locality));
        Assert.assertEquals(
            "http://sesam.senckenberg.de/page/index.asp?objekt_id=704138&sprache_kurz=en",
            fn.apply(DcTerm.references));
      }
    }

    Files.deleteIfExists(verbatim.toPath());
  }
}
