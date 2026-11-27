package org.gbif.pipelines.core.converters;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.junit.Assert;
import org.junit.Test;

public class ConverterUtilsTest {

  @Test
  public void unknownTermDoesNotPopulateInterpretedTermColumnTest() {
    OccurrenceHdfsRecord hdfsRecord = new OccurrenceHdfsRecord();

    ConverterUtils.mapTerm("http://unknown.org/taxonKey", "my-bad-key", hdfsRecord);
    ConverterUtils.mapTerm("http://unknown.org/kingdomKey", "my-bad-kingdom-key", hdfsRecord);

    Assert.assertNull(hdfsRecord.getTaxonkey());
    Assert.assertNull(hdfsRecord.getKingdomkey());
  }

  @Test
  public void interpretedSourceTermDoesNotPopulateInterpretedColumnTest() {
    OccurrenceHdfsRecord hdfsRecord = new OccurrenceHdfsRecord();

    ConverterUtils.mapTerm(GbifTerm.taxonKey.qualifiedName(), "my-bad-key", hdfsRecord);

    Assert.assertNull(hdfsRecord.getTaxonkey());
  }

  @Test
  public void nonInterpretedTermPopulatesVerbatimAndInterpretedColumnsTest() {
    OccurrenceHdfsRecord hdfsRecord = new OccurrenceHdfsRecord();

    ConverterUtils.mapTerm(DwcTerm.locality.qualifiedName(), "Copenhagen", hdfsRecord);

    Assert.assertEquals("Copenhagen", hdfsRecord.getV_locality());
    Assert.assertEquals("Copenhagen", hdfsRecord.getLocality());
  }
}
