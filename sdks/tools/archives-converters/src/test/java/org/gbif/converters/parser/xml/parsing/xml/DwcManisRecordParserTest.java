package org.gbif.converters.parser.xml.parsing.xml;

import static org.junit.Assert.assertEquals;

import org.gbif.converters.parser.xml.model.IdentifierRecord;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.model.TypificationRecord;
import org.junit.Test;

public class DwcManisRecordParserTest extends ParserTestCase {

  @Test
  public void testParseBasicFields() {
    String fileName =
        getClass().getResource("/responses/dwc_manis/dwc_manis_all_simple_fields.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    // common to all schemas
    assertEquals("ANSP", ror.getInstitutionCode());
    assertEquals("HRP", ror.getCollectionCode());
    assertEquals("PreservedSpecimen", ror.getBasisOfRecord());

    // common to dwc
    assertEquals("Gyrinophilus porphyriticus porphyriticus", ror.getScientificName());
    assertEquals("Amphibia", ror.getKlass());
    assertEquals("Caudata", ror.getOrder());
    assertEquals("Plethodontidae", ror.getFamily());
    assertEquals("Gyrinophilus", ror.getGenus());
    assertEquals("E. S. & W. I. Mattin", ror.getCollectorName());
    assertEquals("New Jersey", ror.getStateOrProvince());
    assertEquals("USA", ror.getCountry());
    assertEquals("Sussex Co.", ror.getCounty());

    // dwc manis only
    assertEquals("513", ror.getCatalogueNumber());
    assertEquals("17-21-10", ror.getOccurrenceDate());
  }

  @Test
  public void testParseIdentifiers() {
    String fileName = getClass().getResource("/responses/dwc_manis/dwc_manis_idtype.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    // showIdentifiers(ror);
    assertEquals(1, ror.getIdentifierRecords().size());
    IdentifierRecord idRec = ror.getIdentifierRecords().get(0);
    assertEquals(3, idRec.getIdentifierType().intValue());
    assertEquals("SZ 233", idRec.getIdentifier());
  }

  @Test
  public void testParseTypification() {
    String fileName =
        getClass().getResource("/responses/dwc_manis/dwc_manis_typification.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    /** TODO: is this a valid TypificationRecord? */
    // showTypifications(ror);
    assertEquals(1, ror.getTypificationRecords().size());
    TypificationRecord typRecord = ror.getTypificationRecords().iterator().next();
    assertEquals("0", typRecord.getTypeStatus());
  }
}
