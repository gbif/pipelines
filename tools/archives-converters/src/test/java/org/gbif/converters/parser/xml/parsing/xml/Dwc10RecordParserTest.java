package org.gbif.converters.parser.xml.parsing.xml;

import static org.junit.Assert.assertEquals;

import org.gbif.converters.parser.xml.model.IdentifierRecord;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.model.TypificationRecord;
import org.junit.Test;

public class Dwc10RecordParserTest extends ParserTestCase {

  @Test
  public void testParseBasicFields() {
    String fileName =
        getClass().getResource("/responses/dwc10/dwc10_all_simple_fields.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    // common to all schemas
    assertEquals("BR", ror.getInstitutionCode());
    assertEquals("MYCO", ror.getCollectionCode());
    assertEquals("S", ror.getBasisOfRecord());

    // common to dwc
    assertEquals("Amaurochaete", ror.getScientificName());
    assertEquals("Protista", ror.getKingdom());
    assertEquals("Myxomycota", ror.getPhylum());
    assertEquals("Myxomycetes", ror.getKlass());
    assertEquals("STEMONITALES", ror.getOrder());
    assertEquals("STEMONITIDACEAE", ror.getFamily());
    assertEquals("Amaurochaete", ror.getGenus());
    assertEquals("Mannenga-Bremekamp N.", ror.getCollectorName());
    assertEquals("Wetering; Beukenburg - Beukenlaan", ror.getLocality());
    assertEquals("NL", ror.getCountry());

    // dwc 1.0 only
    assertEquals("1", ror.getCatalogueNumber());
    assertEquals("52.755", ror.getLatitude());
    assertEquals("5.9963", ror.getLongitude());
    assertEquals("1958-8-15", ror.getOccurrenceDate());
  }

  @Test
  public void testParseIdentifiers() {
    String fileName = getClass().getResource("/responses/dwc10/dwc10_idtype.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    // showIdentifiers(ror);
    assertEquals(1, ror.getIdentifierRecords().size());
    IdentifierRecord idRec = ror.getIdentifierRecords().get(0);
    assertEquals(3, idRec.getIdentifierType().intValue());
    assertEquals("2873", idRec.getIdentifier());
  }

  @Test
  public void testParseTypification() {
    String fileName = getClass().getResource("/responses/dwc10/dwc10_typification.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    // showTypifications(ror);
    assertEquals(1, ror.getTypificationRecords().size());
    TypificationRecord typRecord = ror.getTypificationRecords().iterator().next();
    assertEquals("Isotype", typRecord.getTypeStatus());
  }
}
