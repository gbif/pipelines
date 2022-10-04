package org.gbif.converters.parser.xml.parsing.xml;

import static org.junit.Assert.assertEquals;

import org.gbif.converters.parser.xml.model.IdentifierRecord;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.junit.Test;

public class Dwc2009RecordParserTest extends ParserTestCase {

  @Test
  public void testParseBasicFields() {
    String fileName =
        getClass().getResource("/responses/dwc2009/dwc2009_simple_fields.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    // common to all schemas
    assertEquals("BGBM", ror.getInstitutionCode());
    assertEquals("Pontaurus", ror.getCollectionCode());
    assertEquals("specimen", ror.getBasisOfRecord());
    assertEquals("2360", ror.getMinAltitude());

    // common to dwc
    assertEquals("Acantholimon armenum var. armenum", ror.getScientificName());
    assertEquals("Markus Döring", ror.getCollectorName());
    assertEquals("Markus Döring", ror.getIdentifierName());
    assertEquals("Plantae", ror.getKingdom());
    assertEquals("Magnoliophyta", ror.getPhylum());
    assertEquals("Magnoliopsida", ror.getKlass());
    assertEquals("Plumbaginales", ror.getOrder());
    assertEquals("Plumbaginaceae", ror.getFamily());
    assertEquals("Acantholimon", ror.getGenus());

    // dwc 1.4/2009 only
    assertEquals("2004", ror.getCatalogueNumber());
    assertEquals("Turkey", ror.getCountry());
    assertEquals("29", ror.getDay());
    assertEquals("7", ror.getMonth());
    assertEquals("1999", ror.getYear());
    assertEquals("1999-7-29", ror.getOccurrenceDate());
    assertEquals("37.42", ror.getLatitude());
    assertEquals("34.568", ror.getLongitude());

    IdentifierRecord id = ror.getIdentifierRecords().get(0);
    assertEquals("142316220", id.getIdentifier());

    assertEquals("142316220", ror.getId());

    /** TODO: more complete sample */
  }
}
