package org.gbif.converters.parser.xml.parsing.xml;

import static org.junit.Assert.assertEquals;

import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.junit.Test;

public class Dwc14RecordParserTest extends ParserTestCase {

  @Test
  public void testParseBasicFields() {
    String fileName =
        getClass().getResource("/responses/dwc14/dwc14_all_simple_fields.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    // common to all schemas
    assertEquals("UGENT", ror.getInstitutionCode());
    assertEquals("vertebrata", ror.getCollectionCode());
    assertEquals("PreservedSpecimen", ror.getBasisOfRecord());
    assertEquals("200", ror.getMinAltitude());
    assertEquals("400", ror.getMaxAltitude());
    assertEquals("25", ror.getLatLongPrecision());

    // common to dwc
    assertEquals("Alouatta villosa Gray, 1845", ror.getScientificName());
    assertEquals("Gray, 1845", ror.getAuthor());
    assertEquals("Animalia", ror.getKingdom());
    assertEquals("Chordata", ror.getPhylum());
    assertEquals("Mammalia", ror.getKlass());
    assertEquals("Primates", ror.getOrder());
    assertEquals("Atelidae", ror.getFamily());
    assertEquals("Alouatta", ror.getGenus());
    assertEquals("villosa", ror.getSpecies());

    // dwc 1.4 only
    assertEquals("50058", ror.getCatalogueNumber());
    assertEquals("UGENT:vertebrata:50058", ror.getId());

    /** TODO: more complete sample */
  }

  @Test
  public void testParseIdentifiers() {
    String fileName = getClass().getResource("/responses/dwc14/dwc14_idtype.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    // showIdentifiers(ror);
    assertEquals(2, ror.getIdentifierRecords().size());
  }

  @Test
  public void testParseTypification() {
    String fileName = getClass().getResource("/responses/dwc14/dwc14_typification.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    assertEquals(0, ror.getTypificationRecords().size());
  }

  /** TODO: find test record with image(s) */
  @Test
  public void testParseImages() {
    String fileName = getClass().getResource("/responses/dwc14/dwc14_images.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);
    assertEquals(0, ror.getImageRecords().size());
  }

  /** TODO: find test record with links */
  @Test
  public void testParseLinks() {
    String fileName = getClass().getResource("/responses/dwc14/dwc14_links.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);
    assertEquals(0, ror.getLinkRecords().size());
  }
}
