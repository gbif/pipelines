package org.gbif.converters.parser.xml.parsing.xml;

import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Dwc14RecordParserTest extends ParserTestCase {

  @Test
  public void testParseBasicFields() {
    String fileName =
        getClass().getResource("/responses/dwc14/dwc14_all_simple_fields.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    // System.out.println(ror.debugDump());
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

    /** TODO: find test record with types */
    // showTypifications(ror);
    assertEquals(0, ror.getTypificationRecords().size());
    // TypificationRecord typRecord = ror.getTypificationRecords().iterator().next();
    // assertEquals("Fake species Linnaeus, 1771", typRecord.getScientificName());
    // assertEquals("Fake title citation.", typRecord.getPublication());
    // assertEquals("Holotype", typRecord.getTypeStatus());
    // assertEquals("Some fake notes.", typRecord.getNotes());
  }

  /** TODO: find test record with image(s) */
  @Test
  public void testParseImages() {
    String fileName = getClass().getResource("/responses/dwc14/dwc14_images.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);
    // showImages(ror);
    assertEquals(0, ror.getImageRecords().size());
    // ImageRecord image = ror.getImageRecords().get(0);
    // assertEquals("http://biology.africamuseum.be/STERNAImages/Ornithology/SternaRMCADetails.php?image=_PHM7832",
    // image.getUrl());
    // assertEquals("zoomable image", image.getDescription());
  }

  /** TODO: find test record with links */
  @Test
  public void testParseLinks() {
    String fileName = getClass().getResource("/responses/dwc14/dwc14_links.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);
    // showLinks(ror);
    assertEquals(0, ror.getLinkRecords().size());
  }
}
