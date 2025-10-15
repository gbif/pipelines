package org.gbif.converters.parser.xml.parsing.xml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import org.gbif.converters.parser.xml.model.Collector;
import org.gbif.converters.parser.xml.model.ImageRecord;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.model.TypificationRecord;
import org.junit.Test;

public class Abcd206RecordParserTest extends ParserTestCase {

  @Test
  public void testParseBasicFields() {
    String fileName =
        getClass().getResource("/responses/abcd206/abcd206_all_simple_fields.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    assertEquals("BGBM", ror.getInstitutionCode());
    assertEquals("AlgaTerra", ror.getCollectionCode());
    assertEquals("HumanObservation", ror.getBasisOfRecord());
    assertEquals("DE", ror.getCountryCode());
    assertEquals("Germany", ror.getCountry());
    assertEquals(
        "Kusber, W.-H.", ror.getCollectors().stream().map(Collector::getName).findFirst().get());
    assertEquals("Nikolassee, Berlin", ror.getLocality());
    assertEquals("5834", ror.getCatalogueNumber());
    assertEquals("52.423798", ror.getDecimalLatitude());
    assertEquals("13.191434", ror.getDecimalLongitude());
    assertEquals("1987-04-13T00:00:00", ror.getOccurrenceDate());
    assertEquals("400", ror.getMinAltitude());
    assertEquals("500", ror.getMaxAltitude());
    assertEquals("50", ror.getLatLongPrecision());
    assertEquals("GDA94", ror.getGeodeticDatum());
    assertEquals("0123456789ABCD", ror.getId());
    assertEquals("19870413whk1", ror.getCollectorsFieldNumber());
    assertEquals("SEE REMARKS REF W45501", ror.getOccurrenceRemarks());
    assertEquals("length measurement", ror.getPreparations());
    assertEquals("06/06/1978", ror.getModified());
    assertEquals(
        "http://purl.oclc.org/net/edu.harvard.huh/guid/uuid/0e90e511-4ab7-4ca1-ba13-eac3d37bd345|http://viaf.org/viaf/171878794|http://www.wikidata.org/entity/Q1612245",
        ror.getRecordedByID());
    assertEquals(
        "https://www.europlusmed.org/cdm_dataportal/taxon/F8D1EADB-7D91-4E3B-BF96-FD15A0BD888B",
        ror.getScientificNameID());
    assertEquals(
        "http://purl.oclc.org/net/edu.harvard.huh/guid/uuid/d5fea488-5786-4106-af90-396ef452c3aa|http://viaf.org/viaf/100383596|http://www.wikidata.org/entity/Q78738",
        ror.getIdentifiedByID());
    assertEquals("112358", ror.getAssociatedSequences());
  }

  @Test
  public void testParseIdentifiers() {
    String fileName = getClass().getResource("/responses/abcd206/abcd206_idtype.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    assertEquals(7, ror.getIdentifierRecords().size());
    assertEquals("0123456789ABCD", ror.getId());
  }

  @Test
  public void testParseSingleIdentificationLatin() {
    String fileName = getClass().getResource("/responses/abcd206/abcd206_id_latin.gz").getFile();
    List<RawOccurrenceRecord> rors = setupRor(fileName);
    assertEquals(1, rors.size());
    RawOccurrenceRecord ror = rors.get(0);

    assertEquals("Conjugatophyceae", ror.getKlass());
    assertEquals("Desmidiales", ror.getOrder());
    assertEquals("Closteriaceae", ror.getFamily());
  }

  @Test
  public void testParseMultiIdentificationPreferredFalse() {
    String fileName =
        getClass().getResource("/responses/abcd206/abcd206_multi_ids_preferred_false.gz").getFile();
    List<RawOccurrenceRecord> rors = setupRor(fileName);
    assertEquals(2, rors.size());

    RawOccurrenceRecord ror = rors.get(0);
    assertEquals("Schistidium agassizii Sull. & Lesq. in Sull.", ror.getScientificName());
    assertEquals("Ochyra, Ryszard", ror.getIdentifierName());

    ror = rors.get(1);
    assertEquals("Grimmia alpicola Sw. ex Hedw.", ror.getScientificName());
    assertEquals("Bridel, Samuel", ror.getIdentifierName());
  }

  @Test
  public void testParseMultiIdentificationNoPreferred() {
    String fileName =
        getClass().getResource("/responses/abcd206/abcd206_multi_ids_no_preferred.gz").getFile();
    List<RawOccurrenceRecord> rors = setupRor(fileName);
    assertEquals(2, rors.size());

    RawOccurrenceRecord ror = rors.get(0);
    assertEquals("Sheppardia bocagei chapini (Benson, 1955)", ror.getScientificName());
    assertEquals("Benson", ror.getIdentifierName());

    ror = rors.get(1);
    assertEquals("Cossypha bocagei hallae Prigogine, 1969", ror.getScientificName());
    assertNull(ror.getIdentifierName());
  }

  @Test
  public void testParseTypification() {
    String fileName =
        getClass().getResource("/responses/abcd206/abcd206_typification.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    assertEquals(1, ror.getTypificationRecords().size());
    TypificationRecord typRecord = ror.getTypificationRecords().iterator().next();
    assertEquals("Fake species Linnaeus, 1771", typRecord.getScientificName());
    assertEquals("Fake title citation.", typRecord.getPublication());
    assertEquals("Holotype", typRecord.getTypeStatus());
    assertEquals("Some fake notes.", typRecord.getNotes());
  }

  @Test
  public void testParseImages() {
    String fileName = getClass().getResource("/responses/abcd206/abcd206_images.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);
    assertEquals(7, ror.getImageRecords().size());
    ImageRecord image = ror.getImageRecords().get(0);
    assertEquals(
        "http://www.tierstimmenarchiv.de/recordings/Ailuroedus_buccoides_V2010_04_short.mp3",
        image.getUrl());
    assertEquals(
        "http://www.tierstimmenarchiv.de/webinterface/contents/showdetails.php?edit=-1&unique_id=TSA:Ailuroedus_buccoides_V_2010_4_1&autologin=true",
        image.getPageUrl());
    assertEquals(
        "CC BY-NC-ND (Attribution for non commercial use only and without derivative)",
        image.getRights());

    image = ror.getImageRecords().get(1);
    assertEquals(
        "http://biology.africamuseum.be/STERNAImages/Ornithology/SternaRMCADetails.php?image=_PHM7832",
        image.getUrl());
    assertEquals("zoomable image", image.getDescription());
  }

  @Test
  public void testParseLinks() {
    String fileName = getClass().getResource("/responses/abcd206/abcd206_links.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);
    assertEquals(2, ror.getLinkRecords().size());
  }
}
