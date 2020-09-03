/*
 * Copyright 2011 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.converters.parser.xml.parsing.xml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import org.gbif.converters.parser.xml.model.ImageRecord;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.model.TypificationRecord;
import org.junit.Test;

public class Abcd12RecordParserTest extends ParserTestCase {

  @Test
  public void testParseBasicFields() {
    String fileName =
        getClass().getResource("/responses/abcd12/abcd12_all_simple_fields.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    assertEquals("TLMF", ror.getInstitutionCode());
    assertEquals("Tiroler Landesmuseum Ferdinandeum", ror.getCollectionCode());
    assertEquals("AUT", ror.getCountry());
    assertEquals("Oschütz", ror.getCollectorName());
    assertEquals("Seefeld", ror.getLocality());
    assertEquals("82D45C93-B297-490E-B7B0-E0A9BEED1326", ror.getCatalogueNumber());
    assertEquals("47.3303167192", ror.getLatitude());
    assertEquals("11.2041081855", ror.getLongitude());
    assertEquals("1999-12-31", ror.getOccurrenceDate());
  }

  @Test
  public void testParseBasicFields2() {
    String fileName =
        getClass().getResource("/responses/abcd12/abcd12_simple_fields_2.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    assertEquals("TLMF", ror.getInstitutionCode());
    assertEquals("Tiroler Landesmuseum Ferdinandeum", ror.getCollectionCode());
    assertEquals("AUT", ror.getCountry());
    assertEquals("Polatschek A.", ror.getCollectorName());
    assertEquals("Woergl - Bhf. bis Woergler-Bach-Muendung und Angath", ror.getLocality());
    assertEquals("9DACB0BF-470D-4FD1-853A-5B04A995E073", ror.getCatalogueNumber());
    assertEquals("47.5007432291", ror.getLatitude());
    assertEquals("12.0669257631", ror.getLongitude());
    assertEquals("1968-06-23", ror.getOccurrenceDate());
  }

  @Test
  public void testParseMultiIdentificationPreferredTrue() {
    String fileName =
        getClass().getResource("/responses/abcd12/abcd12_multi_ids_preferred_true.gz").getFile();
    List<RawOccurrenceRecord> rors = setupRor(fileName);
    assertEquals(1, rors.size());

    RawOccurrenceRecord ror = rors.get(0);
    assertEquals("Ancylobotrys petersiana", ror.getScientificName());
    assertEquals("Vonk, G.J.A", ror.getIdentifierName());
    assertEquals("Apocynaceae", ror.getFamily());
    assertEquals("19920000", ror.getDateIdentified());
  }

  @Test
  public void testParseIdentifiers() {
    String fileName = getClass().getResource("/responses/abcd12/abcd12_idtype.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    assertEquals(2, ror.getIdentifierRecords().size());
  }

  @Test
  public void testParseTypification() {
    String fileName = getClass().getResource("/responses/abcd12/abcd12_typification.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);

    assertEquals(1, ror.getTypificationRecords().size());
    TypificationRecord typRecord = ror.getTypificationRecords().iterator().next();
    assertEquals("Species", typRecord.getScientificName());
    assertNull(typRecord.getPublication());
    assertEquals("Holotype", typRecord.getTypeStatus());
    assertNull(typRecord.getNotes());
  }

  @Test
  public void testParseImages() {
    String fileName = getClass().getResource("/responses/abcd12/abcd12_images.gz").getFile();
    RawOccurrenceRecord ror = setupRor(fileName).get(0);
    assertEquals(1, ror.getImageRecords().size());
    ImageRecord image = ror.getImageRecords().get(0);
    assertEquals(
        "http://www.herbariumhamburgense.uni-hamburg.de/hbg/imag_bg/HBG-502719.jpg",
        image.getUrl());
  }
}
