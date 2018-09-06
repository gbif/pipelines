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

import org.gbif.converters.parser.xml.OccurrenceParser;
import org.gbif.converters.parser.xml.model.IdentifierRecord;
import org.gbif.converters.parser.xml.model.ImageRecord;
import org.gbif.converters.parser.xml.model.LinkRecord;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.model.TypificationRecord;
import org.gbif.converters.parser.xml.parsing.RawXmlOccurrence;

import java.io.File;
import java.util.List;

import org.junit.Before;

public abstract class ParserTestCase {

  protected OccurrenceParser occurrenceParser;

  @Before
  public void setUp() {
    occurrenceParser = new OccurrenceParser();
  }

  protected List<RawOccurrenceRecord> setupRor(String fileName) {
    File response = new File(fileName);
    RawXmlOccurrence xmlRecord = occurrenceParser.parseResponseFileToRawXml(response).get(0);

    return XmlFragmentParser.parseRecord(xmlRecord);
  }

  protected void showIdentifiers(RawOccurrenceRecord ror) {
    System.out.println("got [" + ror.getIdentifierRecords().size() + "] identifier records");
    for (IdentifierRecord idRec : ror.getIdentifierRecords()) {
      System.out.println(
          "IdRec type ["
              + idRec.getIdentifierType()
              + "] identifier ["
              + idRec.getIdentifier()
              + "]");
    }
  }

  protected void showTaxons(RawOccurrenceRecord ror) {
    System.out.println("got taxons:");
    System.out.println("Kingdom: [" + ror.getKingdom() + "]");
    System.out.println("Phylum: [" + ror.getPhylum() + "]");
    System.out.println("Class: [" + ror.getKlass() + "]");
    System.out.println("Order: [" + ror.getOrder() + "]");
    System.out.println("Family: [" + ror.getFamily() + "]");
    System.out.println("Genus: [" + ror.getGenus() + "]");
    System.out.println("Species: [" + ror.getSpecies() + "]");
    System.out.println("Subspecies: [" + ror.getSubspecies() + "]");
  }

  protected void showTypifications(RawOccurrenceRecord ror) {
    System.out.println("got [" + ror.getTypificationRecords().size() + "] typification records");
    for (TypificationRecord typRec : ror.getTypificationRecords()) {
      System.out.println(typRec.debugDump());
    }
  }

  protected void showImages(RawOccurrenceRecord ror) {
    System.out.println("got [" + ror.getImageRecords().size() + "] image records");
    for (ImageRecord image : ror.getImageRecords()) {
      System.out.println(image.debugDump());
    }
  }

  protected void showLinks(RawOccurrenceRecord ror) {
    System.out.println("got [" + ror.getLinkRecords().size() + "] link records");
    for (LinkRecord link : ror.getLinkRecords()) {
      System.out.println(link.debugDump());
    }
  }
}
