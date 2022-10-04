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

import java.io.File;
import java.util.List;
import org.gbif.converters.parser.xml.parsing.RawXmlOccurrence;
import org.junit.Test;

public class BadXmlCharsParserTest extends ParserTestCase {

  @Test
  public void testParseInvalidXmlChar0xb() {
    String fileName = getClass().getResource("/responses/problematic/spanish_bad_xml.gz").getFile();
    List<RawXmlOccurrence> records = occurrenceParser.parseResponseFileToRawXml(new File(fileName));
    assertEquals(2, records.size());
  }
}
