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
package org.gbif.converters.parser.xml.parsing.response.file;

import static org.junit.Assert.assertEquals;

import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ResponseSchemaDetectorTest {

  private ResponseSchemaDetector detector;

  @Before
  public void setUp() throws ParserConfigurationException {
    detector = new ResponseSchemaDetector();
    DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document doc = docBuilder.newDocument();
    Element root = doc.createElement("occurrence");
    doc.appendChild(root);
  }

  @Test
  public void testAbcd1() throws IOException {
    String xml =
        Resources.toString(
            Resources.getResource("response_schema/abcd1.xml"), StandardCharsets.UTF_8);
    OccurrenceSchemaType result = detector.detectSchema(xml);
    assertEquals(OccurrenceSchemaType.ABCD_1_2, result);
  }

  @Test
  public void testAbcd2() throws IOException {
    String xml =
        Resources.toString(
            Resources.getResource("response_schema/abcd2.xml"), StandardCharsets.UTF_8);
    OccurrenceSchemaType result = detector.detectSchema(xml);
    assertEquals(OccurrenceSchemaType.ABCD_2_0_6, result);
  }

  @Test
  public void testDwc1_0() throws IOException {
    String xml =
        Resources.toString(
            Resources.getResource("response_schema/dwc_1_0.xml"), StandardCharsets.UTF_8);
    OccurrenceSchemaType result = detector.detectSchema(xml);
    assertEquals(OccurrenceSchemaType.DWC_1_0, result);
  }

  @Test
  public void testDwc1_4() throws IOException {
    String xml =
        Resources.toString(
            Resources.getResource("response_schema/dwc_1_4.xml"), StandardCharsets.UTF_8);
    OccurrenceSchemaType result = detector.detectSchema(xml);
    assertEquals(OccurrenceSchemaType.DWC_1_4, result);
  }

  @Test
  public void testTapirDwc1_4() throws IOException {
    String xml =
        Resources.toString(
            Resources.getResource("response_schema/tapir_dwc_1_4_contains_unrecorded.xml"),
            StandardCharsets.UTF_8);
    OccurrenceSchemaType result = detector.detectSchema(xml);
    assertEquals(OccurrenceSchemaType.DWC_1_4, result);
  }

  @Test
  public void testTapirDwc1_4_2() throws IOException {
    String xml =
        Resources.toString(
            Resources.getResource("response_schema/tapir_dwc_1_4_s2.xml"), StandardCharsets.UTF_8);
    OccurrenceSchemaType result = detector.detectSchema(xml);
    assertEquals(OccurrenceSchemaType.DWC_1_4, result);
  }

  @Test
  public void testDwcManis() throws IOException {
    String xml =
        Resources.toString(
            Resources.getResource("response_schema/dwc_manis.xml"), StandardCharsets.UTF_8);
    OccurrenceSchemaType result = detector.detectSchema(xml);
    assertEquals(OccurrenceSchemaType.DWC_MANIS, result);
  }

  @Test
  public void testDwc2009() throws IOException {
    String xml =
        Resources.toString(
            Resources.getResource("response_schema/dwc_2009.xml"), StandardCharsets.UTF_8);
    OccurrenceSchemaType result = detector.detectSchema(xml);
    assertEquals(OccurrenceSchemaType.DWC_2009, result);
  }
}
