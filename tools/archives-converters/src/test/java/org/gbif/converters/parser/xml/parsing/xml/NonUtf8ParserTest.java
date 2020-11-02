package org.gbif.converters.parser.xml.parsing.xml;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;
import org.gbif.converters.parser.xml.parsing.RawXmlOccurrence;
import org.junit.Test;

public class NonUtf8ParserTest extends ParserTestCase {

  @Test
  public void testParseIso8859_1() {
    String fileName =
        getClass().getResource("/responses/problematic/dwc_manis_iso8859-1.gz").getFile();
    List<RawXmlOccurrence> records = occurrenceParser.parseResponseFileToRawXml(new File(fileName));
    assertEquals(900, records.size());
  }

  @Test
  public void testParseNoXmlDeclaration() {
    String fileName =
        getClass().getResource("/responses/problematic/dwc_manis_no_encoding.gz").getFile();

    List<RawXmlOccurrence> records = occurrenceParser.parseResponseFileToRawXml(new File(fileName));
    assertEquals(900, records.size());
  }

  @Test
  public void testParseUtf8WrongEncodingGiven() {
    // file is really iso8859-1 but claims to be utf8
    String fileName =
        getClass().getResource("/responses/problematic/dwc_10_utf8_badcase.gz").getFile();
    List<RawXmlOccurrence> records = occurrenceParser.parseResponseFileToRawXml(new File(fileName));
    assertEquals(21, records.size());
  }
}
