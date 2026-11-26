package org.gbif.dwca.validation.xml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * XML parsers that don't resolve external entities or DTDs. The JDK implementations are used, so
 * other providers on the classpath (e.g. Apache Xerces, which ignores the JAXP access properties)
 * can't weaken them.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class XmlSecurity {

  public static final String DISALLOW_DOCTYPE =
      "http://apache.org/xml/features/disallow-doctype-decl";

  /** DocumentBuilderFactory rejecting documents with a DOCTYPE. */
  public static DocumentBuilderFactory documentBuilderFactory()
      throws ParserConfigurationException {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newDefaultInstance();
    dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    dbf.setFeature(DISALLOW_DOCTYPE, true);
    dbf.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
    dbf.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
    dbf.setXIncludeAware(false);
    dbf.setExpandEntityReferences(false);
    dbf.setNamespaceAware(true);
    dbf.setValidating(false);
    return dbf;
  }

  /** Namespace aware XMLReader rejecting documents with a DOCTYPE. */
  public static XMLReader xmlReader() throws ParserConfigurationException, SAXException {
    SAXParserFactory spf = SAXParserFactory.newDefaultInstance();
    spf.setNamespaceAware(true);
    spf.setValidating(false);
    spf.setXIncludeAware(false);
    spf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    spf.setFeature(DISALLOW_DOCTYPE, true);
    XMLReader reader = spf.newSAXParser().getXMLReader();
    reader.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
    reader.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
    return reader;
  }

  /**
   * SchemaFactory for trusted schemas: they may import other schemas over http(s), never from local
   * files or jars.
   */
  public static SchemaFactory schemaFactory() throws SAXException {
    SchemaFactory factory = SchemaFactory.newDefaultInstance();
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    factory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "http,https");
    factory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "http,https");
    return factory;
  }

  /** Stops a validator from fetching anything referenced by the validated document. */
  public static Validator harden(Validator validator) throws SAXException {
    validator.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
    validator.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
    return validator;
  }

  /**
   * Asserts the document is well-formed and has no DOCTYPE, so it can be given to parsers that
   * resolve external entities.
   */
  public static void assertNoDoctype(byte[] document) throws SAXException, IOException {
    try {
      XMLReader reader = xmlReader();
      reader.setContentHandler(new DefaultHandler());
      reader.parse(new InputSource(new ByteArrayInputStream(document)));
    } catch (ParserConfigurationException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** As {@link #assertNoDoctype(byte[])} for a string. */
  public static void assertNoDoctype(String document) throws SAXException, IOException {
    try {
      XMLReader reader = xmlReader();
      reader.setContentHandler(new DefaultHandler());
      reader.parse(new InputSource(new StringReader(document)));
    } catch (ParserConfigurationException ex) {
      throw new IllegalStateException(ex);
    }
  }
}
