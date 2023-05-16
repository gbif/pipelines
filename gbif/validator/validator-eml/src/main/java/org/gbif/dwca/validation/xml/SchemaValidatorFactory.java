package org.gbif.dwca.validation.xml;

import java.io.StringReader;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwca.validation.XmlSchemaValidator;
import org.gbif.validator.api.EvaluationType;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SchemaValidatorFactory {

  private final Map<String, Schema> cache = new HashMap<>();

  @SneakyThrows
  public SchemaValidatorFactory(String... preLoadedSchemaLocations) {
    for (String schema : preLoadedSchemaLocations) {
      load(new URI(schema));
    }
  }

  @SneakyThrows
  public Schema load(URI schema) {
    log.info("Loading xml schema from {}", schema);
    // define the type of schema - we use W3C:
    // resolve validation driver:
    SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    // create schema by reading it from gbif online resources:
    Schema xmlSchema = null;
    try {
      xmlSchema = factory.newSchema(schema.toURL());
    } catch (SAXException ex) {
      log.warn(ex.getMessage(), ex);
    }
    cache.put(schema.toString(), xmlSchema);
    return xmlSchema;
  }

  @SneakyThrows
  public XmlSchemaValidator newValidator(String schemaUrl) {
    return Optional.ofNullable(schemaUrl)
        .map(schema -> Optional.ofNullable(cache.get(schema)).orElse(load(URI.create(schema))))
        .map(val -> XmlSchemaValidatorImpl.builder().schema(val).build())
        .orElseThrow(() -> new IllegalArgumentException(schemaUrl + "XML schema not supported"));
  }

  @SneakyThrows
  public XmlSchemaValidator newValidatorFromDocument(String xmlDocument) {
    return Optional.ofNullable(getSchema(xmlDocument))
        .map(this::newValidator)
        .orElseThrow(() -> new IllegalArgumentException("schemaLocation not found in document"));
  }

  public List<IssueInfo> validate(String xmlDocument) {
    try {
      return newValidatorFromDocument(xmlDocument).validate(xmlDocument);
    } catch (Exception ex) {
      return Collections.singletonList(
          IssueInfo.create(
              EvaluationType.EML_GBIF_SCHEMA, "EML document", ex.getLocalizedMessage()));
    }
  }

  /**
   * Extracts the schemaLocation from a xml document. The schema location it is expected to be as an
   * attribute of the first element in the XML document. For example:
   *
   * <pre>
   *   <?xml version="1.0" encoding="utf-8"?>
   * <eml:eml xmlns:eml="eml://ecoinformatics.org/eml-2.1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
   *          xsi:schemaLocation="eml://ecoinformatics.org/eml-2.1.1 http://rs.gbif.org/schema/eml-gbif-profile/1.1/eml.xsd"
   *          ...
   * </pre>
   */
  @SneakyThrows
  private String getSchema(String xmlDocument) {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    dbf.setValidating(false);
    DocumentBuilder db = dbf.newDocumentBuilder();
    Document doc = db.parse(new InputSource(new StringReader(xmlDocument)));
    if (doc.hasChildNodes()) {
      Node node = doc.getChildNodes().item(0).getAttributes().getNamedItem("xsi:schemaLocation");
      if (node != null) {
        String[] schemaLocation = node.getTextContent().split(" ", -1);
        if (schemaLocation.length == 2) {
          return schemaLocation[1];
        }
      }
    }
    return null;
  }
}
