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

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.converters.parser.xml.constants.ResponseElementEnum;
import org.gbif.converters.parser.xml.parsing.RawXmlOccurrence;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

@Slf4j
public class ParsedSearchResponse {

  private List<RawXmlOccurrence> records;
  private OccurrenceSchemaType schemaType;
  private Transformer transformer;
  private ResponseSchemaDetector schemaDetector;
  private Map<ResponseElementEnum, String> responseElements;
  private Node abcd1Header;
  private DocumentBuilder docBuilder;

  public ParsedSearchResponse() throws TransformerException, ParserConfigurationException {
    records = new ArrayList<>();

    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    transformerFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    transformer = transformerFactory.newTransformer();

    transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
    transformer.setOutputProperty(OutputKeys.METHOD, "xml");
    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
    transformer.setOutputProperty(OutputKeys.STANDALONE, "yes");
    schemaDetector = new ResponseSchemaDetector();

    DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
    documentFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    docBuilder = documentFactory.newDocumentBuilder();
  }

  public void addRecordAsXml(Node rawRecord) {
    Node workingNode = rawRecord;
    if (abcd1Header != null) {
      Document doc = docBuilder.newDocument();
      doc.adoptNode(rawRecord);
      doc.adoptNode(abcd1Header);
      Element root = doc.createElement("occurrence");
      Element dataSource = doc.createElement("DataSource");
      dataSource.appendChild(abcd1Header);
      root.appendChild(dataSource);
      root.appendChild(rawRecord);
      workingNode = root;
    }

    String xml = nodeToString(workingNode);

    log.debug("Serialized record: [{}]", xml);
    checkSchema(xml);
    if (responseElements != null) {
      RawXmlOccurrence record = new RawXmlOccurrence();
      record.setSchemaType(schemaType);

      populateRecordCodes(workingNode, record);

      record.setXml(xml);
      records.add(record);
    }
  }

  private void checkSchema(String xml) {
    if (schemaType == null) {
      schemaType = schemaDetector.detectSchema(xml);
      if (schemaType != null) {
        log.debug("Setting schema to [{}]", schemaType);
        responseElements = schemaDetector.getResponseElements(schemaType);
      }
    }
  }

  /**
   * Recursively traverse the node, returning the text value of the first node that has a name
   * matching targetElement. If node isn't found, returns null. Saves constructing elaborate,
   * namespace aware machinery for quick traverse of typically small data (a single occurrence
   * record).
   *
   * @param node the parsed xml to traverse
   * @param targetElement the name of the node to find
   * @return the text value of the target node
   */
  private String fakeXPath(Node node, String targetElement) {
    if (node.getNodeName().equals(targetElement)) {
      return node.getTextContent();
    } else if (node.hasChildNodes()) {
      NodeList nodeList = node.getChildNodes();
      for (int i = 0; i < nodeList.getLength(); i++) {
        Node child = nodeList.item(i);
        String result = fakeXPath(child, targetElement);
        if (result != null) {
          return result;
        }
      }
      return null;
    } else {
      return null;
    }
  }

  public Node getAbcd1Header() {
    return abcd1Header;
  }

  public List<RawXmlOccurrence> getRecords() {
    return records;
  }

  public OccurrenceSchemaType getSchemaType() {
    return schemaType;
  }

  private String nodeToString(Node node) {
    StringWriter sw = new StringWriter();
    try {
      DOMSource source = new DOMSource(node);
      transformer.transform(source, new StreamResult(sw));
    } catch (TransformerException e) {
      log.warn("Failed to transform node to string", e);
    }

    String result = sw.toString();
    // now strip out all namespacing
    result = result.replaceAll("[\\s]xmlns[:[a-zA-Z0-9]*]*=\".*?\"", "");
    result = result.replaceAll("ns0:", "");

    return result;
  }

  private void populateRecordCodes(Node node, RawXmlOccurrence record) {
    // the null checks are to guard against overwriting something already set on the record
    String instCode = fakeXPath(node, responseElements.get(ResponseElementEnum.INSTITUTION_CODE));
    if (instCode != null) {
      record.setInstitutionCode(instCode);
    }

    String collectionCode =
        fakeXPath(node, responseElements.get(ResponseElementEnum.COLLECTION_CODE));
    if (collectionCode != null) {
      record.setCollectionCode(collectionCode);
    }

    String catNum = fakeXPath(node, responseElements.get(ResponseElementEnum.CATALOG_NUMBER));
    if (catNum != null) {
      record.setCatalogNumber(catNum);
    }
  }

  public void setAbcd1Header(Node abcd1Header) {
    this.abcd1Header = abcd1Header;
  }

  public void setRecords(List<RawXmlOccurrence> records) {
    this.records = records;
  }

  public void setSchemaType(OccurrenceSchemaType schemaType) {
    this.schemaType = schemaType;
  }
}
