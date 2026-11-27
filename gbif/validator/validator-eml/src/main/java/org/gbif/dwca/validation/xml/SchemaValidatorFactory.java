package org.gbif.dwca.validation.xml;

import java.io.StringReader;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.validation.Schema;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwca.validation.XmlSchemaValidator;
import org.gbif.validator.api.EvaluationType;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Loads and caches XML schemas. Only preloaded schemas and http(s) schemas of trusted hosts are
 * loaded, so a document can't make the validator fetch arbitrary URLs.
 */
@Slf4j
public class SchemaValidatorFactory {

  /** Hosts whose schemas can be loaded by default. */
  public static final Set<String> DEFAULT_TRUSTED_HOSTS = Set.of("rs.gbif.org");

  private static final Set<String> SCHEMES = Set.of("http", "https");

  private final Map<String, Schema> cache = new ConcurrentHashMap<>();

  private final Set<String> trustedHosts;

  private final Set<String> preloaded;

  /** Preloads the schemas, schemas of {@link #DEFAULT_TRUSTED_HOSTS} can be loaded later. */
  public SchemaValidatorFactory(String... preLoadedSchemaLocations) {
    this(DEFAULT_TRUSTED_HOSTS, preLoadedSchemaLocations);
  }

  @SneakyThrows
  public SchemaValidatorFactory(Set<String> trustedHosts, String... preLoadedSchemaLocations) {
    this.trustedHosts =
        trustedHosts.stream().map(h -> h.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
    this.preloaded = Stream.of(preLoadedSchemaLocations).collect(Collectors.toUnmodifiableSet());
    for (String schema : preLoadedSchemaLocations) {
      load(new URI(schema));
    }
  }

  /** True if the schema is preloaded or an http(s) URL of a trusted host. */
  public boolean isAllowed(String schemaUrl) {
    if (schemaUrl == null) {
      return false;
    }
    if (preloaded.contains(schemaUrl)) {
      return true;
    }
    try {
      URI uri = URI.create(schemaUrl);
      return uri.getScheme() != null
          && SCHEMES.contains(uri.getScheme().toLowerCase(Locale.ROOT))
          && uri.getHost() != null
          && trustedHosts.contains(uri.getHost().toLowerCase(Locale.ROOT));
    } catch (IllegalArgumentException ex) {
      return false;
    }
  }

  /** Loads a schema and caches it, null if it can't be loaded. */
  @SneakyThrows
  public Schema load(URI schema) {
    String key = schema.toString();
    if (!isAllowed(key)) {
      throw new IllegalArgumentException("XML schema not supported: " + key);
    }
    log.info("Loading xml schema from {}", schema);
    try {
      Schema xmlSchema = XmlSecurity.schemaFactory().newSchema(schema.toURL());
      cache.put(key, xmlSchema);
      log.info("Put xml schema from {} to cache", schema);
      return xmlSchema;
    } catch (SAXException ex) {
      log.warn(ex.getMessage(), ex);
      return null;
    }
  }

  public XmlSchemaValidator newValidator(String schemaUrl) {
    if (!isAllowed(schemaUrl)) {
      throw new IllegalArgumentException("XML schema not supported: " + schemaUrl);
    }
    return Optional.ofNullable(cache.get(schemaUrl))
        .or(() -> Optional.ofNullable(load(URI.create(schemaUrl))))
        .map(val -> XmlSchemaValidatorImpl.builder().schema(val).build())
        .orElseThrow(() -> new IllegalArgumentException("XML schema not supported: " + schemaUrl));
  }

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
    DocumentBuilder db = XmlSecurity.documentBuilderFactory().newDocumentBuilder();
    Document doc = db.parse(new InputSource(new StringReader(xmlDocument)));
    Node root = doc.getDocumentElement();
    if (root != null) {
      Node node =
          root.getAttributes()
              .getNamedItemNS("http://www.w3.org/2001/XMLSchema-instance", "schemaLocation");
      if (node != null) {
        String[] schemaLocation = node.getTextContent().trim().split("\\s+", -1);
        if (schemaLocation.length == 2) {
          return schemaLocation[1];
        }
      }
    }
    return null;
  }
}
