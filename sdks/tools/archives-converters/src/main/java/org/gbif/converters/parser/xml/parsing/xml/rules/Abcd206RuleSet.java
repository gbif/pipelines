package org.gbif.converters.parser.xml.parsing.xml.rules;

import static org.gbif.converters.parser.xml.constants.PrioritizedPropertyNameEnum.*;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.commons.digester.Digester;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.converters.parser.xml.model.Collector;
import org.gbif.converters.parser.xml.model.Identification;
import org.gbif.converters.parser.xml.model.ImageRecord;
import org.gbif.converters.parser.xml.model.LinkRecord;

public class Abcd206RuleSet extends AbstractRuleSet {

  private static final String MAPPING_FILE = "mapping/indexMapping_abcd_2_0_6.properties";

  public Abcd206RuleSet() throws IOException {
    mappingProps = new Properties();
    URL url = ClassLoader.getSystemResource(MAPPING_FILE);
    mappingProps.load(url.openStream());
  }

  @Override
  public String getNamespaceURI() {
    // TODO: should this be the real xml namespace and not just a flat string?
    return OccurrenceSchemaType.ABCD_2_0_6.toString();
  }

  @Override
  public void addRuleInstances(Digester digester) {
    super.addRuleInstances(digester);

    BiConsumer<String, String> addFn =
        (property, methodName) -> {
          addNonNullMethod(digester, property, methodName, 1);
          addNonNullParam(digester, property, 0);
        };

    // abcd simple fields
    addFn.accept("catalogueNumber", "setCatalogueNumber");
    addFn.accept("altitudePrecision", "setAltitudePrecision");
    addFn.accept("depthPrecision", "setDepthPrecision");
    addFn.accept("locality", "setLocality");
    addFn.accept("collectorsFieldNumber", "setCollectorsFieldNumber");
    addFn.accept("countryCode", "setCountryCode");
    addFn.accept("latitudeDecimal", "setDecimalLatitude");
    addFn.accept("longitudeDecimal", "setDecimalLongitude");
    addFn.accept("verbatimLatitude", "setVerbatimLatitude");
    addFn.accept("verbatimLongitude", "setVerbatimLongitude");
    addFn.accept("footprintWKT", "setFootprintWKT");
    addFn.accept("occurrenceRemarks", "setOccurrenceRemarks");
    addFn.accept("modified", "setModified");
    addFn.accept("preparations", "setPreparations");

    addNonNullMethod(digester, "recordedByID", "addRecordedByID", 0);
    addNonNullMethod(digester, "associatedSequences", "addAssociatedSequence", 0);

    addNonNullPrioritizedProperty(digester, "country", COUNTRY, 2);
    addNonNullPrioritizedProperty(digester, "geodeticDatum", GEODETIC_DATUM, 2);
    addNonNullPrioritizedProperty(digester, "dateCollected", DATE_COLLECTED, 3);

    // possibly many identifications
    String pattern = mappingProps.getProperty("idElement");
    if (pattern != null) {
      pattern = pattern.trim();
      digester.addObjectCreate(pattern, Identification.class);
      digester.addSetNext(pattern, "addIdentification");

      addFn.accept("idPreferred", "setPreferredAsString");
      addFn.accept("idGenus", "setGenus");
      addFn.accept("idScientificName", "setScientificName");
      addFn.accept("scientificNameID", "setScientificNameID");

      addNonNullMethod(digester, "identifiedByID", "addIdentifiedByID", 0);

      addNonNullPrioritizedProperty(digester, "idDateIdentified", ID_DATE_IDENTIFIED, 2);
      addNonNullPrioritizedProperty(digester, "idIdentifierName", ID_IDENTIFIER_NAME, 2);

      // possibly many higher taxons for every identification
      addNonNullMethod(digester, "higherTaxonElement", "addHigherTaxon", 2);
      addNonNullParam(digester, "higherTaxonRank", 0);
      addNonNullParam(digester, "higherTaxonName", 1);
    }

    // possibly many images
    pattern = mappingProps.getProperty("imageElement");
    if (pattern != null) {
      pattern = pattern.trim();
      digester.addObjectCreate(pattern, ImageRecord.class);
      digester.addSetNext(pattern, "addImage");

      addFn.accept("imageType", "setRawImageType");
      addFn.accept("imageDescription", "setDescription");
      addFn.accept("imageUrl", "setUrl");
      addFn.accept("imagePageUrl", "setPageUrl");

      addNonNullPrioritizedProperty(digester, "imageRights", IMAGE_RIGHTS, 2);
    }

    // possibly many links
    pattern = mappingProps.getProperty("linkElement");
    if (pattern != null) {
      pattern = pattern.trim();
      digester.addObjectCreate(pattern, LinkRecord.class);
      digester.addSetNext(pattern, "addLink");

      addFn.accept("linkUrl", "setUrl");
    }

    // possible many collectors
    Consumer<String> collectorFn =
        prop -> {
          String ptrn = mappingProps.getProperty(prop);
          if (ptrn != null) {
            ptrn = ptrn.trim();
            digester.addObjectCreate(ptrn, Collector.class);
            digester.addSetNext(ptrn, "addCollectorName");

            addFn.accept(prop, "setName");
          }
        };

    collectorFn.accept("collectorNameFullName");
    collectorFn.accept("collectorNameGatheringAgentsText");
    collectorFn.accept("collectorNameAgentText");
  }
}
