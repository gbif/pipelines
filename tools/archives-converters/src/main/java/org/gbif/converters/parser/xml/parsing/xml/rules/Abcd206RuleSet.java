package org.gbif.converters.parser.xml.parsing.xml.rules;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import org.apache.commons.digester.Digester;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.converters.parser.xml.constants.PrioritizedPropertyNameEnum;
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

    // abcd simple fields
    addNonNullMethod(digester, "catalogueNumber", "setCatalogueNumber", 1);
    addNonNullParam(digester, "catalogueNumber", 0);

    addNonNullMethod(digester, "altitudePrecision", "setAltitudePrecision", 1);
    addNonNullParam(digester, "altitudePrecision", 0);

    addNonNullMethod(digester, "depthPrecision", "setDepthPrecision", 1);
    addNonNullParam(digester, "depthPrecision", 0);

    addNonNullMethod(digester, "locality", "setLocality", 1);
    addNonNullParam(digester, "locality", 0);

    addNonNullMethod(digester, "geodeticDatum", "setGeodeticDatum", 1);
    addNonNullParam(digester, "geodeticDatum", 0);

    addNonNullMethod(digester, "collectorsFieldNumber", "setCollectorsFieldNumber", 1);
    addNonNullParam(digester, "collectorsFieldNumber", 0);

    addNonNullPrioritizedProperty(digester, "country", PrioritizedPropertyNameEnum.COUNTRY, 3);
    addNonNullPrioritizedProperty(
        digester, "collectorName", PrioritizedPropertyNameEnum.COLLECTOR_NAME, 3);
    addNonNullPrioritizedProperty(digester, "latitude", PrioritizedPropertyNameEnum.LATITUDE, 2);
    addNonNullPrioritizedProperty(digester, "longitude", PrioritizedPropertyNameEnum.LONGITUDE, 2);
    addNonNullPrioritizedProperty(
        digester, "dateCollected", PrioritizedPropertyNameEnum.DATE_COLLECTED, 3);

    // possibly many identifications
    String pattern = mappingProps.getProperty("idElement");
    if (pattern != null) {
      pattern = pattern.trim();
      digester.addObjectCreate(pattern, Identification.class);
      digester.addSetNext(pattern, "addIdentification");
      addNonNullMethod(digester, "idPreferred", "setPreferredAsString", 1);
      addNonNullParam(digester, "idPreferred", 0);

      addNonNullMethod(digester, "idGenus", "setGenus", 1);
      addNonNullParam(digester, "idGenus", 0);

      addNonNullMethod(digester, "idGenus", "setGenus", 1);
      addNonNullParam(digester, "idGenus", 0);

      addNonNullMethod(digester, "idScientificName", "setScientificName", 1);
      addNonNullParam(digester, "idScientificName", 0);

      addNonNullPrioritizedProperty(
          digester, "idDateIdentified", PrioritizedPropertyNameEnum.ID_DATE_IDENTIFIED, 2);
      addNonNullPrioritizedProperty(
          digester, "idIdentifierName", PrioritizedPropertyNameEnum.ID_IDENTIFIER_NAME, 2);

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

      addNonNullMethod(digester, "imageType", "setRawImageType", 1);
      addNonNullParam(digester, "imageType", 0);

      addNonNullMethod(digester, "imageDescription", "setDescription", 1);
      addNonNullParam(digester, "imageDescription", 0);

      addNonNullMethod(digester, "imageUrl", "setUrl", 1);
      addNonNullParam(digester, "imageUrl", 0);

      addNonNullMethod(digester, "imagePageUrl", "setPageUrl", 1);
      addNonNullParam(digester, "imagePageUrl", 0);

      addNonNullPrioritizedProperty(
          digester, "imageRights", PrioritizedPropertyNameEnum.IMAGE_RIGHTS, 2);
    }

    // possibly many links
    pattern = mappingProps.getProperty("linkElement");
    if (pattern != null) {
      pattern = pattern.trim();
      digester.addObjectCreate(pattern, LinkRecord.class);
      digester.addSetNext(pattern, "addLink");

      addNonNullMethod(digester, "linkUrl", "setUrl", 1);
      addNonNullParam(digester, "linkUrl", 0);
    }
  }
}
