package org.gbif.converters.parser.xml.parsing.xml.rules;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.RuleSet;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.converters.parser.xml.constants.PrioritizedPropertyNameEnum;
import org.gbif.converters.parser.xml.model.ImageRecord;
import org.gbif.converters.parser.xml.model.LinkRecord;

public class Dwc14RuleSet extends AbstractDwcRuleSet implements RuleSet {

  private static final String MAPPING_FILE = "mapping/indexMapping_dwc_1_4.properties";

  public Dwc14RuleSet() throws IOException {
    mappingProps = new Properties();
    URL url = ClassLoader.getSystemResource(MAPPING_FILE);
    mappingProps.load(url.openStream());
  }

  @Override
  public String getNamespaceURI() {
    return OccurrenceSchemaType.DWC_1_4.toString();
  }

  @Override
  public void addRuleInstances(Digester digester) {
    super.addRuleInstances(digester);

    addNonNullMethod(digester, "dateIdentified", "setDateIdentified", 1);
    addNonNullParam(digester, "dateIdentified", 0);

    addNonNullPrioritizedProperty(
        digester, "dateCollected", PrioritizedPropertyNameEnum.DATE_COLLECTED, 2);
    addNonNullPrioritizedProperty(digester, "latitude", PrioritizedPropertyNameEnum.LATITUDE, 2);
    addNonNullPrioritizedProperty(digester, "longitude", PrioritizedPropertyNameEnum.LONGITUDE, 2);
    addNonNullPrioritizedProperty(
        digester, "continentOrOcean", PrioritizedPropertyNameEnum.CONTINENT_OR_OCEAN, 2);
    addNonNullPrioritizedProperty(
        digester, "catalogueNumber", PrioritizedPropertyNameEnum.CATALOGUE_NUMBER, 2);

    // possibly many images
    String pattern = mappingProps.getProperty("imageElement");
    if (pattern != null) {
      pattern = pattern.trim();
      digester.addObjectCreate(pattern, ImageRecord.class);
      digester.addSetNext(pattern, "addImage");

      addNonNullMethod(digester, "imageUrl", "setUrl", 1);
      addNonNullParam(digester, "imageUrl", 0);
    }

    // 2 explicit links possible
    pattern = mappingProps.getProperty("linkElement0");
    if (pattern != null) {
      pattern = pattern.trim();
      digester.addObjectCreate(pattern, LinkRecord.class);
      digester.addSetNext(pattern, "addLink");

      digester.addRule(pattern, new SetLiteralRule("setLinkType", 0));

      addNonNullMethod(digester, "linkUrlType0", "setUrl", 1);
      addNonNullParam(digester, "linkUrlType0", 0);
    }

    pattern = mappingProps.getProperty("linkElement1");
    if (pattern != null) {
      pattern = pattern.trim();
      digester.addObjectCreate(pattern, LinkRecord.class);
      digester.addSetNext(pattern, "addLink");

      digester.addRule(pattern, new SetLiteralRule("setLinkType", 1));

      addNonNullMethod(digester, "linkUrlType1", "setUrl", 1);
      addNonNullParam(digester, "linkUrlType1", 0);
    }
  }
}
