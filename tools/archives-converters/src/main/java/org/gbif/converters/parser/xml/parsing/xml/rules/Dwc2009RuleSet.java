package org.gbif.converters.parser.xml.parsing.xml.rules;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.RuleSet;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.converters.parser.xml.constants.PrioritizedPropertyNameEnum;

public class Dwc2009RuleSet extends AbstractDwcRuleSet implements RuleSet {

  private static final String MAPPING_FILE = "mapping/indexMapping_dwc_2009.properties";

  public Dwc2009RuleSet() throws IOException {
    mappingProps = new Properties();
    URL url = ClassLoader.getSystemResource(MAPPING_FILE);
    mappingProps.load(url.openStream());
  }

  @Override
  public String getNamespaceURI() {
    return OccurrenceSchemaType.DWC_2009.toString();
  }

  @Override
  public void addRuleInstances(Digester digester) {
    super.addRuleInstances(digester);

    addNonNullMethod(digester, "catalogueNumber", "setCatalogueNumber", 1);
    addNonNullParam(digester, "catalogueNumber", 0);

    addNonNullMethod(digester, "day", "setDay", 1);
    addNonNullParam(digester, "day", 0);

    addNonNullMethod(digester, "month", "setMonth", 1);
    addNonNullParam(digester, "month", 0);

    addNonNullMethod(digester, "year", "setYear", 1);
    addNonNullParam(digester, "year", 0);

    addNonNullMethod(digester, "dateIdentified", "setDateIdentified", 1);
    addNonNullParam(digester, "dateIdentified", 0);

    addNonNullPrioritizedProperty(
        digester, "dateCollected", PrioritizedPropertyNameEnum.DATE_COLLECTED, 2);
    addNonNullPrioritizedProperty(
        digester, "continentOrOcean", PrioritizedPropertyNameEnum.CONTINENT_OR_OCEAN, 2);
    addNonNullPrioritizedProperty(digester, "latitude", PrioritizedPropertyNameEnum.LATITUDE, 2);
    addNonNullPrioritizedProperty(digester, "longitude", PrioritizedPropertyNameEnum.LONGITUDE, 2);
  }
}
