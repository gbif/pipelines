package org.gbif.converters.parser.xml.parsing.xml.rules;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.function.BiConsumer;
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

    BiConsumer<String, String> addFn =
        (property, methodName) -> {
          addNonNullMethod(digester, property, methodName, 1);
          addNonNullParam(digester, property, 0);
        };

    addFn.accept("catalogueNumber", "setCatalogueNumber");
    addFn.accept("day", "setDay");
    addFn.accept("month", "setMonth");
    addFn.accept("year", "setYear");
    addFn.accept("dateIdentified", "setDateIdentified");
    addFn.accept("latitudeDecimal", "setDecimalLatitude");
    addFn.accept("longitudeDecimal", "setDecimalLongitude");
    addFn.accept("verbatimLatitude", "setVerbatimLatitude");
    addFn.accept("verbatimLongitude", "setVerbatimLongitude");

    addNonNullPrioritizedProperty(
        digester, "dateCollected", PrioritizedPropertyNameEnum.DATE_COLLECTED, 2);
    addNonNullPrioritizedProperty(
        digester, "continentOrOcean", PrioritizedPropertyNameEnum.CONTINENT_OR_OCEAN, 2);
  }
}
