package org.gbif.converters.parser.xml.parsing.xml.rules;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.RuleSet;
import org.apache.logging.log4j.util.BiConsumer;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.converters.parser.xml.constants.PrioritizedPropertyNameEnum;

public class DwcManisRuleSet extends AbstractDwcRuleSet implements RuleSet {

  private static final String MAPPING_FILE = "mapping/indexMapping_manis_dwc.properties";

  public DwcManisRuleSet() throws IOException {
    mappingProps = new Properties();
    URL url = ClassLoader.getSystemResource(MAPPING_FILE);
    mappingProps.load(url.openStream());
  }

  @Override
  public String getNamespaceURI() {
    return OccurrenceSchemaType.DWC_MANIS.toString();
  }

  @Override
  public void addRuleInstances(Digester digester) {
    super.addRuleInstances(digester);

    BiConsumer<String, String> addFn =
        (property, methodName) -> {
          addNonNullMethod(digester, property, methodName, 1);
          addNonNullParam(digester, property, 0);
        };

    addFn.accept("continentOrOcean", "setContinentOrOcean");
    addFn.accept("year", "setYear");
    addFn.accept("month", "setMonth");
    addFn.accept("day", "setDay");
    addFn.accept("yearIdentified", "setYearIdentified");
    addFn.accept("monthIdentified", "setMonthIdentified");
    addFn.accept("dayIdentified", "setDayIdentified");
    addFn.accept("latitudeDecimal", "setDecimalLatitude");
    addFn.accept("longitudeDecimal", "setDecimalLongitude");
    addFn.accept("verbatimLatitude", "setVerbatimLatitude");
    addFn.accept("verbatimLongitude", "setVerbatimLongitude");

    addNonNullPrioritizedProperty(
        digester, "catalogueNumber", PrioritizedPropertyNameEnum.CATALOGUE_NUMBER, 2);
  }
}
