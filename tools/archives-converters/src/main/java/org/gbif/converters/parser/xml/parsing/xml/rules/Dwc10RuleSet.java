package org.gbif.converters.parser.xml.parsing.xml.rules;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.RuleSet;
import org.gbif.api.vocabulary.OccurrenceSchemaType;

public class Dwc10RuleSet extends AbstractDwcRuleSet implements RuleSet {

  private static final String MAPPING_FILE = "mapping/indexMapping_dwc_1_0.properties";

  public Dwc10RuleSet() throws IOException {
    mappingProps = new Properties();
    URL url = ClassLoader.getSystemResource(MAPPING_FILE);
    mappingProps.load(url.openStream());
  }

  @Override
  public String getNamespaceURI() {
    return OccurrenceSchemaType.DWC_1_0.toString();
  }

  @Override
  public void addRuleInstances(Digester digester) {
    super.addRuleInstances(digester);

    addNonNullMethod(digester, "catalogueNumber", "setCatalogueNumber", 1);
    addNonNullParam(digester, "catalogueNumber", 0);

    addNonNullMethod(digester, "longitude", "setLongitude", 1);
    addNonNullParam(digester, "longitude", 0);

    addNonNullMethod(digester, "latitude", "setLatitude", 1);
    addNonNullParam(digester, "latitude", 0);

    addNonNullMethod(digester, "continentOrOcean", "setContinentOrOcean", 1);
    addNonNullParam(digester, "continentOrOcean", 0);

    addNonNullMethod(digester, "year", "setYear", 1);
    addNonNullParam(digester, "year", 0);

    addNonNullMethod(digester, "month", "setMonth", 1);
    addNonNullParam(digester, "month", 0);

    addNonNullMethod(digester, "day", "setDay", 1);
    addNonNullParam(digester, "day", 0);

    addNonNullMethod(digester, "yearIdentified", "setYearIdentified", 1);
    addNonNullParam(digester, "yearIdentified", 0);

    addNonNullMethod(digester, "monthIdentified", "setMonthIdentified", 1);
    addNonNullParam(digester, "monthIdentified", 0);

    addNonNullMethod(digester, "dayIdentified", "setDayIdentified", 1);
    addNonNullParam(digester, "dayIdentified", 0);
  }
}
