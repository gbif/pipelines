package org.gbif.converters.parser.xml.parsing.xml.rules;

import java.util.function.BiConsumer;
import org.apache.commons.digester.Digester;
import org.gbif.converters.parser.xml.model.Collector;

public abstract class AbstractDwcRuleSet extends AbstractRuleSet {

  @Override
  public void addRuleInstances(Digester digester) {
    super.addRuleInstances(digester);

    BiConsumer<String, String> addFn =
        (property, methodName) -> {
          addNonNullMethod(digester, property, methodName, 1);
          addNonNullParam(digester, property, 0);
        };

    addFn.accept("scientificName", "setScientificName");
    addFn.accept("author", "setAuthor");
    addFn.accept("kingdom", "setKingdom");
    addFn.accept("phylum", "setPhylum");
    addFn.accept("klass", "setKlass");
    addFn.accept("order", "setOrder");
    addFn.accept("family", "setFamily");
    addFn.accept("genus", "setGenus");
    addFn.accept("species", "setSpecies");
    addFn.accept("subspecies", "setSubspecies");
    addFn.accept("country", "setCountry");
    addFn.accept("stateOrProvince", "setStateOrProvince");

    addFn.accept("county", "setCounty");
    addFn.accept("locality", "setLocality");
    addFn.accept("identifierName", "setIdentifierName");

    String ptrn = mappingProps.getProperty("collectorName");
    if (ptrn != null) {
      ptrn = ptrn.trim();
      digester.addObjectCreate(ptrn, Collector.class);
      digester.addSetNext(ptrn, "addCollectorName");

      addFn.accept("collectorName", "setName");
    }
  }
}
