package org.gbif.converters.parser.xml.parsing.xml.rules;

import org.apache.commons.digester.Digester;

public abstract class AbstractDwcRuleSet extends AbstractRuleSet {

  @Override
  public void addRuleInstances(Digester digester) {
    super.addRuleInstances(digester);

    addNonNullMethod(digester, "scientificName", "setScientificName", 1);
    addNonNullParam(digester, "scientificName", 0);

    addNonNullMethod(digester, "author", "setAuthor", 1);
    addNonNullParam(digester, "author", 0);

    addNonNullMethod(digester, "kingdom", "setKingdom", 1);
    addNonNullParam(digester, "kingdom", 0);

    addNonNullMethod(digester, "phylum", "setPhylum", 1);
    addNonNullParam(digester, "phylum", 0);

    addNonNullMethod(digester, "klass", "setKlass", 1);
    addNonNullParam(digester, "klass", 0);

    addNonNullMethod(digester, "order", "setOrder", 1);
    addNonNullParam(digester, "order", 0);

    addNonNullMethod(digester, "family", "setFamily", 1);
    addNonNullParam(digester, "family", 0);

    addNonNullMethod(digester, "genus", "setGenus", 1);
    addNonNullParam(digester, "genus", 0);

    addNonNullMethod(digester, "species", "setSpecies", 1);
    addNonNullParam(digester, "species", 0);

    addNonNullMethod(digester, "subspecies", "setSubspecies", 1);
    addNonNullParam(digester, "subspecies", 0);

    addNonNullMethod(digester, "country", "setCountry", 1);
    addNonNullParam(digester, "country", 0);

    addNonNullMethod(digester, "stateOrProvince", "setStateOrProvince", 1);
    addNonNullParam(digester, "stateOrProvince", 0);

    addNonNullMethod(digester, "county", "setCounty", 1);
    addNonNullParam(digester, "county", 0);

    addNonNullMethod(digester, "locality", "setLocality", 1);
    addNonNullParam(digester, "locality", 0);

    addNonNullMethod(digester, "collectorName", "setCollectorName", 1);
    addNonNullParam(digester, "collectorName", 0);

    addNonNullMethod(digester, "identifierName", "setIdentifierName", 1);
    addNonNullParam(digester, "identifierName", 0);
  }
}
