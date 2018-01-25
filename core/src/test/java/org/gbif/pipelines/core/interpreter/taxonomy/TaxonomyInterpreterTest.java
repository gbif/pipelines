package org.gbif.pipelines.core.interpreter.taxonomy;

import org.gbif.api.vocabulary.Rank;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

public class TaxonomyInterpreterTest {

  @Test
  @Ignore
  public void testAssembledAuthor() {

    TaxonomyInterpreter interpreter = new TaxonomyInterpreter();

    ExtendedRecord record = new ExtendedRecord();

    Map<CharSequence, CharSequence> terms = new HashMap<>();
    terms.put(DwcTerm.kingdom.qualifiedName(), "Animalia");
    terms.put(DwcTerm.genus.qualifiedName(), "Puma");
    terms.put(DwcTerm.scientificName.qualifiedName(), "Puma concolor");
    terms.put(DwcTerm.scientificNameAuthorship.qualifiedName(), "");
    terms.put(DwcTerm.taxonRank.qualifiedName(), Rank.SPECIES.name());

    record.setCoreTerms(terms);

    InterpretedTaxonomy interpretedTaxon = interpreter.interpretTaxonomyFields(record);

    interpretedTaxon.toString();


  }

}
