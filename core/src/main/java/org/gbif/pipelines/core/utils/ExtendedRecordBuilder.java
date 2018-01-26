package org.gbif.pipelines.core.utils;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder for a {@link ExtendedRecord}.
 * <p>
 * Recommended for testing purposes.
 */
public class ExtendedRecordBuilder {

  private String kingdom;
  private String phylum;
  private String clazz;
  private String order;
  private String family;
  private String genus;
  private String rank;
  private String name;
  private String authorship;

  public ExtendedRecordBuilder kingdom(String kingdom) {
    this.kingdom = kingdom;
    return this;
  }

  public ExtendedRecordBuilder phylum(String phylum) {
    this.phylum = phylum;
    return this;
  }

  public ExtendedRecordBuilder clazz(String clazz) {
    this.clazz = clazz;
    return this;
  }

  public ExtendedRecordBuilder order(String order) {
    this.order = order;
    return this;
  }

  public ExtendedRecordBuilder family(String family) {
    this.family = family;
    return this;
  }

  public ExtendedRecordBuilder genus(String genus) {
    this.genus = genus;
    return this;
  }

  public ExtendedRecordBuilder rank(String rank) {
    this.rank = rank;
    return this;
  }

  public ExtendedRecordBuilder name(String name) {
    this.name = name;
    return this;
  }

  public ExtendedRecordBuilder authorship(String authorship) {
    this.authorship = authorship;
    return this;
  }

  public ExtendedRecord build() {
    ExtendedRecord record = new ExtendedRecord();

    Map<CharSequence, CharSequence> terms = new HashMap<>();
    terms.put(DwcTerm.kingdom.qualifiedName(), kingdom);
    terms.put(DwcTerm.genus.qualifiedName(), genus);
    terms.put(DwcTerm.scientificName.qualifiedName(), name);
    terms.put(DwcTerm.scientificNameAuthorship.qualifiedName(), authorship);
    terms.put(DwcTerm.taxonRank.qualifiedName(), rank);
    terms.put(DwcTerm.phylum.qualifiedName(), phylum);
    terms.put(DwcTerm.class_.qualifiedName(), clazz);
    terms.put(DwcTerm.order.qualifiedName(), order);
    terms.put(DwcTerm.family.qualifiedName(), family);

    record.setCoreTerms(terms);

    return record;
  }

}