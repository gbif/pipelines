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
public class ExtendedRecordCustomBuilder {

  private String kingdom;
  private String phylum;
  private String clazz;
  private String order;
  private String family;
  private String genus;
  private String rank;
  private String name;
  private String authorship;
  private String id;
  private String specificEpithet;
  private String infraspecificEpithet;

  public ExtendedRecordCustomBuilder kingdom(String kingdom) {
    this.kingdom = kingdom;
    return this;
  }

  public ExtendedRecordCustomBuilder phylum(String phylum) {
    this.phylum = phylum;
    return this;
  }

  public ExtendedRecordCustomBuilder clazz(String clazz) {
    this.clazz = clazz;
    return this;
  }

  public ExtendedRecordCustomBuilder order(String order) {
    this.order = order;
    return this;
  }

  public ExtendedRecordCustomBuilder family(String family) {
    this.family = family;
    return this;
  }

  public ExtendedRecordCustomBuilder genus(String genus) {
    this.genus = genus;
    return this;
  }

  public ExtendedRecordCustomBuilder rank(String rank) {
    this.rank = rank;
    return this;
  }

  public ExtendedRecordCustomBuilder name(String name) {
    this.name = name;
    return this;
  }

  public ExtendedRecordCustomBuilder authorship(String authorship) {
    this.authorship = authorship;
    return this;
  }

  public ExtendedRecordCustomBuilder specificEpithet(String specificEpithet) {
    this.specificEpithet = specificEpithet;
    return this;
  }

  public ExtendedRecordCustomBuilder infraspecificEpithet(String infraspecificEpithet) {
    this.infraspecificEpithet = infraspecificEpithet;
    return this;
  }

  public ExtendedRecordCustomBuilder id(String id) {
    this.id = id;
    return this;
  }

  public ExtendedRecord build() {
    Map<String, String> terms = new HashMap<>();

    addToTerms(terms, DwcTerm.kingdom.qualifiedName(), kingdom);
    addToTerms(terms, DwcTerm.genus.qualifiedName(), genus);
    addToTerms(terms, DwcTerm.scientificName.qualifiedName(), name);
    addToTerms(terms, DwcTerm.scientificNameAuthorship.qualifiedName(), authorship);
    addToTerms(terms, DwcTerm.taxonRank.qualifiedName(), rank);
    addToTerms(terms, DwcTerm.phylum.qualifiedName(), phylum);
    addToTerms(terms, DwcTerm.class_.qualifiedName(), clazz);
    addToTerms(terms, DwcTerm.order.qualifiedName(), order);
    addToTerms(terms, DwcTerm.family.qualifiedName(), family);
    addToTerms(terms, DwcTerm.specificEpithet.qualifiedName(), specificEpithet);
    addToTerms(terms, DwcTerm.infraspecificEpithet.qualifiedName(), infraspecificEpithet);

    return ExtendedRecord.newBuilder().setId(id).setCoreTerms(terms).build();
  }

  private void addToTerms(Map<String, String> terms, String term, String value) {
    if (value != null) {
      terms.put(term, value);
    }
  }

}