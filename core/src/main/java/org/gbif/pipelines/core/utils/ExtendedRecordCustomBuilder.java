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
    terms.put(DwcTerm.kingdom.qualifiedName(), kingdom);
    terms.put(DwcTerm.genus.qualifiedName(), genus);
    terms.put(DwcTerm.scientificName.qualifiedName(), name);
    terms.put(DwcTerm.scientificNameAuthorship.qualifiedName(), authorship);
    terms.put(DwcTerm.taxonRank.qualifiedName(), rank);
    terms.put(DwcTerm.phylum.qualifiedName(), phylum);
    terms.put(DwcTerm.class_.qualifiedName(), clazz);
    terms.put(DwcTerm.order.qualifiedName(), order);
    terms.put(DwcTerm.family.qualifiedName(), family);
    terms.put(DwcTerm.specificEpithet.qualifiedName(), specificEpithet);
    terms.put(DwcTerm.infraspecificEpithet.qualifiedName(), infraspecificEpithet);

    return ExtendedRecord.newBuilder().setId(id).setCoreTerms(terms).build();
  }

}