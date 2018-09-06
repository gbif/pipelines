/*
 * Copyright 2011 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.converters.parser.xml.model;

import org.gbif.converters.parser.xml.constants.PrioritizedPropertyNameEnum;
import org.gbif.converters.parser.xml.parsing.xml.HigherTaxonParser;
import org.gbif.converters.parser.xml.parsing.xml.PrioritizedProperty;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents one of possibly many "identifications" in ABCD records. There are two
 * primary cases where multiple identifications would happen: - an institution records all of the
 * history of a given sample, and then marks the most recent as "preferred" - a given sample (e.g.
 * drop of swamp water) has many organisms within it and each of those is given as an
 * identification, and none are marked as preferred We will generate a new occurrence record for
 * each identification marked as "preferred", or for all given identifications if none are marked
 * preferred.
 */
public class Identification extends PropertyPrioritizer {

  private static final Logger LOG = LoggerFactory.getLogger(Identification.class);

  private final HigherTaxonParser taxonParser = new HigherTaxonParser();
  private boolean preferred;
  private String genus;
  private String dateIdentified;
  private String scientificName;
  private String identifierName;
  private Set<Taxon> higherTaxons = new HashSet<>();

  /**
   * Once this object has been populated by a Digester, there may be several PrioritizedProperties
   * that need to be resolved, and thereby set the final value of the corresponding field on this
   * object.
   */
  @Override
  public void resolvePriorities() {
    for (Map.Entry<PrioritizedPropertyNameEnum, Set<PrioritizedProperty>> entry :
        prioritizedProps.entrySet()) {
      PrioritizedPropertyNameEnum name = entry.getKey();
      String result = findHighestPriority(entry.getValue());
      switch (entry.getKey()) {
        case ID_DATE_IDENTIFIED:
          dateIdentified = result;
          break;
        case ID_IDENTIFIER_NAME:
          identifierName = result;
          break;
        case ID_SCIENTIFIC_NAME:
          scientificName = result;
          break;
        default:
          LOG.warn("Fell through priority resolution for [{}]", name);
      }
    }
  }

  public void populateRawOccurrenceRecord(RawOccurrenceRecord record) {
    populateRawOccurrenceRecord(record, false);
  }

  public void populateRawOccurrenceRecord(RawOccurrenceRecord record, boolean setUnitQualifier) {
    record.setGenus(this.genus);
    record.setDateIdentified(this.dateIdentified);
    record.setScientificName(this.scientificName);
    record.setIdentifierName(this.identifierName);
    if (setUnitQualifier) {
      record.setUnitQualifier(this.scientificName);
    }
    for (Taxon taxon : higherTaxons) {
      switch (taxon.getRank()) {
        case KINGDOM:
          record.setKingdom(taxon.getName());
          break;
        case PHYLUM:
          record.setPhylum(taxon.getName());
          break;
        case CLASS:
          record.setKlass(taxon.getName());
          break;
        case ORDER:
          record.setOrder(taxon.getName());
          break;
        case FAMILY:
          record.setFamily(taxon.getName());
          break;
        case SUBSPECIES:
        case SPECIES:
        case GENUS:
          break;
      }
    }
  }

  public void addHigherTaxon(String rank, String name) {
    Taxon taxon = taxonParser.parseTaxon(rank, name);
    if (taxon != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding taxon rank [{}]] name [{}]", taxon.getRank(), taxon.getName());
      }
      higherTaxons.add(taxon);
    }
  }

  public boolean isPreferred() {
    return preferred;
  }

  public void setPreferredAsString(String preferred) {
    this.preferred = "true".equals(preferred) || "1".equals(preferred);
    LOG.debug("Raw preferred is [{}], setting preferred to [{}]", preferred, preferred);
  }

  public void setPreferred(boolean preferred) {
    this.preferred = preferred;
  }

  public String getGenus() {
    return genus;
  }

  public void setGenus(String genus) {
    this.genus = genus;
  }

  public String getDateIdentified() {
    return dateIdentified;
  }

  public void setDateIdentified(String dateIdentified) {
    this.dateIdentified = dateIdentified;
  }

  public String getScientificName() {
    return scientificName;
  }

  public void setScientificName(String scientificName) {
    this.scientificName = scientificName;
  }

  public String getIdentifierName() {
    return identifierName;
  }

  public void setIdentifierName(String identifierName) {
    this.identifierName = identifierName;
  }

  public Set<Taxon> getHigherTaxons() {
    return higherTaxons;
  }

  public void setHigherTaxons(Set<Taxon> higherTaxons) {
    this.higherTaxons = higherTaxons;
  }
}
