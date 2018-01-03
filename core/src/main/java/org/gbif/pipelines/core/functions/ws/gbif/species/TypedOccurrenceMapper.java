package org.gbif.pipelines.core.functions.ws.gbif.species;

import org.gbif.pipelines.io.avro.TypedOccurrence;

class TypedOccurrenceMapper {

  private TypedOccurrenceMapper() {
  }

  static void mapFromSpeciesMatch(TypedOccurrence occurrence, SpeciesMatchResponseModel model) {
    occurrence.setKingdom(model.getKingdom());
    occurrence.setPhylum(model.getPhylum());
    occurrence.setClass$(model.getClazz());
    occurrence.setOrder(model.getOrder());
    occurrence.setFamily(model.getFamily());
    occurrence.setGenus(model.getGenus());
    occurrence.setScientificName(model.getScientificName());
    occurrence.setKingdomKey(model.getKingdomKey());
    occurrence.setPhylumKey(model.getPhylumKey());
    occurrence.setClassKey(model.getClassKey());
    occurrence.setOrderKey(model.getOrderKey());
    occurrence.setFamilyKey(model.getFamilyKey());
    occurrence.setGenusKey(model.getGenusKey());
    occurrence.setNubKey(model.getUsageKey());
    occurrence.setTaxonRank(model.getRank());
  }
}
