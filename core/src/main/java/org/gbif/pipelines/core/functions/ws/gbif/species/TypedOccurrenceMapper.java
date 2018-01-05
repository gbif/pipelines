package org.gbif.pipelines.core.functions.ws.gbif.species;

import org.gbif.pipelines.io.avro.TypedOccurrence;

class TypedOccurrenceMapper {

  private TypedOccurrenceMapper() {
  }

  static TypedOccurrence mapFromSpeciesMatch(TypedOccurrence occurrence, SpeciesMatchResponseModel model) {
    //Has to be a new object or you will catch "Input values must not be mutated in any way."
    return TypedOccurrence.newBuilder(occurrence)
        .setKingdom(model.getKingdom())
        .setPhylum(model.getPhylum())
        .setClass$(model.getClazz())
        .setOrder(model.getOrder())
        .setFamily(model.getFamily())
        .setGenus(model.getGenus())
        .setScientificName(model.getScientificName())
        .setKingdomKey(model.getKingdomKey())
        .setPhylumKey(model.getPhylumKey())
        .setClassKey(model.getClassKey())
        .setOrderKey(model.getOrderKey())
        .setFamilyKey(model.getFamilyKey())
        .setGenusKey(model.getGenusKey())
        .setNubKey(model.getUsageKey())
        .setTaxonRank(model.getRank())
        .build();
  }
}
