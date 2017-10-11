package org.gbif.pipelines.core.functions;

import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import java.io.Serializable;
import java.util.function.Function;

class InterpretOccurrence implements SerializableFunction<UntypedOccurrence, TypedOccurrence> {

  @Override
  public TypedOccurrence apply(UntypedOccurrence source) {
    // worst code ever... quick test
    TypedOccurrence target = new TypedOccurrence();
    target.setOccurrenceId(source.getOccurrenceId());
    target.setKingdom(source.getKingdom());
    target.setPhylum(source.getPhylum());
    target.setClass$(source.getClass$());
    target.setOrder(source.getOrder());
    target.setFamily(source.getFamily());
    target.setGenus(source.getGenus());
    target.setSpecies(source.getSpecies());
    target.setSpecificEpithet(source.getSpecificEpithet());
    target.setInfraspecificEpithet(source.getInfraspecificEpithet());
    target.setTaxonRank(source.getTaxonRank());
    target.setScientificName(source.getScientificName());
    target.setScientificNameAuthorship(source.getScientificNameAuthorship());
    target.setBasisOfRecord(source.getBasisOfRecord());
    target.setGeodeticDatum(source.getGeodeticDatum());
    target.setCountry(source.getCountry());
    target.setEventDate(source.getEventDate());
    try {
      Double lat = Double.parseDouble(source.getDecimalLatitude().toString());
      Double lng = Double.parseDouble(source.getDecimalLongitude().toString());
      target.setDecimalLatitude(lat);
      target.setDecimalLongitude(lng);
      //target.setLocation(Arrays.asList(new Double[]{lat, lng}));
      //target.setLocation("{lat:" + lat +",lon:" + lng + "}");

    } catch (Exception e) {
    }
    return target;
  }
}
