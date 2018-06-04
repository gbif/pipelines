package org.gbif.pipelines.labs.functions;

import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import org.apache.beam.sdk.transforms.SerializableFunction;

class InterpretOccurrence implements SerializableFunction<UntypedOccurrence, TypedOccurrence> {

  private static final long serialVersionUID = -1358742571915657405L;

  @Override
  public TypedOccurrence apply(UntypedOccurrence source) {
    // worst code ever... quick test
    // note, we override a ton of this in the nub lookup in this - it is just demo code
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
      if (source.getDecimallatitude() != null && source.getDecimallongitude() != null) {
        Double lat = Double.parseDouble(source.getDecimallatitude());
        Double lng = Double.parseDouble(source.getDecimallongitude());

        if (lat >= -90 && lat <= 90 && lng >= -180 && lng <= 180) {
          target.setDecimalLatitude(lat);
          target.setDecimalLongitude(lng);
          target.setLocation(lat + "," + lng);
        }

      }

    } catch (NumberFormatException e) {
      // NOP
    }
    return target;
  }


}
