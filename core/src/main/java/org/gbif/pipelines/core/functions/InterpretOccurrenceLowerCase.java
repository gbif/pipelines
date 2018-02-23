package org.gbif.pipelines.core.functions;

import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.gbif.pipelines.io.avro.UntypedOccurrenceLowerCase;

public class InterpretOccurrenceLowerCase  implements SerializableFunction<UntypedOccurrenceLowerCase, TypedOccurrence> {

  @Override
  public TypedOccurrence apply(UntypedOccurrenceLowerCase source) {
    // worst code ever... quick test
    // note, we override a ton of this in the nub lookup in this - it is just demo code
    TypedOccurrence target = new TypedOccurrence();
    target.setOccurrenceId(source.getOccurrenceid());
    target.setKingdom(source.getKingdom());
    target.setPhylum(source.getPhylum());
    target.setClass$(source.getClass$());
    target.setOrder(source.getOrder());
    target.setFamily(source.getFamily());
    target.setGenus(source.getGenus());
    target.setSpecies(source.getSpecies());
    target.setSpecificEpithet(source.getSpecificepithet());
    target.setInfraspecificEpithet(source.getInfraspecificepithet());
    target.setTaxonRank(source.getTaxonrank());
    target.setScientificName(source.getScientificname());
    target.setScientificNameAuthorship(source.getScientificnameauthorship());
    target.setBasisOfRecord(source.getBasisofrecord());
    target.setGeodeticDatum(source.getGeodeticdatum());
    target.setCountry(source.getCountry());
    target.setEventDate(source.getEventdate());
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
    }
    return target;
  }
}
