package org.gbif.xml.occurrence.parser.parsing.extendedrecord;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.xml.occurrence.parser.model.RawOccurrenceRecord;

import java.util.Optional;
import java.util.function.BiConsumer;

public class ExtendedRecordConverter {

  private ExtendedRecordConverter(){
    // NOP
  }

  public static ExtendedRecord from(RawOccurrenceRecord rawRecord) {

    //TODO: REMOVE AFTER
    String id = Optional.ofNullable(rawRecord.getId()).orElse("1");
    ExtendedRecord record = ExtendedRecord.newBuilder().setId(id).build();

    final BiConsumer<DwcTerm, String> setter =
      (term, value) -> Optional.ofNullable(value).ifPresent(x -> record.getCoreTerms().put(term.qualifiedName(), x));

    setter.accept(DwcTerm.institutionCode, rawRecord.getInstitutionCode());
    setter.accept(DwcTerm.collectionCode, rawRecord.getCollectionCode());
    setter.accept(DwcTerm.catalogNumber, rawRecord.getCatalogueNumber());
    setter.accept(DwcTerm.scientificName ,rawRecord.getScientificName());
    setter.accept(DwcTerm.scientificNameAuthorship, rawRecord.getAuthor());
    setter.accept(DwcTerm.taxonRank, rawRecord.getRank());
    setter.accept(DwcTerm.kingdom, rawRecord.getKingdom());
    setter.accept(DwcTerm.phylum, rawRecord.getPhylum());
    setter.accept(DwcTerm.class_, rawRecord.getKlass());
    setter.accept(DwcTerm.order, rawRecord.getOrder());
    setter.accept(DwcTerm.family, rawRecord.getFamily());
    setter.accept(DwcTerm.genus, rawRecord.getGenus());
    setter.accept(DwcTerm.specificEpithet, rawRecord.getSpecies());
    setter.accept(DwcTerm.infraspecificEpithet, rawRecord.getSubspecies());
    setter.accept(DwcTerm.decimalLatitude, rawRecord.getLatitude());
    setter.accept(DwcTerm.decimalLongitude, rawRecord.getLongitude());
    setter.accept(DwcTerm.coordinatePrecision, rawRecord.getLatLongPrecision());
    setter.accept(DwcTerm.geodeticDatum, rawRecord.getGeodeticDatum());
    setter.accept(DwcTerm.minimumElevationInMeters, rawRecord.getMinAltitude());
    setter.accept(DwcTerm.maximumElevationInMeters, rawRecord.getMaxAltitude());
    setter.accept(DwcTerm.minimumDepthInMeters, rawRecord.getMinDepth());
    setter.accept(DwcTerm.maximumDepthInMeters, rawRecord.getMaxDepth());
    setter.accept(DwcTerm.continent, rawRecord.getContinentOrOcean());
    setter.accept(DwcTerm.country, rawRecord.getCountry());
    setter.accept(DwcTerm.stateProvince, rawRecord.getStateOrProvince());
    setter.accept(DwcTerm.county, rawRecord.getCounty());
    setter.accept(DwcTerm.recordedBy, rawRecord.getCollectorName());
    setter.accept(DwcTerm.fieldNumber, rawRecord.getCollectorsFieldNumber());
    setter.accept(DwcTerm.locality ,rawRecord.getLocality());
    setter.accept(DwcTerm.year, rawRecord.getYear());
    setter.accept(DwcTerm.month, rawRecord.getMonth());
    setter.accept(DwcTerm.day, rawRecord.getDay());
    setter.accept(DwcTerm.eventDate, rawRecord.getOccurrenceDate());
    setter.accept(DwcTerm.basisOfRecord, rawRecord.getBasisOfRecord());
    setter.accept(DwcTerm.identifiedBy, rawRecord.getIdentifierName());

    //TODO - dateIdentified
    //setter.accept(DwcTerm., rawRecord.getYearIdentified());
    //setter.accept(DwcTerm., rawRecord.getMonthIdentified());
    //setter.accept(DwcTerm., rawRecord.getDayIdentified());

    setter.accept(DwcTerm.dateIdentified, rawRecord.getDateIdentified());
    setter.accept(DwcTerm.identificationQualifier, rawRecord.getUnitQualifier());

    return record;
  }


}
