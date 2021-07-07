package org.gbif.pipelines.validator.metircs;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RawToInderpreted {

  private static final Map<Term, String> MAP = new HashMap<>();

  static {
    MAP.put(DwcTerm.basisOfRecord, "basisOfRecord");
    MAP.put(DwcTerm.sex, "sex");
    MAP.put(DwcTerm.lifeStage, "lifeStage");
    MAP.put(DwcTerm.establishmentMeans, "establishmentMeans");
    MAP.put(DwcTerm.individualCount, "individualCount");
    MAP.put(DwcTerm.typeStatus, "typeStatus");
    MAP.put(DcTerm.references, "references");
    MAP.put(DwcTerm.preparations, "preparations");
    MAP.put(DwcTerm.minimumElevationInMeters, "minimumElevationInMeters");
    MAP.put(DwcTerm.maximumElevationInMeters, "maximumElevationInMeters");
    MAP.put(DwcTerm.minimumDepthInMeters, "minimumDepthInMeters");
    MAP.put(DwcTerm.maximumDepthInMeters, "maximumDepthInMeters");
    MAP.put(DwcTerm.minimumDistanceAboveSurfaceInMeters, "minimumDistanceAboveSurfaceInMeters");
    MAP.put(DwcTerm.maximumDistanceAboveSurfaceInMeters, "maximumDistanceAboveSurfaceInMeters");
    MAP.put(DwcTerm.coordinateUncertaintyInMeters, "coordinateUncertaintyInMeters");
    MAP.put(DwcTerm.coordinatePrecision, "coordinatePrecision");
    MAP.put(DwcTerm.decimalLatitude, "decimalLatitude");
    MAP.put(DwcTerm.decimalLongitude, "decimalLongitude");
    MAP.put(DwcTerm.year, "year");
    MAP.put(DwcTerm.month, "month");
    MAP.put(DwcTerm.day, "day");
    MAP.put(DwcTerm.startDayOfYear, "startDayOfYear");
    MAP.put(DwcTerm.endDayOfYear, "endDayOfYear");
    MAP.put(DcTerm.modified, "modified");
    MAP.put(DwcTerm.dateIdentified, "dateIdentified");
    MAP.put(DwcTerm.eventDate, "eventDateSingle");
    MAP.put(DwcTerm.occurrenceStatus, "occurrenceStatus");
    MAP.put(DwcTerm.organismQuantity, "organismQuantity");
    MAP.put(DwcTerm.organismQuantityType, "organismQuantityType");
    MAP.put(DwcTerm.sampleSizeUnit, "sampleSizeUnit");
    MAP.put(DwcTerm.sampleSizeValue, "sampleSizeValue");
    MAP.put(DwcTerm.continent, "continent");
    MAP.put(DwcTerm.recordedBy, "recordedBy");
    MAP.put(DwcTerm.identifiedBy, "identifiedBy");
    MAP.put(DwcTerm.recordNumber, "recordNumber");
    MAP.put(DwcTerm.organismID, "organismId");
    MAP.put(DwcTerm.samplingProtocol, "samplingProtocol");
    MAP.put(DwcTerm.eventID, "eventId");
    MAP.put(DwcTerm.parentEventID, "parentEventId");
    MAP.put(DwcTerm.institutionCode, "institutionCode");
    MAP.put(DwcTerm.locality, "locality");
    MAP.put(DwcTerm.occurrenceID, "occurrenceId");
    MAP.put(DwcTerm.waterBody, "waterBody");
    MAP.put(DwcTerm.countryCode, "countryCode");
    MAP.put(DwcTerm.country, "country");
    MAP.put(DwcTerm.stateProvince, "stateProvince");
    MAP.put(DwcTerm.kingdom, "gbifClassification.kingdom");
    MAP.put(DwcTerm.phylum, "gbifClassification.phylum");
    MAP.put(DwcTerm.class_, "gbifClassification.class");
    MAP.put(DwcTerm.order, "gbifClassification.order");
    MAP.put(DwcTerm.family, "gbifClassification.family");
    MAP.put(DwcTerm.genus, "gbifClassification.genus");
    MAP.put(DwcTerm.taxonID, "gbifClassification.taxonID");
  }

  public static Optional<String> getInterpretedField(Term term) {
    return Optional.ofNullable(MAP.get(term));
  }
}
