package org.gbif.pipelines.validator.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RawToInterpreted {

  private static final Map<String, String> MAP = new HashMap<>();

  static {
    MAP.put(DwcTerm.basisOfRecord.qualifiedName(), "basisOfRecord");
    MAP.put(DwcTerm.sex.qualifiedName(), "sex");
    MAP.put(DwcTerm.lifeStage.qualifiedName(), "lifeStage");
    MAP.put(DwcTerm.establishmentMeans.qualifiedName(), "establishmentMeans");
    MAP.put(DwcTerm.individualCount.qualifiedName(), "individualCount");
    MAP.put(DwcTerm.typeStatus.qualifiedName(), "typeStatus");
    MAP.put(DcTerm.references.qualifiedName(), "references");
    MAP.put(DwcTerm.preparations.qualifiedName(), "preparations");
    MAP.put(DwcTerm.minimumElevationInMeters.qualifiedName(), "minimumElevationInMeters");
    MAP.put(DwcTerm.maximumElevationInMeters.qualifiedName(), "maximumElevationInMeters");
    MAP.put(DwcTerm.minimumDepthInMeters.qualifiedName(), "minimumDepthInMeters");
    MAP.put(DwcTerm.maximumDepthInMeters.qualifiedName(), "maximumDepthInMeters");
    MAP.put(
        DwcTerm.minimumDistanceAboveSurfaceInMeters.qualifiedName(),
        "minimumDistanceAboveSurfaceInMeters");
    MAP.put(
        DwcTerm.maximumDistanceAboveSurfaceInMeters.qualifiedName(),
        "maximumDistanceAboveSurfaceInMeters");
    MAP.put(DwcTerm.coordinateUncertaintyInMeters.qualifiedName(), "coordinateUncertaintyInMeters");
    MAP.put(DwcTerm.coordinatePrecision.qualifiedName(), "coordinatePrecision");
    MAP.put(DwcTerm.decimalLatitude.qualifiedName(), "decimalLatitude");
    MAP.put(DwcTerm.decimalLongitude.qualifiedName(), "decimalLongitude");
    MAP.put(DwcTerm.year.qualifiedName(), "year");
    MAP.put(DwcTerm.month.qualifiedName(), "month");
    MAP.put(DwcTerm.day.qualifiedName(), "day");
    MAP.put(DwcTerm.startDayOfYear.qualifiedName(), "startDayOfYear");
    MAP.put(DwcTerm.endDayOfYear.qualifiedName(), "endDayOfYear");
    MAP.put(DcTerm.modified.qualifiedName(), "modified");
    MAP.put(DwcTerm.dateIdentified.qualifiedName(), "dateIdentified");
    MAP.put(DwcTerm.eventDate.qualifiedName(), "eventDateSingle");
    MAP.put(DwcTerm.occurrenceStatus.qualifiedName(), "occurrenceStatus");
    MAP.put(DwcTerm.organismQuantity.qualifiedName(), "organismQuantity");
    MAP.put(DwcTerm.organismQuantityType.qualifiedName(), "organismQuantityType");
    MAP.put(DwcTerm.sampleSizeUnit.qualifiedName(), "sampleSizeUnit");
    MAP.put(DwcTerm.sampleSizeValue.qualifiedName(), "sampleSizeValue");
    MAP.put(DwcTerm.continent.qualifiedName(), "continent");
    MAP.put(DwcTerm.recordedBy.qualifiedName(), "recordedBy");
    MAP.put(DwcTerm.identifiedBy.qualifiedName(), "identifiedBy");
    MAP.put(DwcTerm.recordNumber.qualifiedName(), "recordNumber");
    MAP.put(DwcTerm.organismID.qualifiedName(), "organismId");
    MAP.put(DwcTerm.samplingProtocol.qualifiedName(), "samplingProtocol");
    MAP.put(DwcTerm.eventID.qualifiedName(), "eventId");
    MAP.put(DwcTerm.parentEventID.qualifiedName(), "parentEventId");
    MAP.put(DwcTerm.institutionCode.qualifiedName(), "institutionCode");
    MAP.put(DwcTerm.locality.qualifiedName(), "locality");
    MAP.put(DwcTerm.occurrenceID.qualifiedName(), "occurrenceId");
    MAP.put(DwcTerm.waterBody.qualifiedName(), "waterBody");
    MAP.put(DwcTerm.countryCode.qualifiedName(), "countryCode");
    MAP.put(DwcTerm.country.qualifiedName(), "country");
    MAP.put(DwcTerm.stateProvince.qualifiedName(), "stateProvince");
    MAP.put(DwcTerm.kingdom.qualifiedName(), "gbifClassification.kingdom");
    MAP.put(DwcTerm.phylum.qualifiedName(), "gbifClassification.phylum");
    MAP.put(DwcTerm.class_.qualifiedName(), "gbifClassification.class");
    MAP.put(DwcTerm.order.qualifiedName(), "gbifClassification.order");
    MAP.put(DwcTerm.family.qualifiedName(), "gbifClassification.family");
    MAP.put(DwcTerm.genus.qualifiedName(), "gbifClassification.genus");
    MAP.put(DwcTerm.taxonID.qualifiedName(), "gbifClassification.taxonID");
  }

  public static Optional<String> getInterpretedField(String term) {
    return Optional.ofNullable(MAP.get(term));
  }
}
