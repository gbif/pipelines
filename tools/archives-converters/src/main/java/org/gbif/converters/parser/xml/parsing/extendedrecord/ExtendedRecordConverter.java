package org.gbif.converters.parser.xml.parsing.extendedrecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.converters.parser.xml.model.IdentifierRecord;
import org.gbif.converters.parser.xml.model.ImageRecord;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.model.TypificationRecord;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ExtendedRecordConverter {

  private static final String RECORD_ID_ERROR = "RECORD_ID_ERROR";

  public static ExtendedRecord from(RawOccurrenceRecord rawRecord) {

    ExtendedRecord record = ExtendedRecord.newBuilder().setId(rawRecord.getId()).build();

    final BiConsumer<Term, String> setter =
        (term, value) ->
            Optional.ofNullable(value)
                .filter(str -> !str.isEmpty())
                .ifPresent(x -> record.getCoreTerms().put(term.qualifiedName(), x));

    setter.accept(DwcTerm.institutionCode, rawRecord.getInstitutionCode());
    setter.accept(DwcTerm.collectionCode, rawRecord.getCollectionCode());
    setter.accept(DwcTerm.catalogNumber, rawRecord.getCatalogueNumber());
    setter.accept(DwcTerm.scientificName, rawRecord.getScientificName());
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
    setter.accept(DwcTerm.coordinateUncertaintyInMeters, rawRecord.getLatLongPrecision());
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
    setter.accept(DwcTerm.locality, rawRecord.getLocality());
    setter.accept(DwcTerm.year, rawRecord.getYear());
    setter.accept(DwcTerm.month, rawRecord.getMonth());
    setter.accept(DwcTerm.day, rawRecord.getDay());
    setter.accept(DwcTerm.eventDate, rawRecord.getOccurrenceDate());
    setter.accept(DwcTerm.basisOfRecord, rawRecord.getBasisOfRecord());
    setter.accept(DwcTerm.identifiedBy, rawRecord.getIdentifierName());
    setter.accept(DwcTerm.dateIdentified, rawRecord.getDateIdentified());
    setter.accept(GbifTerm.elevationAccuracy, rawRecord.getAltitudePrecision());
    setter.accept(GbifTerm.depthAccuracy, rawRecord.getDepthPrecision());

    if (rawRecord.getIdentifierRecords() != null) {
      rawRecord.getIdentifierRecords().stream()
          .filter(ir -> ir.getIdentifierType() == IdentifierRecord.OCCURRENCE_ID_TYPE)
          .map(IdentifierRecord::getIdentifier)
          .findFirst()
          .ifPresent(id -> setter.accept(DwcTerm.occurrenceID, id));
    }

    if (rawRecord.getTypificationRecords() != null
        && !rawRecord.getTypificationRecords().isEmpty()) {
      // just use first one - any more makes no sense
      TypificationRecord typificationRecord = rawRecord.getTypificationRecords().get(0);
      setter.accept(GbifTerm.typifiedName, typificationRecord.getScientificName());
      setter.accept(DwcTerm.typeStatus, typificationRecord.getTypeStatus());
    }

    if (rawRecord.getImageRecords() != null && !rawRecord.getImageRecords().isEmpty()) {
      List<Map<String, String>> verbMediaList =
          rawRecord.getImageRecords().stream()
              .map(ExtendedRecordConverter::convertMediaTerms)
              .collect(Collectors.toList());

      record.getExtensions().put(Extension.MULTIMEDIA.getRowType(), verbMediaList);
    }

    return record;
  }

  private static Map<String, String> convertMediaTerms(ImageRecord imageRecord) {
    Map<String, String> mediaTerms = new HashMap<>(5);

    final BiConsumer<Term, String> mediaSetter =
        (term, value) ->
            Optional.ofNullable(value)
                .filter(str -> !str.isEmpty())
                .ifPresent(x -> mediaTerms.put(term.qualifiedName(), x));

    mediaSetter.accept(DcTerm.format, imageRecord.getRawImageType());
    mediaSetter.accept(DcTerm.identifier, imageRecord.getUrl());
    mediaSetter.accept(DcTerm.references, imageRecord.getPageUrl());
    mediaSetter.accept(DcTerm.description, imageRecord.getDescription());
    mediaSetter.accept(DcTerm.license, imageRecord.getRights());

    return mediaTerms;
  }

  public static String getRecordIdError() {
    return RECORD_ID_ERROR;
  }
}
