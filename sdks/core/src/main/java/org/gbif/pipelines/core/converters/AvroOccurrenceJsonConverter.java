package org.gbif.pipelines.core.converters;

import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.io.avro.json.VerbatimRecord;

@SuppressWarnings("FallThrough")
@Slf4j
@Builder
public class AvroOccurrenceJsonConverter {

  private final MetadataRecord metadataRecord;
  private final BasicRecord basicRecord;
  private final TemporalRecord temporalRecord;
  private final LocationRecord locationRecord;
  private final TaxonRecord taxonRecord;
  private final GrscicollRecord grscicollRecord;
  private final MultimediaRecord multimediaRecord;
  private final ExtendedRecord extendedRecord;

  public OccurrenceJsonRecord convert() {

    OccurrenceJsonRecord.Builder builder = OccurrenceJsonRecord.newBuilder();

    mapMetadataRecord(builder);
    mapBasicRecord(builder);
    mapTemporalRecord(builder);
    mapLocationRecord(builder);
    mapTaxonRecord(builder);
    mapGrscicollRecord(builder);
    mapMultimediaRecord(builder);
    mapExtendedRecord(builder);

    return builder.build();
  }

  private void mapMetadataRecord(OccurrenceJsonRecord.Builder builder) {}

  private void mapBasicRecord(OccurrenceJsonRecord.Builder builder) {
    builder.setGbifId(basicRecord.getGbifId());
  }

  private void mapTemporalRecord(OccurrenceJsonRecord.Builder builder) {}

  private void mapLocationRecord(OccurrenceJsonRecord.Builder builder) {}

  private void mapTaxonRecord(OccurrenceJsonRecord.Builder builder) {}

  private void mapGrscicollRecord(OccurrenceJsonRecord.Builder builder) {}

  private void mapMultimediaRecord(OccurrenceJsonRecord.Builder builder) {}

  private void mapExtendedRecord(OccurrenceJsonRecord.Builder builder) {

    builder.setId(extendedRecord.getId());

    extractOptValue(extendedRecord, DwcTerm.recordNumber).ifPresent(builder::setRecordNumber);
    extractOptValue(extendedRecord, DwcTerm.organismID).ifPresent(builder::setOrganismId);
    extractOptValue(extendedRecord, DwcTerm.eventID).ifPresent(builder::setEventId);
    extractOptValue(extendedRecord, DwcTerm.parentEventID).ifPresent(builder::setParentEventId);
    extractOptValue(extendedRecord, DwcTerm.institutionCode).ifPresent(builder::setInstitutionCode);
    extractOptValue(extendedRecord, DwcTerm.collectionCode).ifPresent(builder::setCollectionCode);
    extractOptValue(extendedRecord, DwcTerm.catalogNumber).ifPresent(builder::setCatalogNumber);
    extractOptValue(extendedRecord, DwcTerm.occurrenceID).ifPresent(builder::setOccurrenceId);

    // Set extensions list
    List<String> extensions =
        extendedRecord.getExtensions().entrySet().stream()
            .filter(e -> e.getValue() != null && !e.getValue().isEmpty())
            .map(Entry::getKey)
            .distinct()
            .collect(Collectors.toList());
    builder.setExtensions(extensions);

    // Verbatim data
    VerbatimRecord verbatimRecord =
        VerbatimRecord.newBuilder()
            .setCoreTerms(extendedRecord.getCoreTerms())
            .setExtensions(extendedRecord.getExtensions())
            .build();
    builder.setVerbatim(verbatimRecord);
  }
}
