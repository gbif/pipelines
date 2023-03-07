package au.org.ala.pipelines.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.EVENTS_AVRO_TO_JSON_COUNT;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.StringUtils;
import org.gbif.pipelines.core.converters.JsonConverter;
import org.gbif.pipelines.io.avro.*;

/**
 * This converter will take AVRO record artefacts related to an event and produce a single AVRO
 * document that will support Spark SQL searching.
 */
@SuppressWarnings("ConstantConditions")
@Builder
public class ALAEventToSearchTransform implements Serializable {

  private static final long serialVersionUID = 1279313941024805871L;
  // Core
  @NonNull private final TupleTag<EventCoreRecord> eventCoreRecordTag;
  @NonNull private final TupleTag<TemporalRecord> temporalRecordTag;
  @NonNull private final TupleTag<LocationRecord> locationRecordTag;
  // Extension
  @NonNull private final PCollectionView<ALAMetadataRecord> metadataView;
  @NonNull private final TupleTag<MeasurementOrFactRecord> measurementOrFactRecordTag;
  private final TupleTag<Iterable<String>> taxonIDsTag;

  public SingleOutput<KV<String, CoGbkResult>, EventSearchRecord> converter() {

    DoFn<KV<String, CoGbkResult>, EventSearchRecord> fn =
        new DoFn<KV<String, CoGbkResult>, EventSearchRecord>() {

          private final Counter counter =
              Metrics.counter(ALAEventToSearchTransform.class, EVENTS_AVRO_TO_JSON_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            ALAMetadataRecord mdr = c.sideInput(metadataView);
            // Core
            EventCoreRecord core =
                v.getOnly(eventCoreRecordTag, EventCoreRecord.newBuilder().setId(k).build());
            TemporalRecord tr =
                v.getOnly(temporalRecordTag, TemporalRecord.newBuilder().setId(k).build());
            LocationRecord lr =
                v.getOnly(locationRecordTag, LocationRecord.newBuilder().setId(k).build());
            MeasurementOrFactRecord mofr =
                v.getOnly(
                    measurementOrFactRecordTag,
                    MeasurementOrFactRecord.newBuilder().setId(k).build());

            List<String> taxonIDs = new ArrayList<>();

            Iterable<String> retrievedTaxonIDs = v.getOnly(taxonIDsTag, null);

            // Convert and
            EventSearchRecord.Builder builder = EventSearchRecord.newBuilder().setId(core.getId());

            setSearchTerms(mdr, core, tr, lr, mofr, taxonIDs, retrievedTaxonIDs, builder);

            // copy core fields
            builder.setParentEventID(core.getParentEventID());
            builder.setEventType(
                core.getEventType() != null ? core.getEventType().getConcept() : null);
            builder.setSampleSizeValue(core.getSampleSizeValue());
            builder.setSampleSizeUnit(core.getSampleSizeUnit());
            builder.setReferences(core.getReferences());
            builder.setLicense(core.getLicense());
            builder.setDatasetID(core.getDatasetID());
            builder.setDatasetName(core.getDatasetName());
            // copy location fields
            builder.setContinent(lr.getContinent());
            builder.setWaterBody(lr.getWaterBody());
            builder.setCountry(lr.getCountry());
            builder.setMinimumElevationInMeters(lr.getMinimumElevationInMeters());
            builder.setMaximumElevationInMeters(lr.getMinimumElevationInMeters());
            builder.setElevation(lr.getElevation());
            builder.setElevationAccuracy(lr.getElevationAccuracy());
            builder.setMinimumDepthInMeters(lr.getMinimumDepthInMeters());
            builder.setMaximumDepthInMeters(lr.getMaximumDepthInMeters());
            builder.setDepth(lr.getDepth());
            builder.setDepthAccuracy(lr.getDepthAccuracy());
            builder.setMinimumDistanceAboveSurfaceInMeters(
                lr.getMinimumDistanceAboveSurfaceInMeters());
            builder.setMaximumDistanceAboveSurfaceInMeters(
                lr.getMaximumDistanceAboveSurfaceInMeters());
            builder.setDecimalLatitude(lr.getDecimalLatitude());
            builder.setDecimalLongitude(lr.getDecimalLongitude());
            builder.setCoordinateUncertaintyInMeters(lr.getCoordinateUncertaintyInMeters());
            builder.setCoordinatePrecision(lr.getCoordinatePrecision());
            builder.setLocality(lr.getLocality());
            builder.setGeoreferencedDate(lr.getGeoreferencedDate());
            builder.setFootprintWKT(lr.getFootprintWKT());
            builder.setBiome(lr.getBiome());
            // copy temporal fields
            builder.setDay(tr.getDay());
            builder.setEventDate(JsonConverter.convertEventDateSingle(tr).orElse(null));
            builder.setStartDayOfYear(tr.getStartDayOfYear());
            builder.setEndDayOfYear(tr.getEndDayOfYear());
            builder.setDateIdentified(tr.getDateIdentified());
            builder.setDatePrecision(tr.getDatePrecision());

            List<String> issues = new ArrayList<String>();
            // concat temporal, taxonomic etc
            issues.addAll(core.getIssues().getIssueList());
            issues.addAll(tr.getIssues().getIssueList());
            issues.addAll(lr.getIssues().getIssueList());
            builder.setIssues(issues);

            c.output(builder.build());
          }
        };
    return ParDo.of(fn).withSideInputs(metadataView);
  }

  private void setSearchTerms(
      ALAMetadataRecord mdr,
      EventCoreRecord core,
      TemporalRecord tr,
      LocationRecord lr,
      MeasurementOrFactRecord mofr,
      List<String> taxonIDs,
      Iterable<String> retrievedTaxonIDs,
      EventSearchRecord.Builder builder) {
    // set mof
    builder.setMeasurementOrFactTypes(
        mofr.getMeasurementOrFactItems().stream()
            .map(MeasurementOrFact::getMeasurementType)
            .filter(x -> StringUtils.isNotEmpty(x))
            .distinct()
            .collect(Collectors.toList()));
    builder
        .setDatasetKey(mdr.getDataResourceUid())
        .setTaxonKey(taxonIDs)
        .setLocationID(consolidate(core.getLocationID(), null))
        .setYear(tr.getYear())
        .setMonth(tr.getMonth())
        .setCountryCode(consolidate(lr.getCountryCode(), null))
        .setStateProvince(consolidate(lr.getStateProvince(), null));

    List<String> eventIDs =
        core.getParentsLineage().stream()
            .sorted(Comparator.comparingInt(Parent::getOrder).reversed())
            .map(e -> e.getId())
            .collect(Collectors.toList());
    eventIDs.add(core.getId());

    List<String> eventTypes =
        core.getParentsLineage().stream()
            .sorted(Comparator.comparingInt(Parent::getOrder).reversed())
            .map(e -> e.getEventType())
            .collect(Collectors.toList());

    if (core.getEventType() != null && core.getEventType().getConcept() != null) {
      eventTypes.add(core.getEventType().getConcept());
    }

    builder.setEventTypeHierarchy(eventTypes).setEventHierarchy(eventIDs);

    // associate taxonIDs from occurrences with events to support
    // taxon search
    if (retrievedTaxonIDs != null) {
      List<String> retrievedTaxonIDsToAdd = new ArrayList<>();
      retrievedTaxonIDs.forEach(retrievedTaxonIDsToAdd::add);
      builder.setTaxonKey(retrievedTaxonIDsToAdd);
    }
  }

  public List<String> consolidate(String value, String denormedValue) {
    List<String> list = new ArrayList<>();
    if (value != null) list.add(value);
    if (denormedValue != null) list.add(denormedValue);
    return list;
  }
}
