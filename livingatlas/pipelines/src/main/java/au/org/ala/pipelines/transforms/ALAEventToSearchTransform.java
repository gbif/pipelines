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
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;

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
  @NonNull private final TupleTag<LocationInheritedRecord> locationInheritedRecordTag;
  @NonNull private final TupleTag<TemporalInheritedRecord> temporalInheritedRecordTag;
  @NonNull private final TupleTag<EventInheritedRecord> eventInheritedRecordTag;

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
            // Inherited
            EventInheritedRecord eir =
                v.getOnly(
                    eventInheritedRecordTag, EventInheritedRecord.newBuilder().setId(k).build());
            LocationInheritedRecord lir =
                v.getOnly(
                    locationInheritedRecordTag,
                    LocationInheritedRecord.newBuilder().setId(k).build());
            TemporalInheritedRecord tir =
                v.getOnly(
                    temporalInheritedRecordTag,
                    TemporalInheritedRecord.newBuilder().setId(k).build());

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
                .setLocationID(consolidate(core.getLocationID(), eir.getLocationID()))
                .setYear(tr.getYear())
                .setMonth(tr.getMonth())
                .setCountryCode(consolidate(lr.getCountryCode(), lir.getCountryCode()))
                .setStateProvince(consolidate(lr.getStateProvince(), lir.getStateProvince()));

            List<String> eventIDs =
                core.getParentsLineage().stream()
                    .sorted(
                        Comparator.comparingInt(org.gbif.pipelines.io.avro.Parent::getOrder)
                            .reversed())
                    .map(e -> e.getId())
                    .collect(Collectors.toList());
            eventIDs.add(core.getId());

            List<String> eventTypes =
                core.getParentsLineage().stream()
                    .sorted(
                        Comparator.comparingInt(org.gbif.pipelines.io.avro.Parent::getOrder)
                            .reversed())
                    .map(e -> e.getEventType())
                    .collect(Collectors.toList());

            if (core.getEventType() != null && core.getEventType().getConcept() != null) {
              eventTypes.add(core.getEventType().getConcept());
            }

            builder.setEventTypeHierarchy(eventTypes).setEventHierarchy(eventIDs);

            boolean hasYearInfo = builder.getYear() != null;
            boolean hasMonthInfo = builder.getMonth() != null;

            // extract location & temporal information from
            if (!hasYearInfo && tir.getYear() != null) {
              builder.setYear(tir.getYear());
            }

            if (!hasMonthInfo && tir.getMonth() != null) {
              builder.setMonth(tir.getMonth());
            }
            c.output(builder.build());
          }
        };
    return ParDo.of(fn).withSideInputs(metadataView);
  }

  public List<String> consolidate(String value, String denormedValue) {
    List<String> list = new ArrayList<>();
    if (value != null) list.add(value);
    if (denormedValue != null) list.add(denormedValue);
    return list;
  }
}
