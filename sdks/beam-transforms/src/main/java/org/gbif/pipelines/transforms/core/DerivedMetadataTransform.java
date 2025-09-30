package org.gbif.pipelines.transforms.core;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.core.converters.JsonConverter;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MultiTaxonRecord;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;

@Builder
public class DerivedMetadataTransform implements Serializable {

  private static final TupleTag<DerivedMetadataRecord> TAG =
      new TupleTag<DerivedMetadataRecord>() {};

  private static final TupleTag<Iterable<MultiTaxonRecord>> ITERABLE_MULTI_TAXON_TAG =
      new TupleTag<Iterable<MultiTaxonRecord>>() {};

  @NonNull private final TupleTag<ExtendedRecord> extendedRecordTag;

  @NonNull private final TupleTag<String> convexHullTag;

  @NonNull private final TupleTag<EventDate> temporalCoverageTag;

  public ParDo.SingleOutput<KV<String, CoGbkResult>, KV<String, DerivedMetadataRecord>>
      converter() {

    DoFn<KV<String, CoGbkResult>, KV<String, DerivedMetadataRecord>> fn =
        new DoFn<KV<String, CoGbkResult>, KV<String, DerivedMetadataRecord>>() {

          private ExtendedRecord getAssociatedVerbatim(
              MultiTaxonRecord multiTaxonRecord, List<ExtendedRecord> verbatimRecords) {
            return verbatimRecords.stream()
                .filter(er -> er.getId().equals(multiTaxonRecord.getId()))
                .findFirst()
                .orElse(null);
          }

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult result = c.element().getValue();
            String key = c.element().getKey();
            String convexHull = result.getOnly(convexHullTag);

            EventDate temporalCoverage =
                result.getOnly(temporalCoverageTag, EventDate.newBuilder().build());

            List<MultiTaxonRecord> classifications =
                StreamSupport.stream(
                        result
                            .getOnly(ITERABLE_MULTI_TAXON_TAG, Collections.emptyList())
                            .spliterator(),
                        false)
                    .collect(Collectors.toList());

            List<ExtendedRecord> verbatimRecords =
                StreamSupport.stream(result.getAll(extendedRecordTag).spliterator(), false)
                    .collect(Collectors.toList());

            DerivedMetadataRecord.Builder builder = DerivedMetadataRecord.newBuilder().setId(key);
            if (convexHull != null && !convexHull.isEmpty()) {
              builder.setWktConvexHull(convexHull);
            }

            if (temporalCoverage.getGte() != null || temporalCoverage.getLte() != null) {
              builder.setTemporalCoverageBuilder(
                  org.gbif.pipelines.io.avro.json.EventDate.newBuilder()
                      .setGte(temporalCoverage.getGte())
                      .setLte(temporalCoverage.getLte()));
            }

            builder.setTaxonomicCoverage(
                classifications.stream()
                    .map(
                        tr ->
                            Optional.ofNullable(getAssociatedVerbatim(tr, verbatimRecords))
                                .map(
                                    vr ->
                                        JsonConverter.convertToGbifClassificationFromMultiTaxon(
                                            vr, tr)))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList()));
            c.output(KV.of(key, builder.build()));
          }
        };
    return ParDo.of(fn);
  }

  public static TupleTag<DerivedMetadataRecord> tag() {
    return TAG;
  }

  public static TupleTag<Iterable<MultiTaxonRecord>> iterableMultiTaxonTupleTag() {
    return ITERABLE_MULTI_TAXON_TAG;
  }

  /**
   * Maps {@link DerivedMetadataRecord} to key value, where key is {@link
   * DerivedMetadataRecord#getId}
   */
  public MapElements<DerivedMetadataRecord, KV<String, DerivedMetadataRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, DerivedMetadataRecord>>() {})
        .via((DerivedMetadataRecord dmr) -> KV.of(dmr.getId(), dmr));
  }
}
