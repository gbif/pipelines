package au.org.ala.pipelines.transforms;

import java.io.Serializable;
import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;

/**
 * A version of the @{@link org.gbif.pipelines.transforms.core.DerivedMetadataTransform} without the
 * taxonomic component (which is GBIF specific).
 */
@Builder
public class ALADerivedMetadataTransform implements Serializable {

  private static final TupleTag<DerivedMetadataRecord> TAG =
      new TupleTag<DerivedMetadataRecord>() {};

  @NonNull private final TupleTag<ExtendedRecord> extendedRecordTag;

  @NonNull private final TupleTag<String> convexHullTag;

  @NonNull private final TupleTag<EventDate> temporalCoverageTag;

  public ParDo.SingleOutput<KV<String, CoGbkResult>, KV<String, DerivedMetadataRecord>>
      converter() {

    DoFn<KV<String, CoGbkResult>, KV<String, DerivedMetadataRecord>> fn =
        new DoFn<KV<String, CoGbkResult>, KV<String, DerivedMetadataRecord>>() {

          private ExtendedRecord getAssociatedVerbatim(
              TaxonRecord taxonRecord, List<ExtendedRecord> verbatimRecords) {
            return verbatimRecords.stream()
                .filter(er -> er.getId().equals(taxonRecord.getId()))
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

            c.output(KV.of(key, builder.build()));
          }
        };
    return ParDo.of(fn);
  }

  public static TupleTag<DerivedMetadataRecord> tag() {
    return TAG;
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
