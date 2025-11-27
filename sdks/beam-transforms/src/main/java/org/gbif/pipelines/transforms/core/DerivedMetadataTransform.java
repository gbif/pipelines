package org.gbif.pipelines.transforms.core;

import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MultiTaxonRecord;
import org.gbif.pipelines.io.avro.json.DerivedClassification;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;
import org.gbif.pipelines.io.avro.json.DerivedTaxonUsage;
import org.gbif.pipelines.io.avro.json.TaxonCoverage;

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

            List<MultiTaxonRecord> multiTaxonRecords =
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

            TaxonCoverage.Builder taxonCoverage = TaxonCoverage.newBuilder();
            Map<String, List<DerivedClassification>> classifications = new HashMap<>();
            taxonCoverage.setClassifications(classifications);

            List<String> taxonIDs = new ArrayList<>();
            taxonCoverage.setTaxonIDs(taxonIDs);

            multiTaxonRecords.forEach(
                mt -> {

                  // add taxonID
                  ExtendedRecord er = getAssociatedVerbatim(mt, verbatimRecords);
                  extractOptValue(er, DwcTerm.taxonID).ifPresent(taxonIDs::add);

                  // add classification
                  mt.getTaxonRecords()
                      .forEach(
                          tr -> {
                            DerivedClassification.Builder derivedTaxon =
                                DerivedClassification.newBuilder();

                            if (tr.getUsage() != null) {
                              derivedTaxon
                                  .setUsage(
                                      DerivedTaxonUsage.newBuilder()
                                          .setKey(tr.getUsage().getRank())
                                          .setRank(tr.getUsage().getRank())
                                          .setName(tr.getUsage().getName())
                                          .build())
                                  .setStatus(tr.getUsage().getStatus());
                            }

                            if (tr.getAcceptedUsage() != null) {
                              derivedTaxon.setAcceptedUsage(
                                  DerivedTaxonUsage.newBuilder()
                                      .setKey(tr.getAcceptedUsage().getRank())
                                      .setRank(tr.getAcceptedUsage().getRank())
                                      .setName(tr.getAcceptedUsage().getName())
                                      .build());
                            }

                            derivedTaxon.setIucnRedListCategoryCode(
                                tr.getIucnRedListCategoryCode());

                            if (tr.getClassification() != null) {
                              Map<String, String> classification = new HashMap<>();
                              Map<String, String> classificationKeys = new HashMap<>();
                              List<String> taxonKeys = new ArrayList<>();
                              tr.getClassification()
                                  .forEach(
                                      cl -> {
                                        classification.put(cl.getRank(), cl.getName());
                                        classificationKeys.put(cl.getRank(), cl.getKey());
                                        taxonKeys.add(cl.getKey());
                                      });

                              derivedTaxon.setClassification(classification);
                              derivedTaxon.setClassificationKeys(classificationKeys);
                              derivedTaxon.setTaxonKeys(taxonKeys);
                            }

                            if (tr.getIssues() != null) {
                              derivedTaxon.setIssues(tr.getIssues().getIssueList());
                            }

                            classifications
                                .computeIfAbsent(tr.getDatasetKey(), k -> new ArrayList<>())
                                .add(derivedTaxon.build());
                          });
                });

            builder.setTaxonomicCoverage(taxonCoverage.build());

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
