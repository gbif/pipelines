package org.gbif.pipelines.interpretation.taxonomy;

import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.interpretation.interpreters.TaxonomyInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.Validation;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform} to convert {@link ExtendedRecord} into {@link TaxonRecord} with its {@link OccurrenceIssue}.
 */
public class TaxonomicInterpretationTransform extends PTransform<PCollection<ExtendedRecord>, PCollectionTuple> {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonomicInterpretationTransform.class);

  public static final TupleTag<TaxonRecord> TAXON_RECORD_TUPLE_TAG = new TupleTag<TaxonRecord>() {};

  public static final TupleTag<OccurrenceIssue> TAXON_ISSUES_TUPLE_TAG = new TupleTag<OccurrenceIssue>() {};

  @Override
  public PCollectionTuple expand(PCollection<ExtendedRecord> input) {
    return input.apply("Applying a taxonomic interpretation",
                       ParDo.of(interpretTaxonomy())
                         .withOutputTags(TAXON_RECORD_TUPLE_TAG, TupleTagList.of(TAXON_ISSUES_TUPLE_TAG)));
  }

  private DoFn<ExtendedRecord, TaxonRecord> interpretTaxonomy() {
    return new DoFn<ExtendedRecord, TaxonRecord>() {

      @ProcessElement
      public void processElement(ProcessContext context) {

        ExtendedRecord extendedRecord = context.element();
        Collection<Validation> validations = new ArrayList<>();

        TaxonRecord taxonRecord = new TaxonRecord();

        Interpretation.of(extendedRecord)
          .using(TaxonomyInterpreter.taxonomyInterpreter(taxonRecord))
          .forEachValidation(trace -> validations.add(toValidation(trace.getContext())));

        // taxon records
        context.output(TAXON_RECORD_TUPLE_TAG, taxonRecord);

        // issues
        if (!validations.isEmpty()) {
          context.output((TAXON_ISSUES_TUPLE_TAG),
                         OccurrenceIssue.newBuilder().setId(extendedRecord.getId()).setIssues(validations).build());
        }

      }

    };
  }

  /**
   * FIXME: move to utility class when all PR merged
   * <p>
   * Translates a OccurrenceIssue into Validation object.
   */
  private static Validation toValidation(org.gbif.api.vocabulary.OccurrenceIssue occurrenceIssue) {
    return Validation.newBuilder()
      .setName(occurrenceIssue.name())
      .setSeverity(occurrenceIssue.getSeverity().name())
      .build();
  }

}
