package org.gbif.pipelines.core.functions;

import org.gbif.pipelines.core.interpreter.taxonomy.InterpretedTaxonomy;
import org.gbif.pipelines.core.interpreter.taxonomy.TaxonomyInterpretationException;
import org.gbif.pipelines.core.interpreter.taxonomy.TaxonomyInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.TaxonRecord;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        try {
          InterpretedTaxonomy interpretedTaxonomy = TaxonomyInterpreter.interpretTaxonomyFields(extendedRecord);

          context.output(TAXON_RECORD_TUPLE_TAG, interpretedTaxonomy.getTaxonRecord());
          context.output(TAXON_ISSUES_TUPLE_TAG, interpretedTaxonomy.getOccurrenceIssue());

        } catch (TaxonomyInterpretationException e) {
          LOG.error("Error while interpreting taxonmy of record {}", extendedRecord.getId(), e);
          // TODO: add to side output?? these are unexpected erros, they should not be issues
        }

      }

    };
  }

}
