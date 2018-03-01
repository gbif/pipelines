package org.gbif.pipelines.transform;

import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.interpretation.TaxonomyInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.Validation;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * {@link PTransform} to convert {@link ExtendedRecord} into {@link TaxonRecord} with its {@link OccurrenceIssue}.
 */
public class TaxonRecordTransform extends RecordTransform<ExtendedRecord, TaxonRecord> {

  public TaxonRecordTransform() {
    super("Interpret taxonomic record");
  }

  @Override
  DoFn<ExtendedRecord, TaxonRecord> interpret() {
    return new DoFn<ExtendedRecord, TaxonRecord>() {

      @ProcessElement
      public void processElement(ProcessContext context) {

        ExtendedRecord extendedRecord = context.element();
        Collection<Validation> validations = new ArrayList<>();

        TaxonRecord taxonRecord = new TaxonRecord();

        // interpretation
        Interpretation.of(extendedRecord)
          .using(TaxonomyInterpreter.taxonomyInterpreter(taxonRecord))
          .forEachValidation(trace -> validations.add(toValidation(trace.getContext())));

        // taxon record result
        context.output(getDataTupleTag(), taxonRecord);

        // issues
        if (!validations.isEmpty()) {
          context.output((getIssueTupleTag()),
                         OccurrenceIssue.newBuilder().setId(extendedRecord.getId()).setIssues(validations).build());
        }

      }

    };
  }

}
