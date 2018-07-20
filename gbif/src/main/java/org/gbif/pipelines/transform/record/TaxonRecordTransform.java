package org.gbif.pipelines.transform.record;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.interpretation.Interpretation;
import org.gbif.pipelines.core.interpretation.TaxonomyInterpreter;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.io.avro.issue.Validation;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.transform.RecordTransform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform} to convert {@link ExtendedRecord} into {@link TaxonRecord} with its {@link
 * OccurrenceIssue}.
 */
public class TaxonRecordTransform extends RecordTransform<ExtendedRecord, TaxonRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonRecordTransform.class);

  private final Config wsConfig;

  private TaxonRecordTransform(Config wsConfig) {
    super("Interpret taxonomic record");
    this.wsConfig = wsConfig;
  }

  public static TaxonRecordTransform create(Config wsConfig) {
    Objects.requireNonNull(wsConfig);
    return new TaxonRecordTransform(wsConfig);
  }

  @Override
  public DoFn<ExtendedRecord, KV<String, TaxonRecord>> interpret() {
    return new DoFn<ExtendedRecord, KV<String, TaxonRecord>>() {

      @ProcessElement
      public void processElement(ProcessContext context) {

        ExtendedRecord extendedRecord = context.element();
        String id = extendedRecord.getId();
        Collection<Validation> validations = new ArrayList<>();

        // creates avro target file
        TaxonRecord taxonRecord = new TaxonRecord();

        // interpretation
        Interpretation.of(extendedRecord)
            .using(TaxonomyInterpreter.taxonomyInterpreter(taxonRecord, wsConfig))
            .forEachValidation(trace -> validations.add(toValidation(trace.getContext())));

        // taxon record result
        if (Objects.nonNull(taxonRecord.getId())) {
          // the id is null when there is an error in the interpretation. In these cases we do not
          // write the
          // taxonRecord because it is totally empty.
          context.output(getDataTag(), KV.of(id, taxonRecord));
        } else {
          LOG.info("TaxonRecord empty for extended record {} -- Not written.", id);
        }

        // issues
        if (!validations.isEmpty()) {
          OccurrenceIssue issue =
              OccurrenceIssue.newBuilder().setId(id).setIssues(validations).build();
          context.output(getIssueTag(), KV.of(id, issue));
        }
      }
    };
  }

  @Override
  public TaxonRecordTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(
        pipeline, OccurrenceIssue.class, TaxonRecord.class, ExtendedRecord.class);
    return this;
  }
}
