package org.gbif.pipelines.builder;

import org.apache.avro.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

/**
 * A function to read an UntypedOccurrence and return it as a SOLR document.
 * <p>
 * For demo only.
 */
public class SolrDocBuilder extends DoFn<TypedOccurrence, SolrInputDocument> {
    private static final Schema SCHEMA = UntypedOccurrence.getClassSchema();
    private static final float DEFAULT_BOOST = 1f;

    @ProcessElement
    public void processElement(ProcessContext c) {
        TypedOccurrence inputRecord = c.element();
        SolrInputDocument outputRecord = new SolrInputDocument();
        if (inputRecord == null) {
            return;
        }
        Object occurrenceId = inputRecord.get("occurrenceId");
        if (occurrenceId == null || occurrenceId.toString().isEmpty()) {
            return;
        }

        // As a quick POC we just copy the fields in using the same name as the Avro schema (very naive)
        SCHEMA.getFields().stream()
                .filter(f -> f != null && f.name() != null)
                .filter(f -> inputRecord.getSchema().getField(f.name()) != null)
                .filter(f -> inputRecord.get(f.name()) != null)
                .forEach(f -> {
                    SolrInputField inputField = new SolrInputField(f.name());
                    inputField.addValue(inputRecord.get(f.name()).toString(), DEFAULT_BOOST);
                    outputRecord.put(f.name(), inputField);
                });

        c.output(outputRecord);
    }
}
