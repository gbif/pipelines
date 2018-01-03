package org.gbif.pipelines.livingatlases.indexing;

import org.apache.avro.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

/**
 * A function to read an UntypedOccurrence and return it as a SOLR document.
 *
 * For demo only.
 */
public class SolrDocBuilder extends DoFn<UntypedOccurrence, SolrInputDocument> {
  private static final Schema SCHEMA = UntypedOccurrence.getClassSchema();
  private static final float DEFAULT_BOOST = 1f;

  @ProcessElement
  public void processElement(ProcessContext c) {
    UntypedOccurrence inputRecord = c.element();
    SolrInputDocument outputRecord = new SolrInputDocument();

    // As a quick POC we just copy the fields in using the same name as the Avro schema (very naive)
    for (Schema.Field f : SCHEMA.getFields()) {
      if (inputRecord.get(f.name()) != null) {
        SolrInputField inputField = new SolrInputField(f.name());
        inputField.addValue(inputRecord.get(f.name()).toString(), DEFAULT_BOOST);
        outputRecord.put(f.name(), inputField);
      }
    }

    c.output(outputRecord);
  }
}
