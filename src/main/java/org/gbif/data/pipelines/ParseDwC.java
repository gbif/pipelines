package org.gbif.data.pipelines;

import org.gbif.data.io.avro.VerbatimOccurrence;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;

/**
 * A function to read a line of text and generate a VerbatimOccurrence.
 * For demo only.
 */
public class ParseDwC extends DoFn<String, VerbatimOccurrence> {

  @ProcessElement
  public void processElement(ProcessContext c) {
    String raw = c.element();
    String atoms[] = raw.split(",");
    VerbatimOccurrence o = new VerbatimOccurrence();
    o.setOccurrenceId(atoms[0]);
    o.setScientificName(atoms[1]);
    c.output(o);
  }
}
