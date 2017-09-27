package org.gbif.data.pipelines;

import org.gbif.data.io.avro.ExtendedRecord;
import org.gbif.data.io.avro.UntypedOccurrence;

import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;

/**
 * A function to read a line of text and generate a VerbatimOccurrence.
 * For demo only.
 */
public class ParseDwC extends DoFn<ExtendedRecord, UntypedOccurrence> {
  @ProcessElement
  public void processElement(ProcessContext c) {
    ExtendedRecord raw = c.element();
    UntypedOccurrence o = new UntypedOccurrence();
    o.setOccurrenceId(raw.getId());

    // rewrite to enable lookup
    Map<String,String> termsAsString = new HashMap<>();
    raw.getCoreTerms().forEach((k,v)-> termsAsString.put(k.toString(), v.toString()));

    o.setScientificName(termsAsString.get("http://rs.tdwg.org/dwc/terms/scientificName"));
    o.setBasisOfRecord(termsAsString.get("http://rs.tdwg.org/dwc/terms/basisOfRecord"));
    // TODO: all the terms
    c.output(o);
  }

}
