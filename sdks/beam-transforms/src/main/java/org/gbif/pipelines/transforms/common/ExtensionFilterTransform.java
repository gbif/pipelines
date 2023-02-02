package org.gbif.pipelines.transforms.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/** Transform allows extensions in ExtendedRecord only from allowExtenstionsSet */
@AllArgsConstructor(staticName = "create")
public class ExtensionFilterTransform
    extends PTransform<PCollection<ExtendedRecord>, PCollection<ExtendedRecord>> {
  private final Set<String> allowExtenstionsSet;

  /** Allow all extensions if allowExtenstionsSet is null or empty */
  @Override
  public PCollection<ExtendedRecord> expand(PCollection<ExtendedRecord> input) {
    return allowExtenstionsSet == null || allowExtenstionsSet.isEmpty()
        ? input
        : createDoFn().expand(input);
  }

  /** For Java pipelin */
  public Map<String, ExtendedRecord> transform(Map<String, ExtendedRecord> source) {
    if (allowExtenstionsSet == null || allowExtenstionsSet.isEmpty()) {
      return source;
    }
    Map<String, ExtendedRecord> output = new HashMap<>(source.size());
    source.forEach((k, v) -> output.put(k, filter(v)));
    return output;
  }

  /** For Beam pipelin */
  private ParDo.SingleOutput<ExtendedRecord, ExtendedRecord> createDoFn() {
    return ParDo.of(
        new DoFn<ExtendedRecord, ExtendedRecord>() {
          @ProcessElement
          public void processElement(
              @Element ExtendedRecord er, OutputReceiver<ExtendedRecord> out) {

            if (er.getExtensions() == null || er.getExtensions().isEmpty()) {
              out.output(er);
              return;
            }

            out.output(filter(er));
          }
        });
  }

  /** Common part */
  private ExtendedRecord filter(ExtendedRecord er) {
    Map<String, List<Map<String, String>>> extensions = new HashMap<>();

    er.getExtensions().entrySet().stream()
        .filter(es -> allowExtenstionsSet.contains(es.getKey()))
        .filter(es -> !es.getValue().isEmpty())
        .forEach(es -> extensions.put(es.getKey(), es.getValue()));

    return ExtendedRecord.newBuilder()
        .setId(er.getId())
        .setCoreTerms(er.getCoreTerms())
        .setExtensions(extensions)
        .build();
  }
}
