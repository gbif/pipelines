package org.gbif.pipelines.transforms.converters;

import java.util.List;
import java.util.Map;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.converters.OccurrenceExtensionConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.OCCURRENCE_EXT_COUNT;

/**
 * Beam level transformation for sampling event where occurrence records stored in extensions
 *
 * @see <a href="https://github.com/gbif/ipt/wiki/BestPracticesSamplingEventData>Sampling event</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OccurrenceExtensionTransform extends DoFn<ExtendedRecord, ExtendedRecord> {

  private final Counter counter = Metrics.counter(GbifJsonTransform.class, OCCURRENCE_EXT_COUNT);

  public static SingleOutput<ExtendedRecord, ExtendedRecord> create() {
    return ParDo.of(new OccurrenceExtensionTransform());
  }

  @ProcessElement
  public void processElement(@Element ExtendedRecord er, OutputReceiver<ExtendedRecord> out) {
    List<Map<String, String>> occurrenceExts = er.getExtensions().get(DwcTerm.Occurrence.qualifiedName());
    if (occurrenceExts != null && !occurrenceExts.isEmpty()) {
      Map<String, String> coreTerms = er.getCoreTerms();
      occurrenceExts.forEach(occurrence -> {
        counter.inc();
        out.output(OccurrenceExtensionConverter.convert(coreTerms, occurrence));
      });
    } else {
      out.output(er);
    }
  }

}
