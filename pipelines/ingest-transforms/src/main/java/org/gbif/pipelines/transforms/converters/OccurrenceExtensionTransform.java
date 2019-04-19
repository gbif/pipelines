package org.gbif.pipelines.transforms.converters;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.converters.OccurrenceExtensionConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

import lombok.NoArgsConstructor;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.OCCURRENCE_EXT_COUNT;

/**
 * Beam level transformation for samplong event where occurrence records stored in extensions
 *
 * @see <a href="https://github.com/gbif/ipt/wiki/BestPracticesSamplingEventData>Sampling event</a>
 */
@NoArgsConstructor(staticName = "create")
public class OccurrenceExtensionTransform extends DoFn<ExtendedRecord, ExtendedRecord> {

  private final Counter counter = Metrics.counter(GbifJsonTransform.class, OCCURRENCE_EXT_COUNT);

  @ProcessElement
  public void processElement(@Element ExtendedRecord er, OutputReceiver<ExtendedRecord> out) {
    if (er.getExtensions().containsKey(DwcTerm.Occurrence.qualifiedName())) {
      OccurrenceExtensionConverter.convert(er).forEach(oec -> {
        counter.inc();
        out.output(oec);
      });
    } else {
      out.output(er);
    }
  }

}
