package org.gbif.pipelines.transforms.converters;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.OCCURRENCE_EXT_COUNT;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.converters.OccurrenceExtensionConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;

/**
 * Beam level transformation for sampling event where occurrence records stored in extensions
 *
 * @see <a href="https://github.com/gbif/ipt/wiki/BestPracticesSamplingEventData>Sampling event</a>
 */
public class OccurrenceExtensionTransform extends DoFn<ExtendedRecord, ExtendedRecord> {

  private static final String COUNTER_NAME = OCCURRENCE_EXT_COUNT;
  private final Counter counter = Metrics.counter(GbifJsonTransform.class, COUNTER_NAME);

  private SerializableConsumer<String> counterFn = v -> counter.inc();

  public static SingleOutput<ExtendedRecord, ExtendedRecord> create() {
    return ParDo.of(new OccurrenceExtensionTransform());
  }

  public void setCounterFn(SerializableConsumer<String> counterFn) {
    this.counterFn = counterFn;
  }

  @ProcessElement
  public void processElement(@Element ExtendedRecord er, OutputReceiver<ExtendedRecord> out) {
    convert(er, out::output);
  }

  public void convert(ExtendedRecord er, Consumer<ExtendedRecord> resultConsumer) {
    List<Map<String, String>> occurrenceExts =
        er.getExtensions().get(DwcTerm.Occurrence.qualifiedName());
    if (occurrenceExts != null && !occurrenceExts.isEmpty()) {
      Map<String, String> coreTerms = er.getCoreTerms();
      occurrenceExts.forEach(
          occurrence ->
              OccurrenceExtensionConverter.convert(coreTerms, occurrence)
                  .ifPresent(
                      extEr -> {
                        counterFn.accept(COUNTER_NAME);
                        resultConsumer.accept(extEr);
                      }));
    } else {
      resultConsumer.accept(er);
    }
  }
}
