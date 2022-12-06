package org.gbif.pipelines.transforms.converters;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.OCCURRENCE_EXT_COUNT;

import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.converters.OccurrenceExtensionConverter;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * A transformation that will extract occurrence records from any format of ExtendedRecord input. If
 * the core is of type occurrence, then the occurrence and its extensions will be handled. If the
 * core is of any other type, then a pivot is applied and a flat occurrence record (i.e. no
 * extensions) is emitted for every uniquely identified occurrence record. Where occurrences exist
 * as extensions, but are not uniquely identified, they will be swallowed and not emitted.
 *
 * @see <a href="https://github.com/gbif/ipt/wiki/BestPracticesSamplingEventData>Sampling event</a>
 */
@Slf4j
public class OccurrenceExtensionTransform extends DoFn<ExtendedRecord, ExtendedRecord> {

  private static final String COUNTER_NAME = OCCURRENCE_EXT_COUNT;
  private final Counter counter = Metrics.counter(OccurrenceJsonTransform.class, COUNTER_NAME);

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
    var occurrenceExts = er.getExtensions().get(DwcTerm.Occurrence.qualifiedName());

    if (occurrenceExts != null) {
      if (occurrenceExts.isEmpty()) {
        // Do nothing, this Event/Taxon record contains no occurrences
        // Fix for https://github.com/gbif/pipelines/issues/471
        log.debug("Event/Taxon core archive with empty occurrence extensions");
      } else {

        OccurrenceExtensionConverter.convert(er)
            .forEach(
                extEr -> {
                  counterFn.accept(COUNTER_NAME);
                  resultConsumer.accept(extEr);
                });
      }
      // Core type is Occurrence
    } else if (DwcTerm.Occurrence.qualifiedName().equals(er.getCoreRowType())) {
      resultConsumer.accept(er);
    } else {
      log.warn("Event/Taxon core archive with no extensions maybe? Is this possible?");
    }
  }
}
