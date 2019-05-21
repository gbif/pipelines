package org.gbif.pipelines.transforms;

import org.gbif.pipelines.core.interpreters.core.BasicInterpreter;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MISSED_GBIF_ID_COUNT;

@Slf4j
@NoArgsConstructor
public class FilterMissedGbifIdTransform extends DoFn<String, String> {

  private final Counter counter = Metrics.counter(FilterMissedGbifIdTransform.class, MISSED_GBIF_ID_COUNT);

  public static SingleOutput<String, String> create() {
    return ParDo.of(new FilterMissedGbifIdTransform());
  }

  @ProcessElement
  public void processElement(@Element String in, OutputReceiver<String> out) {
    if (in.contains(BasicInterpreter.GBIF_ID_INVALID)) {
      log.warn("GBIF ID DOESN'T EXIST - {}", in);
      counter.inc();
    } else {
      out.output(in);
    }
  }

}
