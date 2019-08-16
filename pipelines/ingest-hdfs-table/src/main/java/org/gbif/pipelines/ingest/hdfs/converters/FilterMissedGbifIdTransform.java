package org.gbif.pipelines.ingest.hdfs.converters;

import org.gbif.pipelines.core.interpreters.core.BasicInterpreter;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

@Slf4j
@NoArgsConstructor
public class FilterMissedGbifIdTransform extends DoFn<OccurrenceHdfsRecord, OccurrenceHdfsRecord> {

  public static ParDo.SingleOutput<OccurrenceHdfsRecord, OccurrenceHdfsRecord> create() {
    return ParDo.of(new FilterMissedGbifIdTransform());
  }

  @ProcessElement
  public void processElement(@Element OccurrenceHdfsRecord in, OutputReceiver<OccurrenceHdfsRecord> out) {
    if (in.getIssue().contains(BasicInterpreter.GBIF_ID_INVALID)) {
      log.warn("GBIF ID DOESN'T EXIST - {}", in);
    } else {
      out.output(in);
    }
  }

}
