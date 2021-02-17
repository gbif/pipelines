package org.gbif.pipelines.transforms.common;

import lombok.NoArgsConstructor;
import org.apache.beam.sdk.io.AvroIO;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.transforms.Transform;

@NoArgsConstructor(staticName = "create")
public class OccurrenceHdfsRecordTransform {

  /**
   * Writes {@link OccurrenceHdfsRecord} *.avro files to path, data will be split into several
   * files, uses Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public AvroIO.Write<OccurrenceHdfsRecord> write(String toPath, Integer numShards) {
    AvroIO.Write<OccurrenceHdfsRecord> write =
        AvroIO.write(OccurrenceHdfsRecord.class)
            .to(toPath)
            .withSuffix(PipelinesVariables.Pipeline.AVRO_EXTENSION)
            .withCodec(Transform.getBaseCodec());
    return numShards == null ? write : write.withNumShards(numShards);
  }
}
