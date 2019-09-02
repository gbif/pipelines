package org.gbif.pipelines.transforms.hdfs;

import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.transforms.Transform;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE_HDFS_RECORD;

public class OccurrenceHdfsRecordTransform extends Transform<OccurrenceHdfsRecord, OccurrenceHdfsRecord> {

  private OccurrenceHdfsRecordTransform() {
    super(OccurrenceHdfsRecord.class, OCCURRENCE_HDFS_RECORD);
  }

  public static OccurrenceHdfsRecordTransform create() {
    return new OccurrenceHdfsRecordTransform();
  }

}
