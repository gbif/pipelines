package org.gbif.pipelines.core.utils;

import lombok.experimental.UtilityClass;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;

@UtilityClass
public class HdfsViewUtils {

  public static DwcTerm getCoreTerm(RecordType recordType) {
    if (recordType == RecordType.EVENT) {
      return DwcTerm.Event;
    }
    if (recordType == RecordType.OCCURRENCE) {
      return DwcTerm.Occurrence;
    }
    throw new IllegalArgumentException(recordType.name() + " RecordType is not core term");
  }

  public static StepType getSepType(RecordType recordType) {
    if (recordType == RecordType.EVENT) {
      return StepType.EVENTS_HDFS_VIEW;
    }
    if (recordType == RecordType.OCCURRENCE) {
      return StepType.HDFS_VIEW;
    }
    throw new IllegalArgumentException(recordType.name() + " RecordType is not core term");
  }
}
