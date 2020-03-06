package org.gbif.pipelines.fragmenter.strategy;

import java.nio.file.Path;
import java.util.function.Consumer;

import org.gbif.pipelines.fragmenter.record.OccurrenceRecord;

public interface Strategy {

  void process(Path path, Consumer<OccurrenceRecord> pushRecordFn);

}
