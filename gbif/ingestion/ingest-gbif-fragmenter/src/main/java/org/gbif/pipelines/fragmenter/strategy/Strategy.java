package org.gbif.pipelines.fragmenter.strategy;

import java.nio.file.Path;
import java.util.function.Consumer;
import org.gbif.pipelines.keygen.OccurrenceRecord;

/** Processing strategy for different archives to specify how to read an archive */
public interface Strategy {

  void process(Path path, Consumer<OccurrenceRecord> pushRecordFn);
}
