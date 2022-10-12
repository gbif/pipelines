package org.gbif.pipelines.keygen;

import java.util.Optional;

public interface OccurrenceRecord {

  String toStringRecord();

  Optional<String> getOccurrenceId();

  Optional<String> getTriplet();
}
