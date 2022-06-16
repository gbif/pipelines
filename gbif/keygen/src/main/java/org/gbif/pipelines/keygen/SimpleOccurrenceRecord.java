package org.gbif.pipelines.keygen;

import java.util.Optional;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@NoArgsConstructor(staticName = "create")
public class SimpleOccurrenceRecord implements OccurrenceRecord {

  private String occurrenceId;
  private String triplet;

  @Override
  public String toStringRecord() {
    return this.toString();
  }

  @Override
  public Optional<String> getOccurrenceId() {
    return Optional.ofNullable(occurrenceId);
  }

  @Override
  public Optional<String> getTriplet() {
    return Optional.ofNullable(triplet);
  }
}
