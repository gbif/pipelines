package org.gbif.pipelines.core.parsers.temporal;

import java.time.temporal.TemporalAccessor;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.gbif.api.vocabulary.OccurrenceIssue;

@Setter
@AllArgsConstructor
@NoArgsConstructor
public class EventRange {

  private TemporalAccessor from;
  private TemporalAccessor to;

  private Set<OccurrenceIssue> issues = new HashSet<>();

  public Optional<TemporalAccessor> getFrom() {
    return Optional.ofNullable(from);
  }

  public Optional<TemporalAccessor> getTo() {
    return Optional.ofNullable(to);
  }

  public Set<OccurrenceIssue> getIssues() {
    return issues;
  }

  public void addIssues(Collection<OccurrenceIssue> issues) {
    this.issues.addAll(issues);
  }

  public void addIssue(OccurrenceIssue issue) {
    this.issues.add(issue);
  }

  public boolean hasIssues() {
    return !issues.isEmpty();
  }
}
