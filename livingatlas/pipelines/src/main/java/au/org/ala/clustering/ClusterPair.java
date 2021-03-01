package au.org.ala.clustering;

import lombok.Builder;
import lombok.Value;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceFeatures;
import org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion;

@Value
@Builder
public class ClusterPair {
  OccurrenceFeatures o1;
  OccurrenceFeatures o2;
  RelationshipAssertion assertion;
}
