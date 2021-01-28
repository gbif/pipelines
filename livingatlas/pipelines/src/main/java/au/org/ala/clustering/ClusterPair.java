package au.org.ala.clustering;

import lombok.Builder;
import lombok.Value;
import org.gbif.pipelines.io.avro.OccurrenceFeatures;

@Value
@Builder
public class ClusterPair {
  OccurrenceFeatures o1;
  OccurrenceFeatures o2;
  RelationshipAssertion assertion;
}
