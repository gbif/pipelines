package au.org.ala.clustering;

import lombok.Builder;
import lombok.Value;
import org.gbif.clustering.parsers.RelationshipAssertion;

/** Paired with an assertion indicated why they are considered related. */
@Value
@Builder
public class ClusterPair {
  HashKeyOccurrence o1;
  HashKeyOccurrence o2;
  RelationshipAssertion<HashKeyOccurrence> assertion;
}
