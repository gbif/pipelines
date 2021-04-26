package au.org.ala.clustering;

import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Cluster grouping of occurrences that have matching hash keys. */
@Builder
@Getter
@Setter
@DefaultSchema(JavaBeanSchema.class)
public class ClusteringCandidates {

  @Nullable String hashKey;
  @Nullable List<HashKeyOccurrence> candidates;
}
