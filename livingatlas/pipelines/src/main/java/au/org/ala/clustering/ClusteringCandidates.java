package au.org.ala.clustering;

import java.util.List;
import lombok.Builder;
import lombok.Getter;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceFeatures;

@Builder
@Getter
public class ClusteringCandidates {

  String hashKey;
  List<OccurrenceFeatures> candidates;
}
