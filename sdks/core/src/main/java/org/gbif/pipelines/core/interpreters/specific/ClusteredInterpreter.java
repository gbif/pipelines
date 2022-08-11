package org.gbif.pipelines.core.interpreters.specific;

import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.core.parsers.clustering.ClusteringService;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;

/**
 * Interpreting function that receives a ExtendedRecord instance and applies an interpretation to
 * it.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ClusteredInterpreter {

  public static BiConsumer<GbifIdRecord, ClusteringRecord> interpretIsClustered(
      ClusteringService clusteringService) {
    return (gr, cr) -> {
      if (clusteringService != null) {
        Long gbifId = gr.getGbifId();
        if (gbifId != null) {
          cr.setIsClustered(clusteringService.isClustered(gbifId));
        }
      }
    };
  }
}
