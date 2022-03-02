package org.gbif.pipelines.core.interpreters.specific;

import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.core.parsers.clustering.ClusteringService;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;

/**
 * Interpreting function that receives a ExtendedRecord instance and applies an interpretation to
 * it.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ClusteredInterpreter {

  public static BiConsumer<GbifIdRecord, BasicRecord> interpretIsClustered(
      ClusteringService clusteringService) {
    return (gr, br) -> {
      if (clusteringService != null) {
        Long gbifId = gr.getGbifId();
        if (gbifId != null) {
          br.setIsClustered(clusteringService.isClustered(gbifId));
        }
      }
    };
  }
}
