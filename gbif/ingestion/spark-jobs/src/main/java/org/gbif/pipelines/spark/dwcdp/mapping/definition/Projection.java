package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.List;
import java.util.Map;

/** Final projection hints. Deliberately small; arbitrary Spark expressions remain an escape hatch. */
public record Projection(List<String> select, Map<String, String> rename, List<String> drop) {
  public Projection {
    select = select == null ? List.of() : List.copyOf(select);
    rename = rename == null ? Map.of() : Map.copyOf(rename);
    drop = drop == null ? List.of() : List.copyOf(drop);
  }

  public static Projection none() {
    return new Projection(List.of(), Map.of(), List.of());
  }
}
