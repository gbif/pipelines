package org.gbif.pipelines.spark;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.Parent;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventLineage {
  String id;
  List<Parent> lineage;
}
