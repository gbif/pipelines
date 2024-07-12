package org.gbif.pipelines.backbone.impact.v2;

import java.io.Closeable;
import org.gbif.kvs.species.Identification;

public interface NameUsageMatchV2Service extends Closeable {
  NameUsageMatchV2 match(Identification identification);
}
