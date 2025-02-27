package org.gbif.pipelines.backbone.impact.v2;

import org.gbif.pipelines.backbone.impact.Identification;

import java.io.Closeable;

public interface NameUsageMatchV2Service extends Closeable {
  NameUsageMatchV2 match(Identification identification);
}
