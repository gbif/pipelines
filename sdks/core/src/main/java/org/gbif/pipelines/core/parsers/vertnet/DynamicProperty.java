package org.gbif.pipelines.core.parsers.vertnet;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DynamicProperty {

  private final String field;
  private final String key;
  private final String value;
  private final String type;
}
