package org.gbif.pipelines.io.avro;

import java.util.Comparator;

public class ComparatorUtil {

  public static final Comparator<AgentIdentifier> AGENT_ID_COMPARATOR =
      Comparator.comparing(AgentIdentifier::getType).thenComparing(AgentIdentifier::getValue);
}
