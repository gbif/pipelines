package org.gbif.pipelines.core.interpreters.model;


public interface AgentIdentifier {
    String getIdentifier();
    String getType();
    String getValue();
}
