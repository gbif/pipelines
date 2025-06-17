package org.gbif.pipelines.core.interpreters.model;

public interface Parent {
    String getId();
    String getType();
    String getName();
    String getEventType();
    int getOrder();

    void setEventType(String concept);

    void setId(String parentEventID);

    void setOrder(int i);
}
