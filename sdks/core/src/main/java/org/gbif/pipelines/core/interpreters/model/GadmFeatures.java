package org.gbif.pipelines.core.interpreters.model;

public interface GadmFeatures {
    String getLevel0Gid();
    String getLevel1Gid();
    String getLevel2Gid();
    String getLevel3Gid();
    void setLevel0Gid(String id);
    void setLevel0Name(String name);
    void setLevel1Gid(String id);
    void setLevel1Name(String name);
    void setLevel2Gid(String id);
    void setLevel2Name(String name);
    void setLevel3Gid(String id);
    void setLevel3Name(String name);
}
