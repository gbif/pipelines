package org.gbif.pipelines.core.interpreters.model;

public interface BlastResult {


    String getName();
    void setName(String name);

    Integer getIdentity();
    void setIdentity(Integer identity);

    String getAppliedScientificName();
    void setAppliedScientificName(String appliedScientificName);

    String getMatchType();
    void setMatchType(String matchType);

    Integer getBitScore();
    void setBitScore(Integer bitScore);

    Integer getExpectValue();
    void setExpectValue(Integer expectValue);

    String getQuerySequence();
    void setQuerySequence(String querySequence);

    String getSubjectSequence();
    void setSubjectSequence(String subjectSequence);

    Integer getQstart();
    void setQstart(Integer qstart);

    Integer getQend();
    void setQend(Integer qend);

    Integer getSstart();
    void setSstart(Integer sstart);

    Integer getSend();
    void setSend(Integer send);

    String getDistanceToBestMatch();
    void setDistanceToBestMatch(String distanceToBestMatch);

    Integer getSequenceLength();
    void setSequenceLength(Integer sequenceLength);
}
