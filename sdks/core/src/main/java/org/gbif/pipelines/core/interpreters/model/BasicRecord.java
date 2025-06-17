package org.gbif.pipelines.core.interpreters.model;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public interface BasicRecord {

    Collection<VocabularyConcept> getTypeStatus();
    Double getOrganismQuantity();
    Double getSampleSizeValue();
    GeologicalContext createGeologicalContext();
    GeologicalContext getGeologicalContext();
    String getBasisOfRecord();
    String getLifeStage();
    String getOrganismQuantityType();
    String getSampleSizeUnit();
    String getSex();
    void addIssue(OccurrenceIssue occurrenceIssue);
    void setAssociatedSequences(List<String> list);
    void setBasisOfRecord(String name);
    void setDatasetID(List<String> strings);
    void setDatasetName(List<String> strings);
    void setDegreeOfEstablishment(VocabularyConcept vocabularyConcept);
    void setEstablishmentMeans(VocabularyConcept vocabularyConcept);
    void setGeologicalContext(GeologicalContext gx);
    void setIdentifiedBy(List<String> list);
    void setIdentifiedByIds(List<AgentIdentifier> agentIdentifiers);
    void setIndividualCount(Integer integer);
    void setIsSequenced(boolean b);
    void setLicense(String s);
    void setLifeStage(@NotNull VocabularyConcept vocabularyConcept);
    void setOccurrenceStatus(String name);
    void setOrganismQuantity(@NotNull Double aDouble);
    void setOrganismQuantityType(@NotNull String s);
    void setOtherCatalogNumbers(List<String> list);
    void setPathway(VocabularyConcept vocabularyConcept);
    void setPreparations(List<String> list);
    void setProjectId(List<String> list);
    void setRecordedBy(List<String> list);
    void setRecordedByIds(@NotNull ArrayList<AgentIdentifier> agentIdentifiers);
    void setReferences(String s);
    void setRelativeOrganismQuantity(double v);
    void setSampleSizeUnit(String s);
    void setSampleSizeValue(Double aDouble);
    void setSamplingProtocol(List<String> strings);
    void setSex(VocabularyConcept vocabularyConcept);
    void setTypeStatus(ArrayList<Object> objects);
    void setTypifiedName(String s);
}
