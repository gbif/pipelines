package org.gbif.pipelines.core.interpreters.model;

public interface Amplification {

    String getAmplificationDate();
    void setAmplificationDate(String amplificationDate);

    String getAmplificationStaff();
    void setAmplificationStaff(String amplificationStaff);

    String getAmplificationSuccess();
    void setAmplificationSuccess(String amplificationSuccess);

    String getAmplificationSuccessDetails();
    void setAmplificationSuccessDetails(String amplificationSuccessDetails);

    String getAmplificationMethod();
    void setAmplificationMethod(String amplificationMethod);

    String getPrimerSequenceForward();
    void setPrimerSequenceForward(String primerSequenceForward);

    String getPrimerNameForward();
    void setPrimerNameForward(String primerNameForward);

    String getPrimerReferenceCitationForward();
    void setPrimerReferenceCitationForward(String primerReferenceCitationForward);

    String getPrimerReferenceLinkForward();
    void setPrimerReferenceLinkForward(String primerReferenceLinkForward);

    String getPrimerSequenceReverse();
    void setPrimerSequenceReverse(String primerSequenceReverse);

    String getPrimerNameReverse();
    void setPrimerNameReverse(String primerNameReverse);

    String getPrimerReferenceCitationReverse();
    void setPrimerReferenceCitationReverse(String primerReferenceCitationReverse);

    String getPrimerReferenceLinkReverse();
    void setPrimerReferenceLinkReverse(String primerReferenceLinkReverse);

    String getPurificationMethod();
    void setPurificationMethod(String purificationMethod);

    String getConsensusSequence();
    void setConsensusSequence(String consensusSequence);

    String getConsensusSequenceLength();
    void setConsensusSequenceLength(String consensusSequenceLength);

    String getConsensusSequenceChromatogramFileUri();
    void setConsensusSequenceChromatogramFileUri(String consensusSequenceChromatogramFileUri);

    String getBarcodeSequence();
    void setBarcodeSequence(String barcodeSequence);

    String getHaplotype();
    void setHaplotype(String haplotype);

    String getMarker();
    void setMarker(String marker);

    String getMarkerSubfragment();
    void setMarkerSubfragment(String markerSubfragment);

    String getGeneticAccessionNumber();
    void setGeneticAccessionNumber(String geneticAccessionNumber);

    String getBoldProcessId();
    void setBoldProcessId(String boldProcessId);

    String getGeneticAccessionUri();
    void setGeneticAccessionUri(String geneticAccessionUri);

    String getGcContent();
    void setGcContent(String gcContent);

    String getChimeraCheck();
    void setChimeraCheck(String chimeraCheck);

    String getAssembly();
    void setAssembly(String assembly);

    String getSop();
    void setSop(String sop);

    String getFinishingStrategy();
    void setFinishingStrategy(String finishingStrategy);

    String getAnnotSource();
    void setAnnotSource(String annotSource);

    String getMarkerAccordance();
    void setMarkerAccordance(String markerAccordance);

    String getSeqQualityCheck();
    void setSeqQualityCheck(String seqQualityCheck);

    String getAdapters();
    void setAdapters(String adapters);

    String getMid();
    void setMid(String mid);

    BlastResult getBlastResult();
    void setBlastResult(BlastResult blastResult);
}
