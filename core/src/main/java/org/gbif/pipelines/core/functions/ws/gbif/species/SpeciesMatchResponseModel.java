package org.gbif.pipelines.core.functions.ws.gbif.species;

import com.google.gson.annotations.SerializedName;
import java.io.Serializable;

/*
 * Response model for the WS call
 * */
class SpeciesMatchResponseModel implements Serializable {

  private static final long serialVersionUID = 9048332708516275163L;

  private int usageKey;
  private String scientificName;
  private String canonicalName;
  private String rank;
  private String status;
  private int confidence;
  private String note;
  private String matchType;
  private SpeciesMatchResponseModel[] alternatives;
  private String kingdom;
  private String phylum;
  private String order;
  private String family;
  private String genus;
  private int kingdomKey;
  private int phylumKey;
  private int classKey;
  private int orderKey;
  private int familyKey;
  private int genusKey;
  private boolean synonym;
  @SerializedName("class")
  private String clazz;

  int getUsageKey() {
    return usageKey;
  }

  void setUsageKey(int usageKey) {
    this.usageKey = usageKey;
  }

  String getScientificName() {
    return scientificName;
  }

  void setScientificName(String scientificName) {
    this.scientificName = scientificName;
  }

  String getCanonicalName() {
    return canonicalName;
  }

  void setCanonicalName(String canonicalName) {
    this.canonicalName = canonicalName;
  }

  String getRank() {
    return rank;
  }

  void setRank(String rank) {
    this.rank = rank;
  }

  String getStatus() {
    return status;
  }

  void setStatus(String status) {
    this.status = status;
  }

  int getConfidence() {
    return confidence;
  }

  void setConfidence(int confidence) {
    this.confidence = confidence;
  }

  String getNote() {
    return note;
  }

  void setNote(String note) {
    this.note = note;
  }

  String getMatchType() {
    return matchType;
  }

  void setMatchType(String matchType) {
    this.matchType = matchType;
  }

  SpeciesMatchResponseModel[] getAlternatives() {
    return alternatives;
  }

  void setAlternatives(SpeciesMatchResponseModel[] alternatives) {
    this.alternatives = alternatives;
  }

  String getKingdom() {
    return kingdom;
  }

  void setKingdom(String kingdom) {
    this.kingdom = kingdom;
  }

  String getPhylum() {
    return phylum;
  }

  void setPhylum(String phylum) {
    this.phylum = phylum;
  }

  String getOrder() {
    return order;
  }

  void setOrder(String order) {
    this.order = order;
  }

  String getFamily() {
    return family;
  }

  void setFamily(String family) {
    this.family = family;
  }

  String getGenus() {
    return genus;
  }

  void setGenus(String genus) {
    this.genus = genus;
  }

  int getKingdomKey() {
    return kingdomKey;
  }

  void setKingdomKey(int kingdomKey) {
    this.kingdomKey = kingdomKey;
  }

  int getPhylumKey() {
    return phylumKey;
  }

  void setPhylumKey(int phylumKey) {
    this.phylumKey = phylumKey;
  }

  int getClassKey() {
    return classKey;
  }

  void setClassKey(int classKey) {
    this.classKey = classKey;
  }

  int getOrderKey() {
    return orderKey;
  }

  void setOrderKey(int orderKey) {
    this.orderKey = orderKey;
  }

  int getFamilyKey() {
    return familyKey;
  }

  void setFamilyKey(int familyKey) {
    this.familyKey = familyKey;
  }

  int getGenusKey() {
    return genusKey;
  }

  void setGenusKey(int genusKey) {
    this.genusKey = genusKey;
  }

  boolean isSynonym() {
    return synonym;
  }

  void setSynonym(boolean synonym) {
    this.synonym = synonym;
  }

  String getClazz() {
    return clazz;
  }

  void setClazz(String clazz) {
    this.clazz = clazz;
  }
}
