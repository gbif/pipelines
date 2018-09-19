/*
 * Copyright 2011 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.converters.parser.xml.model;

import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.DwcTerm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This is mostly cut and paste from synchronizer-gbif, intended as a place holder until this
 * project is integrated with the main synchronizer process. Differences from sync-gbif are that id
 * and dateIdentified are String, and occurenceDate is retained as a verbatim string rather than
 * parsed to year, month and day.
 */
public class RawOccurrenceRecord implements Serializable {

  private static final long serialVersionUID = -2763668230804275054L;

  private String id;
  private Integer dataProviderId;
  private Integer dataResourceId;
  private Integer resourceAccessPointId;
  private String institutionCode;
  private String collectionCode;
  private String catalogueNumber;
  private String scientificName;
  private String author;
  private String rank;
  private String kingdom;
  private String phylum;
  private String klass;
  private String order;
  private String family;
  private String genus;
  private String species;
  private String subspecies;
  private String latitude;
  private String longitude;
  private String latLongPrecision;
  private String geodeticDatum;
  private String minAltitude;
  private String maxAltitude;
  private String altitudePrecision;
  private String minDepth;
  private String maxDepth;
  private String depthPrecision;
  private String continentOrOcean;
  private String country;
  private String stateOrProvince;
  private String county;
  private String collectorName;
  private String collectorsFieldNumber;
  private String locality;
  private String year;
  private String month;
  private String day;
  private String occurrenceDate;
  private String basisOfRecord;
  private String identifierName;
  private String yearIdentified;
  private String monthIdentified;
  private String dayIdentified;
  private String dateIdentified;
  private String unitQualifier;
  private long created;
  private long modified;

  private List<IdentifierRecord> identifierRecords = new ArrayList<>();
  private List<TypificationRecord> typificationRecords = new ArrayList<>();
  private List<ImageRecord> imageRecords = new ArrayList<>();
  private List<LinkRecord> linkRecords = new ArrayList<>();

  /** Default */
  public RawOccurrenceRecord() {}

  /** TODO: handle supporting table records & maybe dwca extensions? */
  public RawOccurrenceRecord(Record dwcr) {
    this.basisOfRecord = dwcr.value(DwcTerm.basisOfRecord);
    this.catalogueNumber = dwcr.value(DwcTerm.catalogNumber);
    this.klass = dwcr.value(DwcTerm.class_);
    this.collectionCode = dwcr.value(DwcTerm.collectionCode);
    this.continentOrOcean = dwcr.value(DwcTerm.continent);
    this.country =
        dwcr.value(DwcTerm.country) == null || dwcr.value(DwcTerm.country).isEmpty()
            ? dwcr.value(DwcTerm.countryCode)
            : dwcr.value(DwcTerm.country);
    this.county = dwcr.value(DwcTerm.county);
    this.dateIdentified = dwcr.value(DwcTerm.dateIdentified);
    this.latitude = Optional.ofNullable(dwcr.value(DwcTerm.verbatimLatitude))
            .orElse(dwcr.value(DwcTerm.decimalLatitude));
    this.longitude = Optional.ofNullable(dwcr.value(DwcTerm.verbatimLongitude))
            .orElse(dwcr.value(DwcTerm.decimalLongitude));
    this.geodeticDatum = dwcr.value(DwcTerm.geodeticDatum);
    this.family = dwcr.value(DwcTerm.family);
    this.scientificName = dwcr.value(DwcTerm.scientificName);
    this.genus = dwcr.value(DwcTerm.genus);
    this.identifierName = dwcr.value(DwcTerm.identifiedBy);
    this.institutionCode = dwcr.value(DwcTerm.institutionCode);
    this.kingdom = dwcr.value(DwcTerm.kingdom);
    this.maxDepth = dwcr.value(DwcTerm.maximumDepthInMeters);
    this.minDepth = dwcr.value(DwcTerm.minimumDepthInMeters);
    this.maxAltitude = dwcr.value(DwcTerm.maximumElevationInMeters);
    this.minAltitude = dwcr.value(DwcTerm.minimumElevationInMeters);
    this.order = dwcr.value(DwcTerm.order);
    this.phylum = dwcr.value(DwcTerm.phylum);
    this.occurrenceDate =
        dwcr.value(DwcTerm.year) + '-' + dwcr.value(DwcTerm.month) + '-' + dwcr.value(DwcTerm.day);
    this.collectorsFieldNumber = dwcr.value(DwcTerm.recordNumber);
  }

  public String getAltitudePrecision() {
    return altitudePrecision;
  }

  public void setAltitudePrecision(String altitudePrecision) {
    this.altitudePrecision = altitudePrecision;
  }

  public String getAuthor() {
    return author;
  }

  public void setAuthor(String author) {
    this.author = author;
  }

  public String getBasisOfRecord() {
    return basisOfRecord;
  }

  public void setBasisOfRecord(String basisOfRecord) {
    this.basisOfRecord = basisOfRecord;
  }

  public String getCatalogueNumber() {
    return catalogueNumber;
  }

  public void setCatalogueNumber(String catalogueNumber) {
    this.catalogueNumber = catalogueNumber;
  }

  public String getCollectionCode() {
    return collectionCode;
  }

  public void setCollectionCode(String collectionCode) {
    this.collectionCode = collectionCode;
  }

  public String getCollectorName() {
    return collectorName;
  }

  public void setCollectorName(String collectorName) {
    this.collectorName = collectorName;
  }

  public String getContinentOrOcean() {
    return continentOrOcean;
  }

  public void setContinentOrOcean(String continentOrOcean) {
    this.continentOrOcean = continentOrOcean;
  }

  public String getCountry() {
    return country;
  }

  public void setCountry(String country) {
    this.country = country;
  }

  public String getCounty() {
    return county;
  }

  public void setCounty(String county) {
    this.county = county;
  }

  public long getCreated() {
    return created;
  }

  public void setCreated(long created) {
    this.created = created;
  }

  public Integer getDataProviderId() {
    return dataProviderId;
  }

  public void setDataProviderId(Integer dataProviderId) {
    this.dataProviderId = dataProviderId;
  }

  public Integer getDataResourceId() {
    return dataResourceId;
  }

  public void setDataResourceId(Integer dataResourceId) {
    this.dataResourceId = dataResourceId;
  }

  public String getDateIdentified() {
    return dateIdentified;
  }

  public void setDateIdentified(String dateIdentified) {
    this.dateIdentified = dateIdentified;
  }

  public String getDepthPrecision() {
    return depthPrecision;
  }

  public void setDepthPrecision(String depthPrecision) {
    this.depthPrecision = depthPrecision;
  }

  public String getFamily() {
    return family;
  }

  public void setFamily(String family) {
    this.family = family;
  }

  public String getGenus() {
    return genus;
  }

  public void setGenus(String genus) {
    this.genus = genus;
  }

  public String getGeodeticDatum() {
    return geodeticDatum;
  }

  public void setGeodeticDatum(String geodeticDatum) {
    this.geodeticDatum = geodeticDatum;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getIdentifierName() {
    return identifierName;
  }

  public void setIdentifierName(String identifierName) {
    this.identifierName = identifierName;
  }

  public String getInstitutionCode() {
    return institutionCode;
  }

  public void setInstitutionCode(String institutionCode) {
    this.institutionCode = institutionCode;
  }

  public String getKingdom() {
    return kingdom;
  }

  public void setKingdom(String kingdom) {
    this.kingdom = kingdom;
  }

  public String getKlass() {
    return klass;
  }

  public void setKlass(String klass) {
    this.klass = klass;
  }

  public String getLatitude() {
    return latitude;
  }

  public void setLatitude(String latitude) {
    this.latitude = latitude;
  }

  public String getLatLongPrecision() {
    return latLongPrecision;
  }

  public void setLatLongPrecision(String latLongPrecision) {
    this.latLongPrecision = latLongPrecision;
  }

  public String getLocality() {
    return locality;
  }

  public void setLocality(String locality) {
    this.locality = locality;
  }

  public String getLongitude() {
    return longitude;
  }

  public void setLongitude(String longitude) {
    this.longitude = longitude;
  }

  public String getMaxAltitude() {
    return maxAltitude;
  }

  public void setMaxAltitude(String maxAltitude) {
    this.maxAltitude = maxAltitude;
  }

  public String getMaxDepth() {
    return maxDepth;
  }

  public void setMaxDepth(String maxDepth) {
    this.maxDepth = maxDepth;
  }

  public String getMinAltitude() {
    return minAltitude;
  }

  public void setMinAltitude(String minAltitude) {
    this.minAltitude = minAltitude;
  }

  public String getMinDepth() {
    return minDepth;
  }

  public void setMinDepth(String minDepth) {
    this.minDepth = minDepth;
  }

  public long getModified() {
    return modified;
  }

  public void setModified(long modified) {
    this.modified = modified;
  }

  public String getOrder() {
    return order;
  }

  public void setOrder(String order) {
    this.order = order;
  }

  public String getPhylum() {
    return phylum;
  }

  public void setPhylum(String phylum) {
    this.phylum = phylum;
  }

  public String getRank() {
    return rank;
  }

  public void setRank(String rank) {
    this.rank = rank;
  }

  public Integer getResourceAccessPointId() {
    return resourceAccessPointId;
  }

  public void setResourceAccessPointId(Integer resourceAccessPointId) {
    this.resourceAccessPointId = resourceAccessPointId;
  }

  public String getScientificName() {
    return scientificName;
  }

  public void setScientificName(String scientificName) {
    this.scientificName = scientificName;
  }

  public String getSpecies() {
    return species;
  }

  public void setSpecies(String species) {
    this.species = species;
  }

  public String getStateOrProvince() {
    return stateOrProvince;
  }

  public void setStateOrProvince(String stateOrProvince) {
    this.stateOrProvince = stateOrProvince;
  }

  public String getSubspecies() {
    return subspecies;
  }

  public void setSubspecies(String subspecies) {
    this.subspecies = subspecies;
  }

  public String getUnitQualifier() {
    return unitQualifier;
  }

  public void setUnitQualifier(String unitQualifier) {
    this.unitQualifier = unitQualifier;
  }

  public List<IdentifierRecord> getIdentifierRecords() {
    return identifierRecords;
  }

  public void setIdentifierRecords(List<IdentifierRecord> identifierRecords) {
    this.identifierRecords = identifierRecords;
  }

  public List<TypificationRecord> getTypificationRecords() {
    return typificationRecords;
  }

  public void setTypificationRecords(List<TypificationRecord> typificationRecords) {
    this.typificationRecords = typificationRecords;
  }

  public List<ImageRecord> getImageRecords() {
    return imageRecords;
  }

  public void setImageRecords(List<ImageRecord> imageRecords) {
    this.imageRecords = imageRecords;
  }

  public List<LinkRecord> getLinkRecords() {
    return linkRecords;
  }

  public void setLinkRecords(List<LinkRecord> linkRecords) {
    this.linkRecords = linkRecords;
  }

  public String getYear() {
    return year;
  }

  public void setYear(String year) {
    this.year = year;
  }

  public String getMonth() {
    return month;
  }

  public void setMonth(String month) {
    this.month = month;
  }

  public String getDay() {
    return day;
  }

  public void setDay(String day) {
    this.day = day;
  }

  public String getYearIdentified() {
    return yearIdentified;
  }

  public void setYearIdentified(String yearIdentified) {
    this.yearIdentified = yearIdentified;
  }

  public String getMonthIdentified() {
    return monthIdentified;
  }

  public void setMonthIdentified(String monthIdentified) {
    this.monthIdentified = monthIdentified;
  }

  public String getDayIdentified() {
    return dayIdentified;
  }

  public void setDayIdentified(String dayIdentified) {
    this.dayIdentified = dayIdentified;
  }

  public String getOccurrenceDate() {
    return occurrenceDate;
  }

  public void setOccurrenceDate(String occurrenceDate) {
    this.occurrenceDate = occurrenceDate;
  }

  public String getCollectorsFieldNumber() {
    return collectorsFieldNumber;
  }

  public void setCollectorsFieldNumber(String collectorsFieldNumber) {
    this.collectorsFieldNumber = collectorsFieldNumber;
  }

  public String debugDump() {
    return "RawOccurrenceRecord [\n id="
        + id
        + ",\n dataProviderId="
        + dataProviderId
        + ",\n dataResourceId="
        + dataResourceId
        + ",\n resourceAccessPointId="
        + resourceAccessPointId
        + ",\n institutionCode="
        + institutionCode
        + ",\n collectionCode="
        + collectionCode
        + ",\n catalogueNumber="
        + catalogueNumber
        + ",\n scientificName="
        + scientificName
        + ",\n author="
        + author
        + ",\n rank="
        + rank
        + ",\n kingdom="
        + kingdom
        + ",\n phylum="
        + phylum
        + ",\n klass="
        + klass
        + ",\n order="
        + order
        + ",\n family="
        + family
        + ",\n genus="
        + genus
        + ",\n species="
        + species
        + ",\n subspecies="
        + subspecies
        + ",\n latitude="
        + latitude
        + ",\n longitude="
        + longitude
        + ",\n latLongPrecision="
        + latLongPrecision
        + ",\n geodeticDatum="
        + geodeticDatum
        + ",\n minAltitude="
        + minAltitude
        + ",\n maxAltitude="
        + maxAltitude
        + ",\n altitudePrecision="
        + altitudePrecision
        + ",\n minDepth="
        + minDepth
        + ",\n maxDepth="
        + maxDepth
        + ",\n depthPrecision="
        + depthPrecision
        + ",\n continentOrOcean="
        + continentOrOcean
        + ",\n country="
        + country
        + ",\n stateOrProvince="
        + stateOrProvince
        + ",\n county="
        + county
        + ",\n collectorName="
        + collectorName
        + ",\n collectorsFieldNumber="
        + collectorsFieldNumber
        + ",\n locality="
        + locality
        + ",\n occurrenceDate="
        + occurrenceDate
        + ",\n basisOfRecord="
        + basisOfRecord
        + ",\n identifierName="
        + identifierName
        + ",\n dateIdentified="
        + dateIdentified
        + ",\n unitQualifier="
        + unitQualifier
        + "]";
  }
}
