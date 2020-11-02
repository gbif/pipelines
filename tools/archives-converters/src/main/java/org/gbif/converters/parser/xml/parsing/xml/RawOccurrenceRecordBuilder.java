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
package org.gbif.converters.parser.xml.parsing.xml;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.gbif.converters.parser.xml.constants.PrioritizedPropertyNameEnum;
import org.gbif.converters.parser.xml.model.Identification;
import org.gbif.converters.parser.xml.model.IdentifierRecord;
import org.gbif.converters.parser.xml.model.ImageRecord;
import org.gbif.converters.parser.xml.model.LinkRecord;
import org.gbif.converters.parser.xml.model.PropertyPrioritizer;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.model.TypificationRecord;

/**
 * This object is the one that gets populated by Digester when parsing raw xml records into
 * RawOccurrenceRecords. Because some of the xml schemas allow multiple identification records, and
 * we interpret each one as its own RawOccurrenceRecord, this class needs to generate the correct
 * number of RawOccurrenceRecords based on the input xml. In some schemas there are also different
 * ways of representing the same data (eg decimal latitude vs text latitude) and we prefer the more
 * accurate version when we can get it. If more than one representation for a given property is
 * populated we have to take the one with highest priority - a mechanism that is inherited from
 * PropertyPrioritizer.
 */
@Slf4j
public class RawOccurrenceRecordBuilder extends PropertyPrioritizer {

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
  private String basisOfRecord;
  private String identifierName;
  private String unitQualifier;

  // year month and day may be set during parsing, but they will be reconciled into an
  // occurrenceDate
  // during generateRawOccurrenceRecords and that date string will be set on the resulting
  // RawOccurrenceRecord(s).
  private String year;
  private String month;
  private String day;
  private String occurrenceDate;

  // same again as occurrenceDate
  private String yearIdentified;
  private String monthIdentified;
  private String dayIdentified;
  private String dateIdentified;

  private List<IdentifierRecord> identifierRecords = Lists.newArrayList();
  private List<TypificationRecord> typificationRecords = Lists.newArrayList();
  private List<Identification> identifications = Lists.newArrayList();
  private List<ImageRecord> images = Lists.newArrayList();
  private List<LinkRecord> links = Lists.newArrayList();

  public List<RawOccurrenceRecord> generateRawOccurrenceRecords() {
    List<RawOccurrenceRecord> records = Lists.newArrayList();

    // reconcile year/month/date into an occurrenceDate string, if necessary
    occurrenceDate = reconcileDate(occurrenceDate, year, month, day);
    dateIdentified = reconcileDate(dateIdentified, yearIdentified, monthIdentified, dayIdentified);

    // if multiple valid identifications, generate multiple RoRs and set UnitQualifier to
    // Identification's SciName
    if (!identifications.isEmpty()) {
      if (identifications.size() == 1) {
        RawOccurrenceRecord bareBones = generateBareRor();
        Identification ident = identifications.get(0);
        ident.populateRawOccurrenceRecord(bareBones);
        records.add(bareBones);
      } else {
        // take any that have preferred flag set
        // if no preferred flags are set, take them all
        boolean gotPreferred = false;
        List<RawOccurrenceRecord> preferred = Lists.newArrayList();
        for (Identification ident : identifications) {
          RawOccurrenceRecord bareBones = generateBareRor();
          ident.populateRawOccurrenceRecord(bareBones, true);
          if (ident.isPreferred()) {
            gotPreferred = true;
            preferred.add(bareBones);
          } else {
            records.add(bareBones);
          }
        }
        if (gotPreferred) {
          records = preferred;
        }
      }
    } else {
      // no identifications, take bare bones only
      RawOccurrenceRecord bareBones = generateBareRor();
      records.add(bareBones);
    }

    // if we have exactly one final record and an "occurrenceId" identifier, add it to the record
    if (records.size() == 1) {
      for (IdentifierRecord id : identifierRecords) {
        if (id.getIdentifierType() == IdentifierRecord.OCCURRENCE_ID_TYPE) {
          records.get(0).setId(id.getIdentifier());
          break;
        }
      }
    }

    return records;
  }

  private static String reconcileDate(
      String reconciledDate, String year, String month, String day) {
    if (Strings.isNullOrEmpty(reconciledDate) && !Strings.isNullOrEmpty(year)) {
      reconciledDate = year;
      if (!Strings.isNullOrEmpty(month)) {
        reconciledDate = reconciledDate + "-" + month;
        if (!Strings.isNullOrEmpty(day)) reconciledDate = reconciledDate + "-" + day;
      }
    }

    return reconciledDate;
  }

  private RawOccurrenceRecord generateBareRor() {
    RawOccurrenceRecord bareBones = new RawOccurrenceRecord();
    bareBones.setAltitudePrecision(altitudePrecision);
    bareBones.setAuthor(author);
    bareBones.setBasisOfRecord(basisOfRecord);
    bareBones.setCatalogueNumber(catalogueNumber);
    bareBones.setCollectionCode(collectionCode);
    bareBones.setCollectorName(collectorName);
    bareBones.setCollectorsFieldNumber(collectorsFieldNumber);
    bareBones.setContinentOrOcean(continentOrOcean);
    bareBones.setCountry(country);
    bareBones.setCounty(county);
    bareBones.setDataProviderId(dataProviderId);
    bareBones.setDataResourceId(dataResourceId);
    bareBones.setYearIdentified(yearIdentified);
    bareBones.setMonthIdentified(monthIdentified);
    bareBones.setDayIdentified(dayIdentified);
    bareBones.setDateIdentified(dateIdentified);
    bareBones.setDepthPrecision(depthPrecision);
    bareBones.setFamily(family);
    bareBones.setGenus(genus);
    bareBones.setGeodeticDatum(geodeticDatum);
    bareBones.setIdentifierName(identifierName);
    bareBones.setIdentifierRecords(identifierRecords);
    bareBones.setImageRecords(images);
    bareBones.setInstitutionCode(institutionCode);
    bareBones.setKingdom(kingdom);
    bareBones.setKlass(klass);
    bareBones.setLatitude(latitude);
    bareBones.setLatLongPrecision(latLongPrecision);
    bareBones.setLinkRecords(links);
    bareBones.setLocality(locality);
    bareBones.setLongitude(longitude);
    bareBones.setMaxAltitude(maxAltitude);
    bareBones.setMaxDepth(maxDepth);
    bareBones.setMinAltitude(minAltitude);
    bareBones.setMinDepth(minDepth);
    bareBones.setYear(year);
    bareBones.setMonth(month);
    bareBones.setDay(day);
    bareBones.setOccurrenceDate(occurrenceDate);
    bareBones.setOrder(order);
    bareBones.setPhylum(phylum);
    bareBones.setRank(rank);
    bareBones.setResourceAccessPointId(resourceAccessPointId);
    bareBones.setScientificName(scientificName);
    bareBones.setSpecies(species);
    bareBones.setStateOrProvince(stateOrProvince);
    bareBones.setSubspecies(subspecies);
    bareBones.setTypificationRecords(typificationRecords);
    bareBones.setUnitQualifier(unitQualifier);

    return bareBones;
  }

  public void addIdentification(Identification ident) {
    this.identifications.add(ident);
  }

  public void addTypification(
      String scientificName, String publication, String typeStatus, String notes) {
    log.debug(">> addTypification");

    TypificationRecord typRec =
        new TypificationRecord(scientificName, publication, typeStatus, notes);

    if (!typRec.isEmpty()) {
      typificationRecords.add(typRec);
    }

    log.debug("<< addTypification");
  }

  private void addIdentifier(Integer identifierType, String identifier) {
    log.debug(
        "Attempting add of Identifier record with type [{}] and body [{}]",
        identifierType,
        identifier);
    if (!identifier.trim().isEmpty()) {
      IdentifierRecord idRec = new IdentifierRecord();
      idRec.setIdentifier(identifier);
      idRec.setIdentifierType(identifierType);
      identifierRecords.add(idRec);
    }
  }

  public void addImage(ImageRecord image) {
    if (image != null && !image.isEmpty()) {
      images.add(image);
    }
  }

  public void addLink(LinkRecord link) {
    if (link != null && !link.isEmpty()) {
      links.add(link);
    }
  }

  /**
   * Once this object has been populated by a Digester, there may be several PrioritizedProperties
   * that need to be resolved, and thereby set the final value of the corresponding field on this
   * object.
   */
  @Override
  public void resolvePriorities() {
    for (Map.Entry<PrioritizedPropertyNameEnum, Set<PrioritizedProperty>> property :
        prioritizedProps.entrySet()) {
      String result = findHighestPriority(property.getValue());
      switch (property.getKey()) {
        case CATALOGUE_NUMBER:
          this.catalogueNumber = result;
          break;
        case COLLECTOR_NAME:
          this.collectorName = result;
          break;
        case CONTINENT_OR_OCEAN:
          this.continentOrOcean = result;
          break;
        case COUNTRY:
          this.country = result;
          break;
        case DATE_COLLECTED:
          this.occurrenceDate = result;
          break;
        case LATITUDE:
          this.latitude = result;
          break;
        case LONGITUDE:
          this.longitude = result;
          break;
        default:
          log.warn("Fell through priority resolution for [{}]", property.getKey());
      }
    }

    for (Identification ident : identifications) {
      ident.resolvePriorities();
    }
    for (ImageRecord image : images) {
      image.resolvePriorities();
    }
  }

  public void setIdentifierType1(String identifier) {
    addIdentifier(1, identifier);
  }

  public void setIdentifierType2(String identifier) {
    addIdentifier(2, identifier);
  }

  public void setIdentifierType3(String identifier) {
    addIdentifier(3, identifier);
  }

  public void setIdentifierType4(String identifier) {
    addIdentifier(4, identifier);
  }

  public void setIdentifierType5(String identifier) {
    addIdentifier(5, identifier);
  }

  public void setIdentifierType6(String identifier) {
    addIdentifier(6, identifier);
  }

  public void setIdentifierType7(String identifier) {
    addIdentifier(7, identifier);
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

  public Integer getResourceAccessPointId() {
    return resourceAccessPointId;
  }

  public void setResourceAccessPointId(Integer resourceAccessPointId) {
    this.resourceAccessPointId = resourceAccessPointId;
  }

  public String getInstitutionCode() {
    return institutionCode;
  }

  public void setInstitutionCode(String institutionCode) {
    this.institutionCode = institutionCode;
  }

  public String getCollectionCode() {
    return collectionCode;
  }

  public void setCollectionCode(String collectionCode) {
    this.collectionCode = collectionCode;
  }

  public String getCatalogueNumber() {
    return catalogueNumber;
  }

  public void setCatalogueNumber(String catalogueNumber) {
    this.catalogueNumber = catalogueNumber;
  }

  public String getScientificName() {
    return scientificName;
  }

  public void setScientificName(String scientificName) {
    this.scientificName = scientificName;
  }

  public String getAuthor() {
    return author;
  }

  public void setAuthor(String author) {
    this.author = author;
  }

  public String getRank() {
    return rank;
  }

  public void setRank(String rank) {
    this.rank = rank;
  }

  public String getKingdom() {
    return kingdom;
  }

  public void setKingdom(String kingdom) {
    this.kingdom = kingdom;
  }

  public String getPhylum() {
    return phylum;
  }

  public void setPhylum(String phylum) {
    this.phylum = phylum;
  }

  public String getKlass() {
    return klass;
  }

  public void setKlass(String klass) {
    this.klass = klass;
  }

  public String getOrder() {
    return order;
  }

  public void setOrder(String order) {
    this.order = order;
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

  public String getSpecies() {
    return species;
  }

  public void setSpecies(String species) {
    this.species = species;
  }

  public String getSubspecies() {
    return subspecies;
  }

  public void setSubspecies(String subspecies) {
    this.subspecies = subspecies;
  }

  public String getLatitude() {
    return latitude;
  }

  public void setLatitude(String latitude) {
    this.latitude = latitude;
  }

  public String getLongitude() {
    return longitude;
  }

  public void setLongitude(String longitude) {
    this.longitude = longitude;
  }

  public String getLatLongPrecision() {
    return latLongPrecision;
  }

  public void setLatLongPrecision(String latLongPrecision) {
    this.latLongPrecision = latLongPrecision;
  }

  public String getMinAltitude() {
    return minAltitude;
  }

  public void setMinAltitude(String minAltitude) {
    this.minAltitude = minAltitude;
  }

  public String getMaxAltitude() {
    return maxAltitude;
  }

  public void setMaxAltitude(String maxAltitude) {
    this.maxAltitude = maxAltitude;
  }

  public String getAltitudePrecision() {
    return altitudePrecision;
  }

  public void setAltitudePrecision(String altitudePrecision) {
    this.altitudePrecision = altitudePrecision;
  }

  public String getMinDepth() {
    return minDepth;
  }

  public void setMinDepth(String minDepth) {
    this.minDepth = minDepth;
  }

  public String getMaxDepth() {
    return maxDepth;
  }

  public void setMaxDepth(String maxDepth) {
    this.maxDepth = maxDepth;
  }

  public String getDepthPrecision() {
    return depthPrecision;
  }

  public void setDepthPrecision(String depthPrecision) {
    this.depthPrecision = depthPrecision;
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

  public String getStateOrProvince() {
    return stateOrProvince;
  }

  public void setStateOrProvince(String stateOrProvince) {
    this.stateOrProvince = stateOrProvince;
  }

  public String getCounty() {
    return county;
  }

  public void setCounty(String county) {
    this.county = county;
  }

  public String getCollectorName() {
    return collectorName;
  }

  public void setCollectorName(String collectorName) {
    this.collectorName = collectorName;
  }

  public String getLocality() {
    return locality;
  }

  public void setLocality(String locality) {
    this.locality = locality;
  }

  public void setYear(String year) {
    this.year = year;
  }

  public void setMonth(String month) {
    this.month = month;
  }

  public void setDay(String day) {
    this.day = day;
  }

  public String getBasisOfRecord() {
    return basisOfRecord;
  }

  public void setBasisOfRecord(String basisOfRecord) {
    this.basisOfRecord = basisOfRecord;
  }

  public String getIdentifierName() {
    return identifierName;
  }

  public void setIdentifierName(String identifierName) {
    this.identifierName = identifierName;
  }

  public String getGeodeticDatum() {
    return geodeticDatum;
  }

  public void setGeodeticDatum(String geodeticDatum) {
    this.geodeticDatum = geodeticDatum;
  }

  public String getDateIdentified() {
    return dateIdentified;
  }

  public void setDateIdentified(String year, String month, String day) {
    // build our own "verbatim" string
    String result = null;
    if (!Strings.isNullOrEmpty(year)) {
      result = year;
      if (!Strings.isNullOrEmpty(month)) {
        result = result + "-" + month;
        if (!Strings.isNullOrEmpty(day)) result = result + "-" + day;
      }
    }
    if (result != null) this.dateIdentified = result;
  }

  public void setDateIdentified(String dateIdentified) {
    this.dateIdentified = dateIdentified;
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

  public List<Identification> getIdentifications() {
    return identifications;
  }

  public void setIdentifications(List<Identification> identifications) {
    this.identifications = identifications;
  }

  public List<ImageRecord> getImages() {
    return images;
  }

  public void setImages(List<ImageRecord> images) {
    this.images = images;
  }

  public String getOccurrenceDate() {
    return occurrenceDate;
  }

  public void setOccurrenceDate(String occurrenceDate) {
    this.occurrenceDate = occurrenceDate;
  }

  public void setYearIdentified(String yearIdentified) {
    this.yearIdentified = yearIdentified;
  }

  public void setMonthIdentified(String monthIdentified) {
    this.monthIdentified = monthIdentified;
  }

  public void setDayIdentified(String dayIdentified) {
    this.dayIdentified = dayIdentified;
  }

  public String getCollectorsFieldNumber() {
    return collectorsFieldNumber;
  }

  public void setCollectorsFieldNumber(String collectorsFieldNumber) {
    this.collectorsFieldNumber = collectorsFieldNumber;
  }
}
