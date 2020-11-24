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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.DwcTerm;

/**
 * This is mostly cut and paste from synchronizer-gbif, intended as a place holder until this
 * project is integrated with the main synchronizer process. Differences from sync-gbif are that id
 * and dateIdentified are String, and occurenceDate is retained as a verbatim string rather than
 * parsed to year, month and day.
 */
@NoArgsConstructor
@Getter
@Setter
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
    this.latitude =
        Optional.ofNullable(dwcr.value(DwcTerm.verbatimLatitude))
            .orElse(dwcr.value(DwcTerm.decimalLatitude));
    this.longitude =
        Optional.ofNullable(dwcr.value(DwcTerm.verbatimLongitude))
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
}
