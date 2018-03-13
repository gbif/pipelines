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
package org.gbif.xml.occurrence.parser.model;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

public class TypificationRecord implements Serializable {

  private static final long serialVersionUID = 9038478890028911433L;

  private String scientificName;
  private String publication;
  private String typeStatus;
  private String notes;

  public TypificationRecord() {
  }

  public TypificationRecord(String scientificName, String publication, String typeStatus, String notes) {
    if (!StringUtils.isEmpty(scientificName)) {
      this.scientificName = scientificName;
    }
    if (!StringUtils.isEmpty(publication)) {
      this.publication = publication;
    }
    if (!StringUtils.isEmpty(typeStatus)) {
      this.typeStatus = typeStatus;
    }
    if (!StringUtils.isEmpty(notes)) {
      this.notes = notes;
    }
  }

  public boolean isEmpty() {
    return StringUtils.isEmpty(notes)
           && StringUtils.isEmpty(typeStatus)
           && StringUtils.isEmpty(publication)
           && StringUtils.isEmpty(scientificName);
  }

  public String debugDump() {
    return "ScientificName ["
           + scientificName
           + "]\n"
           + "Publication ["
           + publication
           + "]\n"
           + "TypeStatus ["
           + typeStatus
           + "]\n"
           + "Notes ["
           + notes
           + "]\n";
  }

  public String getScientificName() {
    return scientificName;
  }

  public void setScientificName(String scientificName) {
    this.scientificName = scientificName;
  }

  public String getPublication() {
    return publication;
  }

  public void setPublication(String publication) {
    this.publication = publication;
  }

  public String getTypeStatus() {
    return typeStatus;
  }

  public void setTypeStatus(String typeStatus) {
    this.typeStatus = typeStatus;
  }

  public String getNotes() {
    return notes;
  }

  public void setNotes(String notes) {
    this.notes = notes;
  }


}
