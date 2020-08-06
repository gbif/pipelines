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

import com.google.common.base.Strings;
import java.io.Serializable;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class TypificationRecord implements Serializable {

  private static final long serialVersionUID = 9038478890028911433L;

  private String scientificName;
  private String publication;
  private String typeStatus;
  private String notes;

  public TypificationRecord(
      String scientificName, String publication, String typeStatus, String notes) {
    if (!Strings.isNullOrEmpty(scientificName)) {
      this.scientificName = scientificName;
    }
    if (!Strings.isNullOrEmpty(publication)) {
      this.publication = publication;
    }
    if (!Strings.isNullOrEmpty(typeStatus)) {
      this.typeStatus = typeStatus;
    }
    if (!Strings.isNullOrEmpty(notes)) {
      this.notes = notes;
    }
  }

  public boolean isEmpty() {
    return Strings.isNullOrEmpty(notes)
        && Strings.isNullOrEmpty(typeStatus)
        && Strings.isNullOrEmpty(publication)
        && Strings.isNullOrEmpty(scientificName);
  }
}
