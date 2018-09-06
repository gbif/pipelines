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
package org.gbif.converters.parser.xml.parsing;

import org.gbif.api.vocabulary.OccurrenceSchemaType;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawXmlOccurrence {

  private static final Logger LOG = LoggerFactory.getLogger(RawXmlOccurrence.class);

  private String resourceName;
  private String institutionCode;
  private String collectionCode;
  private String catalogNumber;
  private String xml;
  private OccurrenceSchemaType schemaType;

  public byte[] getHash() {
    byte[] hash = null;
    try {
      String plain = resourceName + xml;
      byte[] bytesOfMessage = plain.getBytes(StandardCharsets.UTF_8);
      MessageDigest md = MessageDigest.getInstance("MD5");
      hash = md.digest(bytesOfMessage);
    } catch (NoSuchAlgorithmException e) {
      LOG.error("Encoding error creating hash - must be JVM problem", e);
    }

    return hash;
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

  public String getCatalogNumber() {
    return catalogNumber;
  }

  public void setCatalogNumber(String catalogNumber) {
    this.catalogNumber = catalogNumber;
  }

  public String getXml() {
    return xml;
  }

  public void setXml(String xml) {
    this.xml = xml;
  }

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public OccurrenceSchemaType getSchemaType() {
    return schemaType;
  }

  public void setSchemaType(OccurrenceSchemaType schemaType) {
    this.schemaType = schemaType;
  }
}
