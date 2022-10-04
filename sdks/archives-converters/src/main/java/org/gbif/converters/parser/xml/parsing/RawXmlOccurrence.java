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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.OccurrenceSchemaType;

@Slf4j
@Getter
@Setter
public class RawXmlOccurrence implements Serializable {

  private static final long serialVersionUID = -162646273274391257L;

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
      log.error("Encoding error creating hash - must be JVM problem", e);
    }

    return hash;
  }
}
