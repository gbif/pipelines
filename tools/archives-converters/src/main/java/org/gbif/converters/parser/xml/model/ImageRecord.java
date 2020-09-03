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
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.gbif.converters.parser.xml.constants.PrioritizedPropertyNameEnum;
import org.gbif.converters.parser.xml.parsing.xml.PrioritizedProperty;

/** Holds a single image for a RawOccurrenceRecord. */
@Slf4j
@Getter
@Setter
public class ImageRecord extends PropertyPrioritizer implements Serializable {

  private static final long serialVersionUID = -4610903747490605198L;

  private String rawImageType;
  private Integer imageType;
  private String url;
  private String pageUrl;
  private String description;
  private String rights;
  private String htmlForDisplay;

  /**
   * Once this object has been populated by a Digester, there may be several PrioritizedProperties
   * that need to be resolved, and thereby set the final value of the corresponding field on this
   * object.
   */
  @Override
  public void resolvePriorities() {
    for (Map.Entry<PrioritizedPropertyNameEnum, Set<PrioritizedProperty>> entry :
        prioritizedProps.entrySet()) {
      PrioritizedPropertyNameEnum name = entry.getKey();
      String result = findHighestPriority(prioritizedProps.get(name));
      switch (name) {
        case IMAGE_URL:
          url = result;
          break;
        case IMAGE_RIGHTS:
          rights = result;
          break;
        default:
          log.warn("Fell through priority resolution for [{}]", name);
      }
    }
  }

  public boolean isEmpty() {
    return Strings.isNullOrEmpty(rawImageType)
        && imageType == null
        && Strings.isNullOrEmpty(url)
        && Strings.isNullOrEmpty(pageUrl)
        && Strings.isNullOrEmpty(description)
        && Strings.isNullOrEmpty(rights)
        && Strings.isNullOrEmpty(htmlForDisplay);
  }
}
