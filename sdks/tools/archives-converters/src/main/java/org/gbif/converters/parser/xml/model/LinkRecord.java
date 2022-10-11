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
import lombok.Setter;

@Getter
@Setter
public class LinkRecord implements Serializable {

  private static final long serialVersionUID = 4281236213527332610L;

  private String rawLinkType;
  private Integer linkType;
  private String url;
  private String description;

  public boolean isEmpty() {
    return Strings.isNullOrEmpty(rawLinkType)
        && Strings.isNullOrEmpty(url)
        && Strings.isNullOrEmpty(description);
  }
}
