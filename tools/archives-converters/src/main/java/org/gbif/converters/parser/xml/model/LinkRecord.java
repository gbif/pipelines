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

import com.google.common.base.Strings;

public class LinkRecord implements Serializable {

  private static final long serialVersionUID = 4281236213527332610L;

  private String rawLinkType;
  private Integer linkType;
  private String url;
  private String description;

  public String getRawLinkType() {
    return rawLinkType;
  }

  public void setRawLinkType(String rawLinkType) {
    this.rawLinkType = rawLinkType;
  }

  public Integer getLinkType() {
    return linkType;
  }

  public void setLinkType(Integer linkType) {
    this.linkType = linkType;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public boolean isEmpty() {
    return Strings.isNullOrEmpty(rawLinkType)
        && Strings.isNullOrEmpty(url)
        && Strings.isNullOrEmpty(description);
  }

  public String debugDump() {
    return "LinkRecord [\nrawLinkType="
        + rawLinkType
        + ",\nlinkType="
        + linkType
        + ",\nurl="
        + url
        + ",\ndescription="
        + description
        + "]";
  }
}
