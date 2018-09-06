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

import org.gbif.converters.parser.xml.constants.PrioritizedPropertyNameEnum;

public class PrioritizedProperty {

  private Integer priority;
  private PrioritizedPropertyNameEnum name;
  private String property;

  public Integer getPriority() {
    return priority;
  }

  public void setPriority(Integer priority) {
    this.priority = priority;
  }

  public PrioritizedPropertyNameEnum getName() {
    return name;
  }

  public void setName(PrioritizedPropertyNameEnum name) {
    this.name = name;
  }

  public String getProperty() {
    return property;
  }

  public void setProperty(String property) {
    this.property = property;
  }

  public String debugDump() {
    return "PrioritizedProperty [priority="
        + priority
        + ", name="
        + name
        + ", property="
        + property
        + "]";
  }
}
