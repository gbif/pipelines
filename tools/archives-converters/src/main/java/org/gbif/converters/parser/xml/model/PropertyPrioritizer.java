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

import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.gbif.converters.parser.xml.constants.PrioritizedPropertyNameEnum;
import org.gbif.converters.parser.xml.parsing.xml.PrioritizedProperty;

/**
 * In schemas that have multiple representations of the same field (eg decimal latitude vs text
 * latitude) this class gives a framework for setting the order of preference of fields for
 * resolving cases where more than one of them is populated.
 */
@Slf4j
public abstract class PropertyPrioritizer {

  protected final Map<PrioritizedPropertyNameEnum, Set<PrioritizedProperty>> prioritizedProps =
      new EnumMap<>(PrioritizedPropertyNameEnum.class);

  public abstract void resolvePriorities();

  public void addPrioritizedProperty(PrioritizedProperty prop) {
    if (prop.getName() != null) {
      Set<PrioritizedProperty> nameProps = prioritizedProps.get(prop.getName());
      if (nameProps == null) {
        nameProps = new HashSet<>();
      }
      nameProps.add(prop);
      prioritizedProps.put(prop.getName(), nameProps);
    } else {
      log.warn("Attempting add of null PrioritizedProperty");
    }
  }

  /** Highest priority is 1. */
  protected static String findHighestPriority(Set<PrioritizedProperty> props) {
    return props.stream()
        .min(
            Comparator.comparing(PrioritizedProperty::getPriority)
                .thenComparing(PrioritizedProperty::getProperty))
        .map(PrioritizedProperty::getProperty)
        .orElse(null);
  }
}
