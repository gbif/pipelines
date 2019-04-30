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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.gbif.converters.parser.xml.constants.PrioritizedPropertyNameEnum;
import org.gbif.converters.parser.xml.parsing.xml.PrioritizedProperty;

import lombok.extern.slf4j.Slf4j;

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

    TreeMap<Integer, List<PrioritizedProperty>> map = new TreeMap<>();
    props.forEach(p -> {
      List<PrioritizedProperty> list = map.get(p.getPriority());
      if (list == null) {
        List<PrioritizedProperty> created = new ArrayList<>(props.size());
        created.add(p);
        map.put(p.getPriority(), created);
      } else {
        list.add(p);
        map.put(p.getPriority(), list);
      }
    });

    return map.firstEntry().getValue().stream()
        .map(PrioritizedProperty::getProperty)
        .sorted()
        .findFirst()
        .orElse(null);
  }
}
