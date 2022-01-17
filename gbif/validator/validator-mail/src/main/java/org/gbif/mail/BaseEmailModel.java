/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
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
package org.gbif.mail;

import java.util.Collections;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/** Very basic email model that holds the main components of an email to send. */
@AllArgsConstructor
@Builder
@Data
public class BaseEmailModel {

  @Builder.Default private final Set<String> emailAddresses = Collections.emptySet();
  private final String subject;
  private final String body;
  @Builder.Default private final Set<String> ccAddresses = Collections.emptySet();
}
