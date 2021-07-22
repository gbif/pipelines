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

import java.util.Locale;

/** Represents email type, provides with email subject and raw template. */
public interface EmailType {

  /** Returns email key to search for subject. */
  String getKey();

  /** Returns email template name. */
  String getTemplate();

  /** Returns email subject. */
  String getSubject(Locale locale, String... subjectParams);
}
