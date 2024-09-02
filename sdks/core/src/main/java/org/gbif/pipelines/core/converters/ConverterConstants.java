package org.gbif.pipelines.core.converters;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
class ConverterConstants {

  public static final String DELIMITER = " / ";
  public static final String SURVEY = "Survey";
  public static final String EVENT = "event";
  public static final String OCCURRENCE = "occurrence";
  public static final String EVENT_NAME = "http://rs.gbif.org/terms/1.0/eventName";
  public static final String OCCURRENCE_EXT = "http://rs.tdwg.org/dwc/terms/Occurrence";
}
