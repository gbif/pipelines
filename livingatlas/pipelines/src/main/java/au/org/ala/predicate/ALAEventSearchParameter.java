package au.org.ala.predicate;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.gbif.api.model.common.search.SearchParameter;

@JsonDeserialize(as = ALAEventSearchParameter.class)
public enum ALAEventSearchParameter implements SearchParameter {
  DATASET_KEY(String.class),
  TAXON_KEY(String.class),
  LOCATION_ID(String.class),
  COUNTRY_CODE(String.class),
  YEAR(Integer.class),
  MONTH(Integer.class),
  SAMPLING_PROTOCOL(String.class),
  STATE_PROVINCE(String.class),
  EVENT_TYPE(String.class),
  EVENT_TYPE_HIERARCHY(String.class),

  // probably a nicer way to do this, but for now add the darwin core ID version
  datasetKey(String.class),
  taxonKey(String.class),
  locationID(String.class),
  countryCode(String.class),
  year(Integer.class),
  month(Integer.class),
  samplingProtocol(String.class),
  stateProvince(String.class),
  eventHierarchy(String.class),
  eventTypeHierarchy(String.class),
  eventType(String.class),
  measurementOfFactTypes(String.class);

  private final Class<?> type;

  ALAEventSearchParameter(Class type) {
    this.type = type;
  }

  public Class<?> type() {
    return this.type;
  }
}
