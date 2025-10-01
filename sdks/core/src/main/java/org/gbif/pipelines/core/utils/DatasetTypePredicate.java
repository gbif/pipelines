package org.gbif.pipelines.core.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.EndpointType;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DatasetTypePredicate {

  public static boolean isEndpointDwca(EndpointType endpointType) {
    return EndpointType.DWC_ARCHIVE == endpointType
        || EndpointType.EML == endpointType
        || EndpointType.BIOM_1_0 == endpointType
        || EndpointType.BIOM_2_1 == endpointType
        || EndpointType.CAMTRAP_DP == endpointType;
  }

  public static boolean isEndpointXml(EndpointType endpointType) {
    return EndpointType.BIOCASE == endpointType
        || EndpointType.BIOCASE_XML_ARCHIVE == endpointType
        || EndpointType.DIGIR == endpointType
        || EndpointType.DIGIR_MANIS == endpointType;
  }
}
