package org.gbif.pipelines.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class EventCoordinate implements java.io.Serializable {
  String eventId;
  Double latitude;
  Double longitude;
}
