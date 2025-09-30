package au.org.ala.kvs;

import java.io.Serializable;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;

/** DTO for a shape file. This is mapped to configuration in pipelines.yaml. */
@AllArgsConstructor
@Data
public class ShapeFile implements Serializable {
  /** Path to the shape file */
  String path;

  /** The name field to use from the shape file. */
  String field;

  /** URL to source of the shapefile */
  String source;

  /** Intersect buffer 0.1 = 11km, 0.135 = 15km, 0.18 = 20km */
  Double intersectBuffer = 0.18;

  /** Intersect mapping to allow intersected values to mapped to different values e.g. CX -> AU * */
  Map<String, String> intersectMapping;
}
