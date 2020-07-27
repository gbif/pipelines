package org.gbif.pipelines.parsers.parsers.location.parser;

import lombok.Getter;
import lombok.ToString;
import org.gbif.rest.client.geocode.Location;

/** Models a set of GADM features at different levels. */
@Getter
@ToString
public class GadmFeatures {

  private String level0Gid;
  private String level1Gid;
  private String level2Gid;
  private String level3Gid;
  private String level0Name;
  private String level1Name;
  private String level2Name;
  private String level3Name;

  public void accept(Location l) {
    if (l.getType() != null) {
      switch (l.getType()) {
        case "GADM0":
          if (level0Gid == null) {
            level0Gid = l.getId();
            level0Name = l.getName();
          }
          return;
        case "GADM1":
          if (level1Gid == null) {
            level1Gid = l.getId();
            level1Name = l.getName();
          }
          return;
        case "GADM2":
          if (level2Gid == null) {
            level2Gid = l.getId();
            level2Name = l.getName();
          }
          return;
        case "GADM3":
          if (level3Gid == null) {
            level3Gid = l.getId();
            level3Name = l.getName();
          }
          return;
        default:
      }
    }
  }
}
