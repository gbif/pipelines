package org.gbif.pipelines.parsers.parsers.location.parser;

import lombok.Getter;
import org.gbif.rest.client.geocode.Location;

/** Models a set of GADM features at different levers. */
@Getter
public class GadmFeatures {

  private String level0;
  private String level1;
  private String level2;
  private String level3;

  public void accept(Location l) {
    if (l.getType() != null) {
      switch (l.getType()) {
        case "GADM0":
          if (level0 == null) {
            level0 = l.getId();
          }
          return;
        case "GADM1":
          if (level1 == null) {
            level1 = l.getId();
          }
          return;
        case "GADM2":
          if (level2 == null) {
            level2 = l.getId();
          }
          return;
        case "GADM3":
          if (level3 == null) {
            level3 = l.getId();
          }
          return;
      }
    }
  }

  @Override
  public String toString() {
    return "GadmFeatures{" +
      "level0='" + level0 + '\'' +
      ", level1='" + level1 + '\'' +
      ", level2='" + level2 + '\'' +
      ", level3='" + level3 + '\'' +
      '}';
  }
}
