package au.org.ala.pipelines.interpreters;

import static org.gbif.pipelines.core.utils.ModelUtils.*;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.DistributionOutlierRecord;
import org.gbif.pipelines.io.avro.IndexRecord;

/*
 * living atlases.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DistributionOutlierInterpreter {

  public static void interpretOccurrenceID(IndexRecord ir, DistributionOutlierRecord dr) {
    dr.setId(ir.getId());
  }

  public static void interpretLocation(IndexRecord ir, DistributionOutlierRecord dr) {
    String latlng = ir.getLatLng();
    String[] coordinates = latlng.split(",");
    dr.setDecimalLatitude(Double.parseDouble(coordinates[0]));
    dr.setDecimalLongitude(Double.parseDouble(coordinates[1]));
  }

  public static void interpretSpeciesId(IndexRecord ir, DistributionOutlierRecord dr) {
    dr.setSpeciesID(ir.getTaxonID());
  }
}
