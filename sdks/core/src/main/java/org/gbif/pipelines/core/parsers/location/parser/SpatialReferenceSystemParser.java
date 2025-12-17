package org.gbif.pipelines.core.parsers.location.parser;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.DatumParser;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.datum.DatumAuthorityFactory;
import org.geotools.api.referencing.datum.GeodeticDatum;
import org.geotools.factory.BasicFactories;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.cs.DefaultEllipsoidalCS;
import org.geotools.util.factory.FactoryRegistryException;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SpatialReferenceSystemParser {

  private static final DatumParser PARSER = DatumParser.getInstance();
  private static DatumAuthorityFactory datumFactory;

  static {
    try {
      datumFactory = BasicFactories.getDefault().getDatumAuthorityFactory();
    } catch (FactoryRegistryException e) {
      log.error("Failed to create geotools datum factory", e);
    }
  }

  /**
   * Parses the given datum or SRS code and constructs a full 2D geographic reference system.
   *
   * @return the parsed CRS or null if it can't be interpreted
   */
  public static CoordinateReferenceSystem parseCRS(String datum) {
    CoordinateReferenceSystem crs = null;
    ParseResult<Integer> epsgCode = PARSER.parse(datum);
    if (epsgCode.isSuccessful()) {
      final String code = "EPSG:" + epsgCode.getPayload();

      // first try to create a full fledged CRS from the given code
      try {
        crs = CRS.decode(code);

      } catch (FactoryException e) {
        // that didn't work, maybe it is *just* a datum
        try {
          GeodeticDatum dat = datumFactory.createGeodeticDatum(code);
          crs = new DefaultGeographicCRS(dat, DefaultEllipsoidalCS.GEODETIC_2D);

        } catch (FactoryException e1) {
          // also not a datum, no further ideas, log error
          // swallow anything and return null instead
          log.info("No CRS or DATUM for given datum code >>{}<<: {}", datum, e1.getMessage());
        }
      }
    }
    return crs;
  }
}
