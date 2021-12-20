package au.org.ala.pipelines.transforms;

import au.org.ala.distribution.DistributionLayer;
import au.org.ala.distribution.DistributionServiceImpl;
import au.org.ala.distribution.ExpertDistributionException;
import au.org.ala.pipelines.common.ALARecordTypes;
import au.org.ala.pipelines.interpreters.DistributionOutlierInterpreter;
import java.util.*;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.transforms.Transform;

@Slf4j
public class DistributionOutlierTransform
    extends Transform<IndexRecord, DistributionOutlierRecord> {

  private String spatialUrl;

  public DistributionOutlierTransform(String spatialUrl) {
    super(
        DistributionOutlierRecord.class,
        ALARecordTypes.ALA_DISTRIBUTION,
        DistributionOutlierTransform.class.getName(),
        "alaDistributionCount");
    this.spatialUrl = spatialUrl;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {}

  public DistributionOutlierTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<DistributionOutlierRecord> convert(IndexRecord source) {
    DistributionOutlierRecord dr =
        DistributionOutlierRecord.newBuilder().setId(source.getId()).setSpeciesID("").build();
    return Interpretation.from(source)
        .to(dr)
        .via(DistributionOutlierInterpreter::interpretOccurrenceID)
        .via(DistributionOutlierInterpreter::interpretLocation)
        .via(DistributionOutlierInterpreter::interpretSpeciesId)
        .getOfNullable();
  }

  public MapElements<IndexRecord, KV<String, IndexRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, IndexRecord>>() {})
        .via((IndexRecord ir) -> KV.of(ir.getTaxonID(), ir));
  }

  public MapElements<KV<String, Iterable<IndexRecord>>, Iterable<DistributionOutlierRecord>>
      calculateOutlier() {
    return MapElements.via(
        (new SimpleFunction<
            KV<String, Iterable<IndexRecord>>, Iterable<DistributionOutlierRecord>>() {
          @Override
          public Iterable<DistributionOutlierRecord> apply(
              KV<String, Iterable<IndexRecord>> input) {
            String lsid = input.getKey();
            Iterable<IndexRecord> records = input.getValue();
            Iterator<IndexRecord> iter = records.iterator();
            List<DistributionOutlierRecord> outputs = new ArrayList();

            try {
              DistributionServiceImpl distributionService =
                  DistributionServiceImpl.init(spatialUrl);
              List<DistributionLayer> edl = distributionService.findLayersByLsid(lsid);
              boolean hasEDL = edl.size() > 0 ? true : false;
              double distanceToEDL = hasEDL ? 0 : -1; // 0 -inside, -1: no EDL
              // Available EDLD of this species

              if (hasEDL) {
                Map points = new HashMap();
                while (iter.hasNext()) {
                  IndexRecord record = iter.next();
                  DistributionOutlierRecord dr = convertToDistribution(record, distanceToEDL);
                  if (dr != null) {
                    outputs.add(dr);
                    Map point = new HashMap();
                    point.put("decimalLatitude", dr.getDecimalLatitude());
                    point.put("decimalLongitude", dr.getDecimalLongitude());
                    points.put(dr.getId(), point);
                  }
                }

                log.debug(
                    String.format("Calculating %d records of the species %s", points.size(), lsid));
                Map<String, Double> results = distributionService.outliers(lsid, points);
                Iterator<Map.Entry<String, Double>> iterator = results.entrySet().iterator();
                while (iterator.hasNext()) {
                  Map.Entry<String, Double> entry = iterator.next();
                  StreamSupport.stream(outputs.spliterator(), false)
                      .filter(it -> it.getId().equalsIgnoreCase(entry.getKey()))
                      .forEach(it -> it.setDistanceOutOfEDL(entry.getValue()));
                }
              }
            } catch (ExpertDistributionException e) {
              log.error("Error in processing the species: " + lsid + " . Ignored");
              log.error(e.getMessage());
              // throw new RuntimeException(
              //  "Expert distribution service throws a runtime error, please check
              // logs");
            } catch (Exception e) {
              log.error("Error in processing the species: " + lsid + " . Ignored");
              log.error(e.getMessage());
              // throw new RuntimeException("Runtime error: " + e.getMessage());
            }

            return outputs;
          }
        }));
  }

  /**
   * Stringify {@Link DistributionOutlierRecord}
   *
   * @return
   */
  public MapElements<DistributionOutlierRecord, String> flatToString() {
    return MapElements.into(new TypeDescriptor<String>() {})
        .via((DistributionOutlierRecord dr) -> this.convertRecordToString(dr));
  }

  /**
   * ID / Taxon ID / LatLng MUST be valid
   *
   * <p>distanceOutOfEDL 0: inside edl, -1: no edl
   *
   * @param record
   * @return
   */
  private DistributionOutlierRecord convertToDistribution(
      IndexRecord record, double distanceToEDL) {
    try {
      DistributionOutlierRecord newRecord =
          DistributionOutlierRecord.newBuilder()
              .setId(record.getId())
              .setSpeciesID(record.getTaxonID())
              .setDistanceOutOfEDL(distanceToEDL)
              .build();

      String latlng = record.getLatLng();
      String[] coordinates = latlng.split(",");
      newRecord.setDecimalLatitude(Double.parseDouble(coordinates[0]));
      newRecord.setDecimalLongitude(Double.parseDouble(coordinates[1]));

      return newRecord;
    } catch (Exception ex) {
      log.debug(record.getId() + " has incorrect lat/lng or taxon. ignored..");
    }
    return null;
  }

  private String convertRecordToString(DistributionOutlierRecord record) {
    return String.format(
        "occurrenceId:%s, lat:%f, lng:%f, speciesId:%s, distanceToEDL:%f",
        record.getId(),
        record.getDecimalLatitude(),
        record.getDecimalLongitude(),
        record.getSpeciesID(),
        record.getDistanceOutOfEDL());
  }
}
