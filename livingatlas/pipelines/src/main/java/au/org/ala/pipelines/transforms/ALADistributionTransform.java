package au.org.ala.pipelines.transforms;

import au.org.ala.distribution.DistributionServiceImpl;
import au.org.ala.distribution.DistributionLayer;
import au.org.ala.distribution.ExpertDistributionException;
import au.org.ala.pipelines.common.ALARecordTypes;
import au.org.ala.pipelines.interpreters.ALADistributionInterpreter;
import java.util.*;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.transforms.Transform;

public class ALADistributionTransform extends Transform<IndexRecord, ALADistributionRecord> {

  private String spatialUrl;

  public ALADistributionTransform(String spatialUrl) {
    super(
        ALADistributionRecord.class,
        ALARecordTypes.ALA_DISTRIBUTION,
        ALADistributionTransform.class.getName(),
        "alaDistributionCount");
    this.spatialUrl = spatialUrl;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {}

  public ALADistributionTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<ALADistributionRecord> convert(IndexRecord source) {
    ALADistributionRecord dr =
        ALADistributionRecord.newBuilder().setId(source.getId()).setSpeciesID("").build();
    return Interpretation.from(source)
        .to(dr)
        .via(ALADistributionInterpreter::interpretOccurrenceID)
        .via(ALADistributionInterpreter::interpretLocation)
        .via(ALADistributionInterpreter::interpretSpeciesId)
        .getOfNullable();
  }


  /**
   * Maps {@link ALADistributionRecord} to key value, where key is {@link
   * ALADistributionRecord#getSpeciesID()}
   */
  public MapElements<ALADistributionRecord, KV<String, ALADistributionRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, ALADistributionRecord>>() {})
        .via((ALADistributionRecord dr) -> KV.of(dr.getSpeciesID(), dr));
  }

  public MapElements<KV<String, Iterable<ALADistributionRecord>>, Iterable<ALADistributionRecord>>
      calculateOutlier() {
    return MapElements.via(
        (new SimpleFunction<
            KV<String, Iterable<ALADistributionRecord>>, Iterable<ALADistributionRecord>>() {
          @Override
          public Iterable<ALADistributionRecord> apply(
              KV<String, Iterable<ALADistributionRecord>> input) {
            String lsid = input.getKey();
            Iterable<ALADistributionRecord> records = input.getValue();
            Iterator<ALADistributionRecord> iter = records.iterator();

            try {
              DistributionServiceImpl distributionService = DistributionServiceImpl.init(spatialUrl);
              List<DistributionLayer> edl = distributionService.findLayersByLsid(lsid);
              // Available EDLD of this species
              if (edl.size() > 0) {
                // Duplicate records because Transform does not allow change input
                List<ALADistributionRecord> outputs = new ArrayList();
                Map points = new HashMap();
                while (iter.hasNext()) {
                  ALADistributionRecord record = iter.next();
                  outputs.add(copy(record));

                  Map point = new HashMap();
                  point.put("decimalLatitude", record.getDecimalLatitude());
                  point.put("decimalLongitude", record.getDecimalLongitude());
                  points.put(record.getOccurrenceID(), point);
                }

                Map<String, Double> results = distributionService.outliers(lsid, points);
                Iterator<Map.Entry<String, Double>> iterator = results.entrySet().iterator();
                while (iterator.hasNext()) {
                  Map.Entry<String, Double> entry = iterator.next();
                  StreamSupport.stream(outputs.spliterator(), false)
                      .filter(it -> it.getOccurrenceID().equalsIgnoreCase(entry.getKey()))
                      .forEach(it -> it.setDistanceOutOfEDL(entry.getValue()));
                }
                return outputs;
              }

            } catch (ExpertDistributionException e) {
              throw new RuntimeException(
                  "Expert distribution service returns error: " + e.getMessage());
            } catch (Exception e) {
              throw new RuntimeException("Runtime error: " + e.getMessage());
            }

            return records;
          }
        }));
  }

  /**
   * Stringify {@Link ALADistributionRecord}
   *
   * @return
   */
  public MapElements<ALADistributionRecord, String> flatToString() {
    return MapElements.into(new TypeDescriptor<String>() {})
        .via((ALADistributionRecord dr) -> this.convertRecordToString(dr));
  }

  /**
   * Only can be used when EDL exists - which means the record can only in/out EDL Force to reset
   * distanceOutOfEDL 0 because Spatial EDL service only return distance of outlier records
   *
   * @param record
   * @return
   */
  private ALADistributionRecord copy(ALADistributionRecord record) {
    ALADistributionRecord newRecord = new ALADistributionRecord();
    newRecord.setId(record.getId());
    newRecord.setOccurrenceID(record.getOccurrenceID());
    newRecord.setDistanceOutOfEDL(0.0d);
    newRecord.setDecimalLongitude(record.getDecimalLongitude());
    newRecord.setDecimalLatitude(record.getDecimalLatitude());
    newRecord.setSpeciesID(record.getSpeciesID());
    IssueRecord ir =
        IssueRecord.newBuilder().setIssueList(record.getIssues().getIssueList()).build();
    newRecord.setIssues(ir);
    return newRecord;
  }

  private String convertRecordToString(ALADistributionRecord record) {
    return String.format(
        "occurrenceId:%s, lat:%f, lng:%f, speciesId:%s, distanceToEDL:%f",
        record.getOccurrenceID(), record.getDecimalLatitude(), record.getDecimalLongitude(), record.getSpeciesID(), record.getDistanceOutOfEDL());
  }
}
