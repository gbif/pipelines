package au.org.ala.pipelines.beam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.pipelines.options.ClusteringPipelineOptions;
import au.org.ala.utils.ValidationUtils;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.io.AvroReader;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.gbif.pipelines.io.avro.Relationships;
import org.junit.Test;

public class ClusteringPipelineIT {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  /** Tests for SOLR index creation. */
  @Test
  public void testClusteringPipeline() throws Exception {

    // Create index records for dataset dr1
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/clustering"));

    // create index records for clustering test
    createIndexRecordAvro("dr1");
    createIndexRecordAvro("dr2");

    // run pipeline
    ClusteringPipelineOptions options =
        PipelinesOptionsFactory.create(
            ClusteringPipelineOptions.class,
            new String[] {
              "--runner=SparkRunner",
              "--metaFileName=" + ValidationUtils.CLUSTERING_METRICS,
              "--clusteringPath=/tmp/la-pipelines-test/clustering/clustering-output",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/clustering/all-datasets-path",
              "--inputPath=/tmp/la-pipelines-test/clustering",
              "--outputDebugAvro=true"
            });
    ClusteringPipeline.run(options);

    // read relationships
    Map<String, Relationships> records =
        AvroReader.readRecords(
            HdfsConfigs.nullConfig(),
            Relationships.class,
            "/tmp/la-pipelines-test/clustering/clustering-output/relationships/*.avro");

    // expecting 10 entries as the dataset is just duplicated
    assertEquals(10, records.size());

    // test the existence of a relationship and check which is the representative record
    Relationships r = records.get("not-an-uuid-1");
    assertNotNull(r);
    assertEquals(1, r.getRelationships().size());
    assertEquals("not-an-uuid-1", r.getRelationships().get(0).getRepId());
    assertEquals("not-an-uuid-1-duplicate", r.getRelationships().get(0).getDupId());
    assertEquals("dr1", r.getRelationships().get(0).getRepDataset());
    assertEquals("dr2", r.getRelationships().get(0).getDupDataset());
  }

  private void createIndexRecordAvro(String dataResourceUid) throws IOException {

    String absolutePath = new File("src/test/resources").getAbsolutePath();

    // Step 1: load a dataset and verify all records have a UUID associated
    Reader reader =
        Files.newBufferedReader(
            Paths.get(absolutePath + "/clustering/" + dataResourceUid + "/occurrence.csv"),
            StandardCharsets.UTF_8);

    CSVReader csvReader = new CSVReader(reader);

    DatumWriter<IndexRecord> datumWriter = new GenericDatumWriter<>(IndexRecord.getClassSchema());

    FileUtils.forceMkdir(
        new File(
            "/tmp/la-pipelines-test/clustering/all-datasets-path/index-record/"
                + dataResourceUid
                + "/"));
    OutputStream output =
        new FileOutputStream(
            "/tmp/la-pipelines-test/clustering/all-datasets-path/index-record/"
                + dataResourceUid
                + "/index-record.avro");
    DataFileWriter<IndexRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.setCodec(BASE_CODEC);
    dataFileWriter.create(IndexRecord.getClassSchema(), output);

    String[] headers = csvReader.readNext();
    String[] line = null;
    while ((line = csvReader.readNext()) != null) {

      // occurrenceID	recordedBy	scientificName	taxonConceptID	rank	kingdom
      // phylum	class	order	family	genus		decimalLatitude	decimalLongitude
      // countryCode	stateProvince	year	month	day
      Map<String, String> strings = new HashMap<>();
      Map<String, Double> doubles = new HashMap<>();
      Map<String, Integer> ints = new HashMap<>();
      strings.put("dataResourceUid", dataResourceUid);
      strings.put("occurrenceID", line[0]);
      strings.put("recordedBy", line[1]);
      strings.put("scientificName", line[2]);
      strings.put("taxonConceptID", line[3]);
      strings.put("speciesID", line[3]);
      strings.put("rank", line[4]);
      strings.put("kingdom", line[5]);
      strings.put("phylum", line[6]);
      strings.put("class", line[7]);
      strings.put("order", line[8]);
      strings.put("family", line[9]);
      strings.put("genus", line[10]);
      strings.put("species", line[11]);
      doubles.put("decimalLatitude", Double.valueOf(line[12]));
      doubles.put("decimalLongitude", Double.valueOf(line[13]));
      strings.put("countryCode", line[14]);
      strings.put("stateProvince", line[15]);
      ints.put("year", Integer.valueOf(line[16]));
      ints.put("month", Integer.valueOf(line[17]));
      ints.put("day", Integer.valueOf(line[18]));
      IndexRecord ir =
          IndexRecord.newBuilder()
              .setId(line[0])
              .setTaxonID(line[3])
              .setDoubles(doubles)
              .setStrings(strings)
              .setInts(ints)
              .build();

      dataFileWriter.append(ir);
    }
    dataFileWriter.flush();
    dataFileWriter.close();
  }
}
