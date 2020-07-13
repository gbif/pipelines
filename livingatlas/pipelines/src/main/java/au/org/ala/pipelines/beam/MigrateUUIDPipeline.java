package au.org.ala.pipelines.beam;

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.pipelines.common.ALARecordTypes;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.ingest.options.BasePipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.io.avro.ALAUUIDRecord;

import java.io.StringReader;

import static org.apache.beam.sdk.io.FileIO.Write.defaultNaming;

/**
 * A temporary pipeline used for data migration. This uses a extract from the cassandra occ_uuid and generates
 * the <DATASET_ID>/1/identifiers/ala_uuid files in AVRO and distributes them for each dataset into separate
 * directories.
 *
 * This class should not be part of the codebase in the long term and should be removed eventually.
 *
 * Note: this pipeline can currently only be ran with the DirectRunner due to issues with SparkRunner
 * logged here: https://jira.apache.org/jira/browse/BEAM-10100
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MigrateUUIDPipeline {

    private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();
    public static void main(String[] args) throws Exception {
        BasePipelineOptions options = PipelinesOptionsFactory.create(BasePipelineOptions.class, args);
        run(options);
    }

    public static void run(BasePipelineOptions options) {

        // Initialise pipeline
        Pipeline p = Pipeline.create(options);

        // read data into a KV structure UniqueKey -> UUID
        PCollection<KV<String, ALAUUIDRecord>> records = p.apply(TextIO.read().from(options.getInputPath())).apply(ParDo.of(new StringToDatasetIDAvroRecordFcn()));

        //write out into AVRO in each separate directory
        // Group by dataResourceUID, keying on <dataResourceUID> to enable dynamic file writing destinations
        // Write to /data/pipelines-data/<dataResourceUID>/...ala_uuid using the dynamicDestinations capability of Beam
        records.apply("Write avro file per dataset", FileIO.<String, KV<String, ALAUUIDRecord>>writeDynamic()
                .by(KV::getKey)
                .via(Contextful.fn(KV::getValue), Contextful.fn(x -> AvroIO.sink(ALAUUIDRecord.class).withCodec(BASE_CODEC)))
                .to(options.getTargetPath())
                .withDestinationCoder(StringUtf8Coder.of())
                .withNaming(key -> defaultNaming(key + "/1/identifiers/" + ALARecordTypes.ALA_UUID.toString().toLowerCase() + "/interpret", PipelinesVariables.Pipeline.AVRO_EXTENSION)));

        PipelineResult result = p.run();
        result.waitUntilFinish();
    }

    /**
     * Function to create ALAUUIDRecords.
     */
    static class StringToDatasetIDAvroRecordFcn extends DoFn<String, KV<String, ALAUUIDRecord>> {

        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<KV<String, ALAUUIDRecord>> out) {
            try {
                CSVReader csvReader = new CSVReader(new StringReader(line));
                String[] fields = csvReader.readNext();
                String datasetID = fields[0].substring(0, fields[0].indexOf("|"));
                ALAUUIDRecord record = ALAUUIDRecord.newBuilder().setId(fields[1]).setUniqueKey(fields[0]).setUuid(fields[1]).build();

                KV<String, ALAUUIDRecord> kv = KV.of(datasetID,record);
                out.output(kv);
            } catch (Exception e){
                throw new RuntimeException(e.getMessage() + " - problem ID: " + line);
            }
        }
    }
}