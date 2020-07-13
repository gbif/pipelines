package au.org.ala.pipelines.beam;

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.io.avro.LocationFeatureRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.transforms.specific.LocationFeatureTransform;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Pipeline that adds a sampling AVRO extension to the stored interpretation.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALASamplingToAvroPipeline {

    public static void main(String[] args) throws FileNotFoundException {
        String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "sample-avro");
        InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(combinedArgs);
        PipelinesOptionsFactory.registerHdfs(options);
        run(options);
    }

    public static void run(InterpretationPipelineOptions options) {

        log.info("Creating a pipeline from options");
        Pipeline p = Pipeline.create(options);

        // Path equivalent to /data/pipelines-data/dr893/1/sampling
        String samplingPath = ALAFsUtils.buildPathSamplingDownloadsUsingTargetPath(options);
        log.info("Reading sampling from " + samplingPath);

        // Path equivalent to /data/pipelines-data/dr893/1/sampling/australia_spatial
        String outputPath = ALAFsUtils.buildPathSamplingOutputUsingTargetPath(options);
        log.info("Outputting results to " + outputPath);

        FileSystem fs = FsUtils.getFileSystem(options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getInputPath());

        // Read column headers
        final String[] columnHeaders = getColumnHeaders(fs, samplingPath);

        // Read from download sampling CSV files
        PCollection<String> lines = p.apply(TextIO.read().from(samplingPath + "/*.csv"));

        // Location transform output
        String alaRecordDirectoryPath = options.getTargetPath() + "/" + options.getDatasetId().trim()
                + "/1/interpreted/" + PipelinesVariables.Pipeline.Interpretation.RecordType.LOCATION.name().toLowerCase();

        // Filter records without lat/long, create map LatLng -> ExtendedRecord.getId()
        PCollection<KV<String, String>> latLngID = p.apply(AvroIO.read(LocationRecord.class).from(alaRecordDirectoryPath + "/*.avro"))
            .apply(ParDo.of(new DoFn<LocationRecord, LocationRecord>() {
                @ProcessElement
                public void processElement(@Element LocationRecord locationRecord, OutputReceiver<LocationRecord> out) {
                    if (locationRecord.getDecimalLatitude() != null && locationRecord.getDecimalLongitude() != null){
                        out.output(locationRecord);
                    }
                }
            }))
            .apply(ParDo.of(new LocationRecordFcn()));

        // Read in sampling from downloads CSV files, and key it on LatLng -> sampling
        PCollection<KV<String, Map<String,String>>> alaSampling = lines.apply(ParDo.of(new DoFn<String, KV<String, Map<String, String>>>() {
            @ProcessElement
            public void processElement(@Element String sampling, OutputReceiver<KV<String, Map<String, String>>> out) {
                Map<String, String> parsedSampling = new HashMap<String, String>();
                try {
                    //skip the header
                    if (!sampling.startsWith("latitude")) {
                        //need headers as a side input
                        CSVReader csvReader = new CSVReader(new StringReader(sampling));
                        String[] line = csvReader.readNext();
                        //first two columns are latitude,longitude
                        for (int i = 2; i < columnHeaders.length; i++) {
                            if (StringUtils.trimToNull(line[i]) != null) {
                                parsedSampling.put(columnHeaders[i], line[i]);
                            }
                        }

                        String latLng = line[0] + "," + line[1];
                        KV<String, Map<String, String>> aur = KV.of(latLng, parsedSampling);

                        out.output(aur);
                        csvReader.close();
                    }
                } catch (Exception e){
                    throw new RuntimeException(e.getMessage());
                }
            }
        }));

        // Create tuple tags
        final TupleTag<String> latLngIDTag = new TupleTag<>();
        final TupleTag<Map<String, String>> alaSamplingTag = new TupleTag<>();

        // Join collections by LatLng
        PCollection<KV<String, CoGbkResult>> results =
                KeyedPCollectionTuple.of(latLngIDTag, latLngID)
                        .and(alaSamplingTag, alaSampling)
                        .apply(CoGroupByKey.create());

        // Create AustraliaSpatialRecord
        PCollection<LocationFeatureRecord> locationFeatureRecordPCollection =
                results.apply(
                        ParDo.of(
                                new DoFn<KV<String, CoGbkResult>, LocationFeatureRecord>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        KV<String, CoGbkResult> e = c.element();
                                        Iterator<String> idIter =  e.getValue().getAll(latLngIDTag).iterator();
                                        Map<String, String> sampling = e.getValue().getOnly(alaSamplingTag);

                                        while (idIter.hasNext()){
                                            String id = idIter.next();
                                            LocationFeatureRecord aur = LocationFeatureRecord.newBuilder().setItems(sampling).setId(id).build();
                                            c.output(aur);
                                        }
                                    }
                                }));

        // Write out AustraliaSpatialRecord to disk
        LocationFeatureTransform locationFeatureTransform = LocationFeatureTransform.builder().create();
        locationFeatureRecordPCollection.apply("Write sampling to avro", locationFeatureTransform.write(outputPath));

        log.info("Running the pipeline");
        PipelineResult result = p.run();
        result.waitUntilFinish();

        log.info("Pipeline has been finished");
    }


    @NotNull
    private static String[] getColumnHeaders(FileSystem fs, String samplingPath)  {

        try {
            //obtain column header
            if (ALAFsUtils.exists(fs, samplingPath)) {

                Collection<String> samplingFiles = ALAFsUtils.listPaths(fs, samplingPath);

                if (!samplingFiles.isEmpty()) {

                    //read the first line of the first sampling file
                    String samplingFilePath = samplingFiles.iterator().next();
                    String columnHeaderString = new BufferedReader(new InputStreamReader(ALAFsUtils.openInputStream(fs, samplingFilePath))).readLine();
                    return columnHeaderString.split(",");

                } else {
                    throw new RuntimeException("Sampling directory found, but is empty. Has sampling from spatial-service been ran ? Missing dir: " + samplingPath);
                }
            } else {
                throw new RuntimeException("Sampling directory cant be found. Has sampling from spatial-service been ran ? Missing dir: " + samplingPath);
            }
        } catch (IOException e){
            throw new RuntimeException("Problem reading sampling from: " + samplingPath + " - " + e.getMessage(), e);
        }
    }

    /**
     * Function to create ALAUUIDRecords.
     */
    static class LocationRecordFcn extends DoFn<LocationRecord, KV<String,String>> {

        @ProcessElement
        public void processElement(@Element LocationRecord locationRecord, OutputReceiver<KV<String,String>> out) {
            try {
                KV<String, String> kv = KV.of(locationRecord.getDecimalLatitude() + "," + locationRecord.getDecimalLongitude(), locationRecord.getId());
                out.output(kv);
            } catch (Exception e){
                throw new RuntimeException(e.getMessage());
            }
        }
    }
}
