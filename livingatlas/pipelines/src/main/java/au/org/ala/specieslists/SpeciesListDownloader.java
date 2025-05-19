package au.org.ala.specieslists;

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.kvs.client.retrofit.SyncCall;
import au.org.ala.pipelines.options.SpeciesLevelPipelineOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.pipelines.vocabulary.StateProvinceParser;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.WsUtils;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ResponseBody;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.CountryParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.SpeciesListRecord;
import org.slf4j.MDC;
import retrofit2.Call;

/**
 * This class downloads details of species lists marked as "Authoritative" from a species list tool
 * instance (see https://github.com/AtlasOfLivingAustralia/specieslist-webapp) and generates an AVRO
 * file dump with the @{@link SpeciesListRecord}.
 *
 * <p>Includes support for a flag to avoid downloading and regenerating the AVRO export if the last
 * modified date is within a time frame (default 1 day).
 *
 * @see {@link SpeciesListRecord}
 */
@Slf4j
public class SpeciesListDownloader {

  public static void main(String[] args) throws Exception {
    MDC.put("step", "SPECIES_LIST_DOWNLOAD");
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "speciesLists");
    SpeciesLevelPipelineOptions options =
        PipelinesOptionsFactory.create(SpeciesLevelPipelineOptions.class, combinedArgs);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  public static void run(SpeciesLevelPipelineOptions options) throws IOException {

    // read config
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());
    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(hdfsConfigs, options.getProperties()).get();

    final StateProvinceParser stateProvinceParser =
        StateProvinceParser.getInstance(config.getLocationInfoConfig().getStateProvinceNamesFile());

    // get filesystem
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, options.getInputPath());

    String outputPath = options.getSpeciesAggregatesPath() + options.getSpeciesListCachePath();

    // check timestamp
    log.info("Checking output path {}", outputPath);
    Path outputPathFs = new Path(outputPath);
    if (fs.exists(outputPathFs)) {
      FileStatus fileStatus = fs.getFileStatus(new Path(outputPath));
      long modificationTime = fileStatus.getModificationTime();
      long maxAgeInMillis = System.currentTimeMillis();
      long ageInMinutes = (maxAgeInMillis - modificationTime) / 60000;
      log.info(
          "Age: {} hrs {} mins (or {} minutes)",
          ageInMinutes / 60,
          ageInMinutes % 60,
          ageInMinutes);
      if (ageInMinutes < options.getMaxDownloadAgeInMinutes()) {
        log.info(
            "The age of the download is less than the max age {}. Will skip download.",
            options.getMaxDownloadAgeInMinutes());
        return;
      }
    }

    // Load species list service
    SpeciesListService service =
        WsUtils.createClient(config.getSpeciesListService(), SpeciesListService.class);

    // get authoritative list of lists
    Call<ListSearchResponse> call = service.getAuthoritativeLists();
    ListSearchResponse listsResponse = SyncCall.syncCall(call);

    // download individual lists
    log.info("Number of species lists {}", listsResponse.getLists().size());

    log.info("Writing output to {}", outputPath);

    // create the output file

    try (OutputStream output = fs.create(new Path(outputPath));
        DataFileWriter<SpeciesListRecord> dataFileWriter =
            new DataFileWriter<>(new GenericDatumWriter<>(SpeciesListRecord.getClassSchema()))) {
      dataFileWriter.create(SpeciesListRecord.getClassSchema(), output);

      int counter = 0;
      for (SpeciesList list : listsResponse.getLists()) {

        counter++;
        log.info(
            "Downloading list {} of {} - {} -  {}",
            counter,
            listsResponse.getLists().size(),
            list.getDataResourceUid(),
            list.getListName());
        ResponseBody responseBody =
            SyncCall.syncCall(service.downloadList(list.getDataResourceUid()));

        // File source, String encoding, String delimiter, Character quotes, Integer headerRows
        try (CSVReader csvReader =
            new CSVReader(new InputStreamReader(responseBody.byteStream(), "UTF-8"), ',', '"', 0)) {

          List<String> columnHeaders = Arrays.asList(csvReader.readNext());
          int guidIdx = columnHeaders.indexOf("guid");
          int statusIdx = columnHeaders.indexOf("status");
          int sourceStatusIdx = columnHeaders.indexOf("sourceStatus");

          String region = null;

          if (list.getRegion() != null) {
            // match states
            ParseResult<String> match = stateProvinceParser.parse(list.getRegion());

            if (match.isSuccessful()) {
              region = match.getPayload();
            } else {
              // match country
              ParseResult<Country> pr = CountryParser.getInstance().parse(list.getRegion());
              if (pr.isSuccessful()) {
                region = pr.getPayload().name();
              } else {
                region = list.getRegion();
              }
            }
          }

          if (guidIdx > 0) {
            String[] currentLine = csvReader.readNext();
            int taxaRead = 0;

            // build up the map
            while (currentLine != null && currentLine.length == columnHeaders.size()) {
              String taxonID = currentLine[guidIdx];

              if (taxonID.length() > 0) {

                String status = statusIdx > 0 ? currentLine[statusIdx] : null;
                String sourceStatus = sourceStatusIdx > 0 ? currentLine[sourceStatusIdx] : null;

                SpeciesListRecord speciesListRecord =
                    SpeciesListRecord.newBuilder()
                        .setTaxonID(taxonID)
                        .setSpeciesListID(list.getDataResourceUid())
                        .setStatus(status)
                        .setRegion(region)
                        .setIsInvasive(list.isInvasive())
                        .setIsThreatened(list.isThreatened())
                        .setSourceStatus(sourceStatus)
                        .build();
                dataFileWriter.append(speciesListRecord);
                taxaRead++;
              }
              currentLine = csvReader.readNext();
            }
            log.info("List " + list.getDataResourceUid() + " has " + taxaRead + " taxa");
          } else {
            log.warn(
                "List {} - {} does not supply a GUID column - hence this list will not be used",
                list.getDataResourceUid(),
                list.getListName());
          }
        }
      }
    }
    log.info("Finished. Output written to {}", outputPath);
  }
}
