package au.org.ala.specieslists;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.pipelines.options.SpeciesLevelPipelineOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.pipelines.vocabulary.StateProvince;
import au.org.ala.pipelines.vocabulary.Vocab;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.WsUtils;
import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import okhttp3.ResponseBody;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.CountryParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.SpeciesListRecord;
import org.gbif.rest.client.retrofit.SyncCall;
import org.gbif.utils.file.csv.CSVReader;
import org.slf4j.MDC;
import retrofit2.Call;

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

  public static void run(SpeciesLevelPipelineOptions options) throws Exception {

    // read config
    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(
                options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getProperties())
            .get();

    final Vocab stateProvinceVocab = StateProvince.getInstance(config.getLocationInfoConfig().getStateProvinceNamesFile());

    // get filesystem
    FileSystem fs =
        FsUtils.getFileSystem(
            options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getInputPath());

    SpeciesListService service =
        WsUtils.createClient(config.getSpeciesListService(), SpeciesListService.class);

    // get authoritative list of lists
    // https://lists.ala.org.au/ws/speciesList?isAuthoritative=eq:true&max=1000

    Call<ListSearchResponse> call = service.getAuthoritativeLists();
    ListSearchResponse listsResponse = SyncCall.syncCall(call);

    // download individual lists
    // https://lists.ala.org.au/speciesListItem/downloadList/dr650?fetch=%7BkvpValues%3Dselect%7
    log.info("Number of species lists {}", listsResponse.getLists().size());

    String outputPath = options.getSpeciesAggregatesPath() + "/species-lists/species-lists.avro";
    log.info("Writing output to {}", outputPath);

    // create the output file
    OutputStream output = fs.create(new Path(outputPath));
    DatumWriter<SpeciesListRecord> datumWriter =
        new GenericDatumWriter<SpeciesListRecord>(SpeciesListRecord.getClassSchema());
    DataFileWriter dataFileWriter = new DataFileWriter<SpeciesListRecord>(datumWriter);
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
      CSVReader csvReader = new CSVReader(responseBody.byteStream(), "UTF-8", ",", '"', 1);

      List<String> columnHeaders = Arrays.asList(csvReader.getHeader());
      int guidIdx = columnHeaders.indexOf("guid");
      int statusIdx = columnHeaders.indexOf("status");
      int sourceStatusIdx = columnHeaders.indexOf("sourceStatus");

      String region = null;

      if (list.getRegion() != null) {
        // match states
        Optional<String> match = stateProvinceVocab.matchTerm(list.getRegion());

        if (match.isPresent()){
          region = match.get();
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
        String[] currentLine = csvReader.next();

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
          }
          currentLine = csvReader.next();
        }
      } else {
        log.warn(
            "List {} - {} does not supply a GUID column - hence this list will not be used",
            list.getDataResourceUid(),
            list.getListName());
      }
      csvReader.close();
    }
    dataFileWriter.close();
    log.info("Finished. Output written to {}", outputPath);
  }
}
