package au.org.ala.pipelines.transforms;

import static org.apache.avro.Schema.Type.UNION;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;

import au.org.ala.pipelines.interpreters.SensitiveDataInterpreter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.apache.solr.common.SolrInputDocument;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.io.avro.*;
import org.jetbrains.annotations.NotNull;

/**
 * A SOLR transform that aims to provide a index that is backwards compatible with ALA's
 * biocache-service.
 */
@Slf4j
public class ALASolrDocumentTransform implements Serializable {
  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  // TODO review content
  private static final Set<String> SKIP_KEYS =
      new HashSet<>(
          Arrays.asList(
              "id",
              "created",
              "text",
              "name",
              "coreRowType",
              "coreTerms",
              "extensions",
              "usage",
              "classification",
              "eventDate",
              "hasCoordinate",
              "hasGeospatialIssue",
              "gbifId",
              "crawlId",
              "networkKeys",
              "protocol",
              "issues",
              "machineTags"));

  private static final long serialVersionUID = 1279313931024806169L;
  // Core
  @NonNull private TupleTag<ExtendedRecord> erTag;
  @NonNull private TupleTag<BasicRecord> brTag;
  @NonNull private TupleTag<TemporalRecord> trTag;
  @NonNull private TupleTag<LocationRecord> lrTag;

  private TupleTag<TaxonRecord> txrTag;
  @NonNull private TupleTag<ALATaxonRecord> atxrTag;
  // Extension
  @NonNull private TupleTag<MultimediaRecord> mrTag;
  @NonNull private TupleTag<ImageRecord> irTag;
  @NonNull private TupleTag<AudubonRecord> arTag;
  @NonNull private TupleTag<MeasurementOrFactRecord> mfrTag;

  private TupleTag<LocationFeatureRecord> asrTag;

  private TupleTag<ALAAttributionRecord> aarTag;
  @NonNull private TupleTag<ALAUUIDRecord> urTag;

  private TupleTag<ALASensitivityRecord> srTag;

  @NonNull private TupleTag<ImageServiceRecord> isTag;

  @NonNull private TupleTag<TaxonProfile> tpTag;

  @NonNull private PCollectionView<MetadataRecord> metadataView;

  String datasetID;

  public static ALASolrDocumentTransform create(
      TupleTag<ExtendedRecord> erTag,
      TupleTag<BasicRecord> brTag,
      TupleTag<TemporalRecord> trTag,
      TupleTag<LocationRecord> lrTag,
      TupleTag<TaxonRecord> txrTag,
      TupleTag<ALATaxonRecord> atxrTag,
      TupleTag<MultimediaRecord> mrTag,
      TupleTag<ImageRecord> irTag,
      TupleTag<AudubonRecord> arTag,
      TupleTag<MeasurementOrFactRecord> mfrTag,
      TupleTag<LocationFeatureRecord> asrTag,
      TupleTag<ALAAttributionRecord> aarTag,
      TupleTag<ALAUUIDRecord> urTag,
      TupleTag<ImageServiceRecord> isTag,
      TupleTag<TaxonProfile> tpTag,
      TupleTag<ALASensitivityRecord> srTag,
      PCollectionView<MetadataRecord> metadataView,
      String datasetID) {
    ALASolrDocumentTransform t = new ALASolrDocumentTransform();
    t.erTag = erTag;
    t.brTag = brTag;
    t.trTag = trTag;
    t.lrTag = lrTag;
    t.txrTag = txrTag;
    t.atxrTag = atxrTag;
    t.mrTag = mrTag;
    t.irTag = irTag;
    t.arTag = arTag;
    t.mfrTag = mfrTag;
    t.asrTag = asrTag;
    t.aarTag = aarTag;
    t.urTag = urTag;
    t.srTag = srTag;
    t.isTag = isTag;
    t.tpTag = tpTag;
    t.metadataView = metadataView;
    t.datasetID = datasetID;
    return t;
  }

  /**
   * Create a SOLR document using the supplied records.
   *
   * @param mdr
   * @param er
   * @param br
   * @param tr
   * @param lr
   * @param txr
   * @param atxr
   * @param aar
   * @param asr
   * @return
   */
  @NotNull
  public static SolrInputDocument createSolrDocument(
      MetadataRecord mdr,
      BasicRecord br,
      TemporalRecord tr,
      LocationRecord lr,
      TaxonRecord txr,
      ALATaxonRecord atxr,
      ExtendedRecord er,
      ALAAttributionRecord aar,
      LocationFeatureRecord asr,
      ALAUUIDRecord ur,
      ImageServiceRecord isr,
      TaxonProfile tpr,
      ALASensitivityRecord sr) {

    // If a sensitive record, construct new versions of the data with adjustments
    if (sr != null && sr.getSensitive()) {
      Set<Term> sensitive =
          sr.getAltered().keySet().stream()
              .map(TERM_FACTORY::findTerm)
              .collect(Collectors.toSet());
      if (mdr != null) {
        mdr = MetadataRecord.newBuilder(mdr).build();
        SensitiveDataInterpreter.applySensitivity(sensitive, sr, mdr);
      }
      if (br != null) {
        br = BasicRecord.newBuilder(br).build();
        SensitiveDataInterpreter.applySensitivity(sensitive, sr, br);
      }
      if (tr != null) {
        tr = TemporalRecord.newBuilder(tr).build();
        SensitiveDataInterpreter.applySensitivity(sensitive, sr, tr);
      }
      if (lr != null) {
        lr = LocationRecord.newBuilder(lr).build();
        SensitiveDataInterpreter.applySensitivity(sensitive, sr, lr);
      }
      if (txr != null) {
        txr = TaxonRecord.newBuilder(txr).build();
        SensitiveDataInterpreter.applySensitivity(sensitive, sr, txr);
      }
      if (atxr != null) {
        atxr = ALATaxonRecord.newBuilder(atxr).build();
        SensitiveDataInterpreter.applySensitivity(sensitive, sr, atxr);
      }
      if (er != null) {
        er = ExtendedRecord.newBuilder(er).build();
        SensitiveDataInterpreter.applySensitivity(sensitive, sr, er);
      }
      if (aar != null) {
        aar = ALAAttributionRecord.newBuilder(aar).build();
        SensitiveDataInterpreter.applySensitivity(sensitive, sr, aar);
      }
      if (asr != null) {
        asr = LocationFeatureRecord.newBuilder(asr).build();
        SensitiveDataInterpreter.applySensitivity(sensitive, sr, asr);
      }
    }

    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", ur.getUuid());

    addToDoc(lr, doc, SKIP_KEYS);
    addToDoc(tr, doc, SKIP_KEYS);
    addToDoc(br, doc, SKIP_KEYS);
    addToDoc(er, doc, SKIP_KEYS);
    addToDoc(mdr, doc, SKIP_KEYS);

    // add event date
    try {
      if (tr.getEventDate().getGte() != null && tr.getEventDate().getGte().length() == 10) {
        doc.setField(
            "eventDateSingle",
            new SimpleDateFormat("yyyy-MM-dd").parse(tr.getEventDate().getGte()));
      } else {
        TemporalUtils.getTemporal(tr.getYear(), tr.getMonth(), tr.getDay())
            .ifPresent(x -> doc.setField("eventDateSingle", TemporalAccessorUtils.toDate(x)));
      }
    } catch (ParseException e) {
      log.error(
          "Unparseable date produced by downstream interpretation " + tr.getEventDate().getGte());
    }

    // GBIF taxonomy - add if available
    if (txr != null) {
      // add the classification
      List<RankedName> taxonomy = txr.getClassification();
      for (RankedName entry : taxonomy) {
        doc.setField("gbif_s_" + entry.getRank().toString().toLowerCase() + "_id", entry.getKey());
        doc.setField("gbif_s_" + entry.getRank().toString().toLowerCase(), entry.getName());
      }

      String rank = txr.getAcceptedUsage().getRank().toString();
      doc.setField("gbif_s_rank", txr.getAcceptedUsage().getRank().toString());
      doc.setField("gbif_s_scientificName", txr.getAcceptedUsage().getName());
    }

    // Verbatim (Raw) data
    Map<String, String> raw = er.getCoreTerms();
    for (Map.Entry<String, String> entry : raw.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith("http")) {
        key = key.substring(key.lastIndexOf("/") + 1);
      }
      doc.setField("raw_" + key, entry.getValue());
    }

    // Sensitive (Original) data
    if (sr != null && sr.getSensitive()) {
      doc.setField("sensitive", true);
      if (sr.getDataGeneralizations() != null)
        doc.setField("dataGeneralizations", sr.getDataGeneralizations());
      if (sr.getInformationWithheld() != null)
        doc.setField("informationWithheld", sr.getInformationWithheld());
      if (sr.getGeneralisationInMetres() != null)
        doc.setField("generalisationInMetres", sr.getGeneralisationInMetres());
      if (sr.getGeneralisationInMetres() != null)
        doc.setField("generalisationToApplyInMetres", sr.getGeneralisationInMetres());
      for (Map.Entry<String, String> entry : sr.getOriginal().entrySet()) {
        Term field = TERM_FACTORY.findTerm(entry.getKey());
        doc.setField("original_" + field.simpleName(), entry.getValue());
      }
    }

    if (lr.getDecimalLatitude() != null && lr.getDecimalLongitude() != null) {
      addGeo(doc, lr.getDecimalLatitude(), lr.getDecimalLongitude());
    }

    // ALA taxonomy & species groups - backwards compatible for EYA
    if (atxr.getTaxonConceptID() != null) {
      List<Schema.Field> fields = atxr.getSchema().getFields();
      for (Schema.Field field : fields) {
        Object value = atxr.get(field.name());
        if (value != null
            && !field.name().equals("speciesGroup")
            && !field.name().equals("speciesSubgroup")
            && !SKIP_KEYS.contains(field.name())) {
          if (field.name().equalsIgnoreCase("issues")) {
            doc.setField("assertions", value);
          } else {
            if (value instanceof Integer) {
              doc.setField(field.name(), value);
            } else {
              doc.setField(field.name(), value.toString());
            }
          }
        }
      }

      // required for EYA
      doc.setField(
          "names_and_lsid",
          String.join(
              "|",
              atxr.getScientificName(),
              atxr.getTaxonConceptID(),
              atxr.getVernacularName(),
              atxr.getKingdom(),
              atxr.getFamily())); // is set to IGNORE in headerAttributes

      doc.setField(
          "common_name_and_lsid",
          String.join(
              "|",
              atxr.getVernacularName(),
              atxr.getScientificName(),
              atxr.getTaxonConceptID(),
              atxr.getVernacularName(),
              atxr.getKingdom(),
              atxr.getFamily())); // is set to IGNORE in headerAttribute

      // legacy fields referenced in biocache-service code
      doc.setField("taxon_name", atxr.getScientificName());
      doc.setField("lsid", atxr.getTaxonConceptID());
      doc.setField("rank", atxr.getRank());
      doc.setField("rank_id", atxr.getRankID());

      if (atxr.getVernacularName() != null) {
        doc.setField("common_name", atxr.getVernacularName());
      }

      for (String s : atxr.getSpeciesGroup()) {
        doc.setField("species_group", s);
      }
      for (String s : atxr.getSpeciesSubgroup()) {
        doc.setField("species_subgroup", s);
      }
    }

    doc.setField("geospatial_kosher", lr.getHasCoordinate());
    doc.setField("first_loaded_date", new Date());

    if (asr != null) {
      Map<String, String> samples = asr.getItems();
      for (Map.Entry<String, String> sample : samples.entrySet()) {
        if (!StringUtils.isEmpty(sample.getValue())) {
          if (sample.getKey().startsWith("el")) {
            doc.setField(sample.getKey(), Double.valueOf(sample.getValue()));
          } else {
            doc.setField(sample.getKey(), sample.getValue());
          }
        }
      }
    }

    // Add legacy collectory fields
    if (aar != null) {
      addIfNotEmpty(doc, "license", aar.getLicenseType());
      addIfNotEmpty(
          doc, "raw_dataResourceUid", aar.getDataResourceUid()); // for backwards compatibility
      addIfNotEmpty(doc, "dataResourceUid", aar.getDataResourceUid());
      addIfNotEmpty(doc, "dataResourceName", aar.getDataResourceName());
      addIfNotEmpty(doc, "dataProviderUid", aar.getDataProviderUid());
      addIfNotEmpty(doc, "dataProviderName", aar.getDataProviderName());
      addIfNotEmpty(doc, "institutionUid", aar.getInstitutionUid());
      addIfNotEmpty(doc, "collectionUid", aar.getCollectionUid());
      addIfNotEmpty(doc, "institutionName", aar.getInstitutionName());
      addIfNotEmpty(doc, "collectionName", aar.getCollectionName());
    }

    // legacy fields reference directly in biocache-service code
    if (txr != null) {
      IssueRecord taxonomicIssues = txr.getIssues();
      for (String issue : taxonomicIssues.getIssueList()) {
        doc.setField("assertions", issue);
      }
    }

    if (isr != null && isr.getImageIDs() != null && !isr.getImageIDs().isEmpty()) {
      doc.setField("image_url", isr.getImageIDs().get(0));
      for (String imageID : isr.getImageIDs()) {
        doc.setField("all_image_url", imageID);
      }
      // FIX ME - do we need mime type.....
      doc.setField("multimedia", "Image");
    }

    if (tpr != null && tpr.getSpeciesListID() != null && !tpr.getSpeciesListID().isEmpty()) {

      for (String speciesListID : tpr.getSpeciesListID()) {
        doc.setField("species_list_uid", speciesListID);
      }

      // CONSERVATION STATUS
      String stateProvince = lr.getStateProvince();
      String country = lr.getCountry();

      // index conservation status
      List<ConservationStatus> conservationStatuses = tpr.getConservationStatuses();
      for (ConservationStatus conservationStatus : conservationStatuses) {
        if (conservationStatus.getRegion() != null) {
          if (conservationStatus.getRegion().equalsIgnoreCase(stateProvince)) {

            if (Strings.isNotBlank(conservationStatus.getSourceStatus())) {
              doc.setField("raw_state_conservation", conservationStatus.getSourceStatus());
            }
            if (Strings.isNotBlank(conservationStatus.getStatus())) {
              doc.setField("state_conservation", conservationStatus.getStatus());
            }
          }
          if (conservationStatus.getRegion().equalsIgnoreCase(country)) {
            if (Strings.isNotBlank(conservationStatus.getStatus())) {
              doc.setField("country_conservation", conservationStatus.getStatus());
            }
          }
        }
      }

      // index invasive status
      List<InvasiveStatus> invasiveStatuses = tpr.getInvasiveStatuses();
      for (InvasiveStatus invasiveStatus : invasiveStatuses) {
        if (invasiveStatus.getRegion() != null) {
          if (invasiveStatus.getRegion().equalsIgnoreCase(stateProvince)) {
            doc.setField("state_invasive", "invasive");
          }
          if (invasiveStatus.getRegion().equalsIgnoreCase(country)) {
            doc.setField("country_invasive", "invasive");
          }
        }
      }
    }

    IssueRecord geospatialIssues = lr.getIssues();
    for (String issue : geospatialIssues.getIssueList()) {
      doc.setField("assertions", issue);
    }

    IssueRecord temporalIssues = tr.getIssues();
    for (String issue : temporalIssues.getIssueList()) {
      doc.setField("assertions", issue);
    }

    IssueRecord basisOfRecordIssues = br.getIssues();
    for (String issue : basisOfRecordIssues.getIssueList()) {
      doc.setField("assertions", issue);
    }

    IssueRecord sensitivityIssues = sr == null ? null : sr.getIssues();
    if (sensitivityIssues != null) {
      for (String issue : sensitivityIssues.getIssueList()) {
        doc.setField("assertions", issue);
      }
    }

    for (String issue : mdr.getIssues().getIssueList()) {
      doc.setField("assertions", issue);
    }

    return doc;
  }

  public ParDo.SingleOutput<KV<String, CoGbkResult>, SolrInputDocument> converter() {

    DoFn<KV<String, CoGbkResult>, SolrInputDocument> fn =
        new DoFn<KV<String, CoGbkResult>, SolrInputDocument>() {

          private final Counter counter =
              Metrics.counter(ALASolrDocumentTransform.class, AVRO_TO_JSON_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            // Core
            MetadataRecord mdr = c.sideInput(metadataView);
            ExtendedRecord er = v.getOnly(erTag, ExtendedRecord.newBuilder().setId(k).build());
            BasicRecord br = v.getOnly(brTag, BasicRecord.newBuilder().setId(k).build());
            TemporalRecord tr = v.getOnly(trTag, TemporalRecord.newBuilder().setId(k).build());
            LocationRecord lr = v.getOnly(lrTag, LocationRecord.newBuilder().setId(k).build());
            TaxonRecord txr = null;
            if (txrTag != null) {
              txr = v.getOnly(txrTag, TaxonRecord.newBuilder().setId(k).build());
            }

            // Extension
            MultimediaRecord mr = v.getOnly(mrTag, MultimediaRecord.newBuilder().setId(k).build());
            ImageRecord ir = v.getOnly(irTag, ImageRecord.newBuilder().setId(k).build());
            AudubonRecord ar = v.getOnly(arTag, AudubonRecord.newBuilder().setId(k).build());
            MeasurementOrFactRecord mfr =
                v.getOnly(mfrTag, MeasurementOrFactRecord.newBuilder().setId(k).build());

            // ALA specific
            ALAUUIDRecord ur = v.getOnly(urTag);
            ALATaxonRecord atxr = v.getOnly(atxrTag, ALATaxonRecord.newBuilder().setId(k).build());
            ALAAttributionRecord aar =
                v.getOnly(aarTag, ALAAttributionRecord.newBuilder().setId(k).build());

            ImageServiceRecord isr = null;
            if (isTag != null) {
              isr = v.getOnly(isTag, ImageServiceRecord.newBuilder().setId(k).build());
            }

            TaxonProfile tpr = null;
            if (tpTag != null) {
              tpr = v.getOnly(tpTag, TaxonProfile.newBuilder().setId(k).build());
            }

            // Sampling
            LocationFeatureRecord asr = null;
            if (asrTag != null) {
              asr = v.getOnly(asrTag, LocationFeatureRecord.newBuilder().setId(k).build());
            }
            ALASensitivityRecord sr = null;
            if (srTag != null) {
              sr = v.getOnly(srTag, null);
            }

            MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);

            SolrInputDocument doc =
                createSolrDocument(mdr, br, tr, lr, txr, atxr, er, aar, asr, ur, isr, tpr, sr);

            c.output(doc);
            counter.inc();
          }
        };

    return ParDo.of(fn).withSideInputs(metadataView);
  }

  static void addIfNotEmpty(SolrInputDocument doc, String fieldName, String value) {
    if (StringUtils.isNotEmpty(value)) {
      doc.setField(fieldName, value);
    }
  }

  static void addGeo(SolrInputDocument doc, double lat, double lon) {
    String latlon = "";
    // ensure that the lat longs are in the required range before
    if (lat <= 90 && lat >= -90d && lon <= 180 && lon >= -180d) {
      // https://lucene.apache.org/solr/guide/7_0/spatial-search.html#indexing-points
      latlon = lat + "," + lon; // required format for indexing geodetic points in SOLR
    }

    doc.addField("lat_long", latlon); // is set to IGNORE in headerAttributes
    doc.addField(
        "point-1", getLatLongString(lat, lon, "#")); // is set to IGNORE in headerAttributes
    doc.addField(
        "point-0.1", getLatLongString(lat, lon, "#.#")); // is set to IGNORE in headerAttributes
    doc.addField(
        "point-0.01", getLatLongString(lat, lon, "#.##")); // is set to IGNORE in headerAttributes
    doc.addField(
        "point-0.02",
        getLatLongStringStep(lat, lon, "#.##", 0.02)); // is set to IGNORE in headerAttributes
    doc.addField(
        "point-0.001", getLatLongString(lat, lon, "#.###")); // is set to IGNORE in headerAttributes
    doc.addField(
        "point-0.0001",
        getLatLongString(lat, lon, "#.####")); // is set to IGNORE in headerAttributes
  }

  static String getLatLongStringStep(Double lat, Double lon, String format, Double step) {
    DecimalFormat df = new DecimalFormat(format);
    // By some "strange" decision the default rounding model is HALF_EVEN
    df.setRoundingMode(java.math.RoundingMode.HALF_UP);
    return df.format(Math.round(lat / step) * step)
        + ","
        + df.format(Math.round(lon / step) * step);
  }

  /** Returns a lat,long string expression formatted to the supplied Double format */
  static String getLatLongString(Double lat, Double lon, String format) {
    DecimalFormat df = new DecimalFormat(format);
    // By some "strange" decision the default rounding model is HALF_EVEN
    df.setRoundingMode(java.math.RoundingMode.HALF_UP);
    return df.format(lat) + "," + df.format(lon);
  }

  static void addToDoc(SpecificRecordBase record, SolrInputDocument doc, Set<String> skipKeys) {

    record.getSchema().getFields().stream()
        .filter(n -> !skipKeys.contains(n.name()))
        .forEach(
            f ->
                Optional.ofNullable(record.get(f.pos()))
                    .ifPresent(
                        r -> {
                          Schema schema = f.schema();
                          Optional<Schema.Type> type =
                              schema.getType() == UNION
                                  ? schema.getTypes().stream()
                                      .filter(t -> t.getType() != Schema.Type.NULL)
                                      .findFirst()
                                      .map(Schema::getType)
                                  : Optional.of(schema.getType());

                          type.ifPresent(
                              t -> {
                                switch (t) {
                                  case BOOLEAN:
                                    doc.setField(f.name(), r);
                                    break;
                                  case FLOAT:
                                    doc.setField(f.name(), r);
                                    break;
                                  case DOUBLE:
                                    doc.setField(f.name(), r);
                                    break;
                                  case INT:
                                    doc.setField(f.name(), r);
                                    break;
                                  case LONG:
                                    doc.setField(f.name(), r);
                                    break;
                                  default:
                                    doc.setField(f.name(), r.toString());
                                    break;
                                }
                              });
                        }));
  }
}
