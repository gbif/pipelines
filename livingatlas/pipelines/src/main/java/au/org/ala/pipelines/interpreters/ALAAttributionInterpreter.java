package au.org.ala.pipelines.interpreters;

import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;

import au.org.ala.kvs.client.ALACollectionLookup;
import au.org.ala.kvs.client.ALACollectionMatch;
import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.kvs.client.EntityReference;
import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.directory.api.util.Strings;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.io.avro.ALAAttributionRecord;
import org.gbif.pipelines.io.avro.ALAMetadataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;

/** Attribution interpreter providing additional attribution information to records. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAAttributionInterpreter {

  public static Consumer<MetadataRecord> interpretDatasetKeyAsMetadataRecord(
      String datasetId, KeyValueStore<String, ALACollectoryMetadata> dataResourceKvStore) {
    return (mdr) -> {
      if (dataResourceKvStore != null && datasetId != null) {
        ALACollectoryMetadata m = dataResourceKvStore.get(datasetId);
        if (m != null && !m.equals(ALACollectoryMetadata.EMPTY)) {
          mdr.setDatasetKey(m.getUid());
          mdr.setDatasetTitle(m.getName());
          mdr.setLicense(m.getLicenseType());
          if (m.getProvider() != null) {
            mdr.setPublisherTitle(m.getProvider().getName());
          }
        } else {
          if (dataResourceKvStore == null) {
            throw new RuntimeException(
                "Unable to retrieve connection parameters for dataset: "
                    + datasetId
                    + ", dataResourceKvStore is NULL");
          }
          if (datasetId == null) {
            throw new RuntimeException(
                "Unable to retrieve connection parameters. datasetId is NULL");
          }
        }
      }
    };
  }

  public static Consumer<ALAMetadataRecord> interpretDatasetKey(
      String datasetId, KeyValueStore<String, ALACollectoryMetadata> dataResourceKvStore) {
    return (mdr) -> {
      if (dataResourceKvStore != null && datasetId != null) {
        ALACollectoryMetadata m = dataResourceKvStore.get(datasetId);
        if (m != null && !m.equals(ALACollectoryMetadata.EMPTY)) {
          mdr.setDataResourceUid(m.getUid());
          if (m.getProvider() != null) {
            mdr.setDataProviderUid(m.getProvider().getUid());
            mdr.setDataProviderName(m.getProvider().getName());
          }
          mdr.setDataResourceName(m.getName());
          mdr.setLicenseType(m.getLicenseType());
          mdr.setLicenseVersion(m.getLicenseVersion());
          mdr.setProvenance(m.getProvenance());
          mdr.setHasDefaultValues(
              m.getDefaultDarwinCoreValues() != null && !m.getDefaultDarwinCoreValues().isEmpty());
          mdr.setContentTypes(m.getContentTypes());
          // hub memberships
          List<EntityReference> hubs = m.getHubMembership();
          if (hubs != null && !hubs.isEmpty()) {
            List<org.gbif.pipelines.io.avro.EntityReference> refs = new ArrayList<>();
            mdr.setHubMembership(refs);
            hubs.forEach(
                hub ->
                    refs.add(
                        org.gbif.pipelines.io.avro.EntityReference.newBuilder()
                            .setName(hub.getName())
                            .setUid(hub.getUid())
                            .setUri(hub.getUri())
                            .build()));
          }

        } else {
          throw new PipelinesException(
              "Unable to retrieve connection parameters for dataset: " + datasetId);
        }
      } else {
        if (dataResourceKvStore == null) {
          throw new PipelinesException(
              "Unable to retrieve connection parameters for dataset: "
                  + datasetId
                  + ", dataResourceKvStore is NULL");
        }
        throw new PipelinesException("Unable to retrieve connection parameters. datasetId is NULL");
      }
    };
  }

  public static BiConsumer<ExtendedRecord, ALAAttributionRecord> interpretCodes(
      KeyValueStore<ALACollectionLookup, ALACollectionMatch> collectionKvStore,
      ALAMetadataRecord mdr) {
    return (er, aar) -> {

      // copy values from MDR to AAR
      aar.setDataResourceUid(mdr.getDataResourceUid());
      aar.setDataResourceName(mdr.getDataResourceName());
      aar.setDataProviderUid(mdr.getDataProviderUid());
      aar.setDataProviderName(mdr.getDataProviderName());
      aar.setHubMembership(mdr.getHubMembership());
      aar.setLicenseType(mdr.getLicenseType());
      aar.setLicenseVersion(mdr.getLicenseVersion());
      aar.setProvenance(mdr.getProvenance());
      aar.setHasDefaultValues(mdr.getHasDefaultValues());
      aar.setHubMembership(mdr.getHubMembership());
      aar.setContentTypes(mdr.getContentTypes());

      if (collectionKvStore != null) {

        String collectionCode =
            er.getCoreTerms()
                .get(DwcTerm.collectionCode.namespace() + DwcTerm.collectionCode.name());
        String institutionCode =
            er.getCoreTerms()
                .get(DwcTerm.institutionCode.namespace() + DwcTerm.institutionCode.name());

        if (!Strings.isEmpty(collectionCode)) {
          ALACollectionLookup lookup =
              ALACollectionLookup.builder()
                  .collectionCode(collectionCode)
                  .institutionCode(institutionCode)
                  .build();
          ALACollectionMatch m = collectionKvStore.get(lookup);

          // Only the collection code is in the lookup
          if (m.getCollectionUid() == null) {
            addIssue(aar, ALAOccurrenceIssue.UNRECOGNISED_COLLECTION_CODE.name());
          }
          aar.setCollectionUid(m.getCollectionUid());
          aar.setCollectionName(m.getCollectionName());
          aar.setInstitutionUid(m.getInstitutionUid());
          aar.setInstitutionName(m.getInstitutionName());
        }
      }
    };
  }
}
