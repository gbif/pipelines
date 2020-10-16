package org.gbif.pipelines.core.converters;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.MediaType;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MultimediaConverter {

  public static MultimediaRecord merge(
      @NonNull MultimediaRecord mr, @NonNull ImageRecord ir, @NonNull AudubonRecord ar) {

    MultimediaRecord record =
        MultimediaRecord.newBuilder().setId(mr.getId()).setCreated(mr.getCreated()).build();

    boolean isMrEmpty = mr.getMultimediaItems() == null && mr.getIssues().getIssueList().isEmpty();
    boolean isIrEmpty = ir.getImageItems() == null && ir.getIssues().getIssueList().isEmpty();
    boolean isArEmpty = ar.getAudubonItems() == null && ar.getIssues().getIssueList().isEmpty();

    if (isMrEmpty && isIrEmpty && isArEmpty) {
      return record;
    }

    Set<String> issues = new HashSet<>();
    issues.addAll(mr.getIssues().getIssueList());
    issues.addAll(ir.getIssues().getIssueList());
    issues.addAll(ar.getIssues().getIssueList());

    Map<String, Multimedia> multimediaMap = new HashMap<>();
    // The orders of puts is important
    putAllAudubonRecord(multimediaMap, ar);
    putAllImageRecord(multimediaMap, ir);
    putAllMultimediaRecord(multimediaMap, mr);

    if (!multimediaMap.isEmpty()) {
      record.setMultimediaItems(new ArrayList<>(multimediaMap.values()));
    }

    if (!issues.isEmpty()) {
      record.getIssues().getIssueList().addAll(issues);
    }

    return record;
  }

  private static void putAllMultimediaRecord(Map<String, Multimedia> map, MultimediaRecord mr) {
    Optional.ofNullable(mr.getMultimediaItems())
        .filter(i -> !i.isEmpty())
        .ifPresent(
            c ->
                c.stream()
                    .filter(
                        m ->
                            !Strings.isNullOrEmpty(m.getReferences())
                                || !Strings.isNullOrEmpty(m.getIdentifier()))
                    .forEach(
                        r -> {
                          String key =
                              Optional.ofNullable(r.getIdentifier()).orElse(r.getReferences());
                          map.putIfAbsent(key, r);
                        }));
  }

  private static void putAllImageRecord(Map<String, Multimedia> map, ImageRecord ir) {
    Optional.ofNullable(ir.getImageItems())
        .filter(i -> !i.isEmpty())
        .ifPresent(
            c ->
                c.stream()
                    .filter(
                        m ->
                            !Strings.isNullOrEmpty(m.getReferences())
                                || !Strings.isNullOrEmpty(m.getIdentifier()))
                    .forEach(
                        r -> {
                          String key =
                              Optional.ofNullable(r.getIdentifier()).orElse(r.getReferences());
                          Multimedia multimedia =
                              Multimedia.newBuilder()
                                  .setAudience(r.getAudience())
                                  .setContributor(r.getContributor())
                                  .setCreated(r.getCreated())
                                  .setCreator(r.getCreator())
                                  .setDatasetId(r.getDatasetId())
                                  .setDescription(r.getDescription())
                                  .setFormat(r.getFormat())
                                  .setIdentifier(r.getIdentifier())
                                  .setLicense(r.getLicense())
                                  .setPublisher(r.getPublisher())
                                  .setReferences(r.getReferences())
                                  .setRightsHolder(r.getRightsHolder())
                                  .setTitle(r.getTitle())
                                  .setType(MediaType.StillImage.name())
                                  .build();
                          map.putIfAbsent(key, multimedia);
                        }));
  }

  private static void putAllAudubonRecord(Map<String, Multimedia> map, AudubonRecord ar) {
    Optional.ofNullable(ar.getAudubonItems())
        .filter(i -> !i.isEmpty())
        .ifPresent(
            c ->
                c.stream()
                    .filter(m -> !Strings.isNullOrEmpty(m.getAccessUri()))
                    .forEach(
                        r -> {
                          String key = r.getAccessUri();
                          String desc =
                              Strings.isNullOrEmpty(r.getDescription())
                                  ? r.getCaption()
                                  : r.getDescription();
                          String creator =
                              Strings.isNullOrEmpty(r.getCreator())
                                  ? r.getCreatorUri()
                                  : r.getCreator();
                          String identifier =
                              Strings.isNullOrEmpty(r.getAccessUri())
                                  ? r.getIdentifier()
                                  : r.getAccessUri();
                          Multimedia multimedia =
                              Multimedia.newBuilder()
                                  .setCreated(r.getCreateDate())
                                  .setCreator(creator)
                                  .setDescription(desc)
                                  .setFormat(r.getFormat())
                                  .setIdentifier(identifier)
                                  .setLicense(r.getRights())
                                  .setRightsHolder(r.getOwner())
                                  .setSource(r.getSource())
                                  .setTitle(r.getTitle())
                                  .setType(r.getType())
                                  .setSource(r.getSource())
                                  .setPublisher(r.getProviderLiteral())
                                  .build();
                          map.putIfAbsent(key, multimedia);
                        }));
  }
}
