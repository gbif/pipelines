package org.gbif.pipelines.core.converters;

import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;

public class MultimediaConverter {

  private MultimediaConverter() {}

  public static MultimediaRecord merge(MultimediaRecord mr, ImageRecord ir, AudubonRecord ar) {
    return mr;
  }
}
