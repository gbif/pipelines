package org.gbif.pipelines.fragmenter;

import java.nio.file.Path;

import org.gbif.pipelines.fragmenter.common.FragmentsUploader;
import org.gbif.pipelines.fragmenter.common.HbaseConfiguration;

public class DwcaFragmentsUploader extends FragmentsUploader {

  public DwcaFragmentsUploader(HbaseConfiguration config, Path pathToArchive) {
    super(config, pathToArchive);
  }

  @Override
  public void upload() {
    throw new UnsupportedOperationException("EMPTY!");
  }

}
