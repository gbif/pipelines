package org.gbif.pipelines.fragmenter.common;

import java.nio.file.Path;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class FragmentsUploader {

  private HbaseConfiguration config;
  private Path pathToArchive;

  public abstract void upload();

}
