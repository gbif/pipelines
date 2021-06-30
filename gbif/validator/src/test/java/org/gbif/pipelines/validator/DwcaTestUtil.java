/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.validator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;

public class DwcaTestUtil {

  /**
   * Copies a zip archive or a single file from the test resources into a random uuid folder and
   * returns the opened archive.
   */
  public static Archive openArchive(String archiveResourcePath) throws IOException {
    UUID uuid = UUID.randomUUID();

    File srcFile = new File(DwcaTestUtil.class.getResource(archiveResourcePath).getFile());
    File tmpFile = new File(srcFile.getParentFile(), uuid + ".dwca");
    Files.copy(srcFile.toPath(), tmpFile.toPath());

    File dwcaDir = new File(tmpFile.getParent(), uuid.toString());
    if ("zip".equalsIgnoreCase(FilenameUtils.getExtension(srcFile.getName()))) {
      return DwcFiles.fromCompressed(tmpFile.toPath(), dwcaDir.toPath());
    }

    return DwcFiles.fromLocation(tmpFile.toPath());
  }

  public static void cleanupArchive(Archive archive) {
    File zip = archive.getLocation();
    FileUtils.deleteQuietly(zip);
  }
}
