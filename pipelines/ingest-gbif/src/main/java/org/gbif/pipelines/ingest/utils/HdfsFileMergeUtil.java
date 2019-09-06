package org.gbif.pipelines.ingest.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Utility class to merge dataset avro files in HDFS into files of a expected size.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HdfsFileMergeUtil {

  /**
   * Utility inner class to handle file merge data.
   */
  @Builder
  @Data
  private static class FileMerge {

    private final String fileName;

    private final Collection<FileStatus> fileStatuses = new ArrayList<>();

    private long size;



    FileMerge addFile(FileStatus fileStatus) {
      fileStatuses.add(fileStatus);
      size += fileStatus.getLen();
      return this;
    }

    void merge(FileMerge fileMerge) {
      fileMerge.getFileStatuses().forEach(this::addFile);
    }

  }

  /**
   * Tries to merge a file into a FileMerge with enough room (size < sizeThreshold)
   * @param merges existing file merges to check
   * @param fileMerge fileMerge to be merged
   * @param sizeThreshold expected file merge size
   */
  private static void tryMerge(Collection<FileMerge> merges, FileMerge fileMerge, long sizeThreshold) {
    boolean merged = false;
    for (FileMerge merge: merges) {
      if (!merge.getFileName().equals(fileMerge.getFileName()) && merge.getSize() + fileMerge.getSize() < sizeThreshold) {
        merge.merge(fileMerge);
        merged = true;
        break;
      }
    }
    if (merged) {
      merges.remove(fileMerge);
    }

  }

  /**
   *  Merges a list of {@link FileStatus} into a new file.
   * @param fs target file system where all files belong
   * @param fileName output file name and path
   * @param fileStatuses files to be merged
   * @throws IOException in case of error merging the files
   */
  private static void merge(FileSystem fs, String fileName, Collection<FileStatus> fileStatuses) throws IOException {
    try (FSDataOutputStream out = fs.create(new Path(fileName))) {
      for (FileStatus fileStatus : fileStatuses) {
        if (fileStatus.isFile()) {
          try (FSDataInputStream in = fs.open(fileStatus.getPath())) {
            IOUtils.copyBytes(in, out, fs.getConf(), false);
            fs.delete(fileStatus.getPath(), true);
          }
        }
      }
    }
  }


  /**
   * Copies a list files that match against a glob filter into a target directory.
   *
   * @param fs FileSystem
   * @param globFilter filter used to filter files and paths
   * @param targetPath target directory
   * @param fileNamePrefix prefix for the produced file
   * @param fileNamePostfix extension or postfix name of the produced file
   * @param sizeThreshold estimate file size to produce
   * @param minSizeThreshold if any file remain under this size it is merged into another file
   */
  public static void mergeFiles(FileSystem fs, String globFilter, String targetPath, String fileNamePrefix,
                                String fileNamePostfix, long sizeThreshold, long minSizeThreshold) {
    List<FileMerge> merges = new ArrayList<>();
    try {
      for (FileStatus status : fs.globStatus(new Path(globFilter))) {
        boolean added = false;
        for (FileMerge fileMerge: merges) {
          if (fileMerge.getSize() + status.getLen() < sizeThreshold) {
            fileMerge.addFile(status);
            added = true;
          }
        }
        if (!added) {
          merges.add(FileMerge.builder()
                       .fileName(new Path( targetPath,fileNamePrefix + '_' + merges.size() + fileNamePostfix).toString())
                       .build().addFile(status));
        }
      }


      //Merges small files into other files
      Collection<FileMerge> smallMerges = merges.stream()
        .filter(fm -> fm.getSize() < minSizeThreshold)
        .collect(Collectors.toList());

      smallMerges.forEach(fm -> tryMerge(merges, fm, sizeThreshold + minSizeThreshold));


      //Merge all merges in the FilSystem
      for (FileMerge fileMerge : merges) {
        merge(fs, fileMerge.getFileName(), fileMerge.getFileStatuses());
      }

    } catch (IOException e) {
      log.warn("Can't move files using filter - {}, into path - {}", globFilter, targetPath);
      throw new RuntimeException(e);
    }
  }

  /**
   * Copies a list files that match against a glob filter into a target directory.
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param globFilter filter used to filter files and paths
   * @param targetPath target directory
   * @param fileNamePrefix prefix for the produced file
   * @param fileNamePostfix extension or postfix name of the produced file
   * @param sizeThreshold estimate file size to produce
   * @param minSizeThreshold if any file remain under this size it is merged into another file
   */
  public static void mergeFiles(String hdfsSiteConfig, String globFilter, String targetPath, String fileNamePrefix, String fileNamePostfix,
                                long sizeThreshold, long minSizeThreshold) {
    mergeFiles(FsUtils.getFileSystem(hdfsSiteConfig), globFilter, targetPath, fileNamePrefix, fileNamePostfix, sizeThreshold, minSizeThreshold);
  }

}
