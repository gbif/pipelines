package org.gbif.pipelines.tools;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import static java.nio.charset.StandardCharsets.UTF_8;

@Mojo(name = "postprocess", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class AvroPostprocessMojo extends AbstractMojo {

  private static final String DEFAULT = "-gbif";
  private static final String ISSUE = "getIssues() {";
  private static final String BEFORE = "@SuppressWarnings(\"all\")";
  private static final String INTER_BASE = "org.apache.avro.specific.SpecificRecord";
  private static final String INTER = INTER_BASE + " {";

  @Parameter(property = "postprocess.directory", defaultValue = DEFAULT)
  private String directory;

  @Parameter(property = "postprocess.defaultPackage", defaultValue = DEFAULT)
  private String defaultPackage;

  @Override
  public void execute() throws MojoExecutionException {

    if (!DEFAULT.equals(directory) && !DEFAULT.equals(defaultPackage)) {
      boolean interfaceExist = createIssueInterface();

      if (!interfaceExist) {
        searchClasses().forEach(this::modifyFile);
      }
    }
  }

  /** TODO: FILL DOC! */
  private void modifyFile(Path path) {
    List<String> lines = getLines(path);
    List<Integer> idxs = getIdx(lines);

    changeInterface(lines, idxs);
    addAvroCodecAnnotation(lines, idxs);
    addOverrideMethod(lines, idxs);

    writeFile(path, lines, idxs);
  }

  /** TODO: FILL DOC! */
  private void writeFile(Path path, List<String> lines, List<Integer> idxs) {
    if (idxs.get(0) != -1 || idxs.get(1) != -1) {
      try {
        Files.write(path, lines);
      } catch (IOException ex) {
        throw new IORuntimeException(ex.getMessage(), ex);
      }
      getLog().info("Modified - " + path.toString());
    }
  }

  /** TODO: FILL DOC! */
  private void addOverrideMethod(List<String> lines, List<Integer> idxs) {
    int ovrdIdx = idxs.get(2);
    if (ovrdIdx != -1) {
      lines.add(ovrdIdx, "@Override ");
    }
  }

  /** TODO: FILL DOC! */
  private void addAvroCodecAnnotation(List<String> lines, List<Integer> idxs) {
    int beforeIdx = idxs.get(0);
    if (beforeIdx != -1) {
      String imports =
          "import org.apache.beam.sdk.coders.AvroCoder;\nimport org.apache.beam.sdk.coders.DefaultCoder;";
      lines.add(beforeIdx, imports);
      lines.add(beforeIdx + 1, "@DefaultCoder(AvroCoder.class)");
    }
  }

  /** TODO: FILL DOC! */
  private void changeInterface(List<String> lines, List<Integer> idxs) {
    int interIdx = idxs.get(1);
    int ovrdIdx = idxs.get(2);
    if (interIdx != -1 && ovrdIdx != -1) {
      String replace = INTER_BASE + ", " + defaultPackage + ".Issue {";
      replace = lines.get(interIdx).replace(INTER, replace);
      lines.set(interIdx, replace);
    }
  }

  /** TODO: FILL DOC! */
  private List<Integer> getIdx(List<String> lines) {
    int beforeIdx = -1;
    int interIdx = -1;
    int ovrdIdx = -1;
    for (int x = 0; x < lines.size(); x++) {
      String line = lines.get(x);
      if (beforeIdx != -1 && interIdx != -1 && ovrdIdx != -1) {
        break;
      }
      if (line.equals(BEFORE)) {
        beforeIdx = x;
      } else if (line.endsWith(INTER)) {
        interIdx = x;
      } else if (line.endsWith(ISSUE)) {
        ovrdIdx = x;
      }
    }

    return Arrays.asList(beforeIdx, interIdx, ovrdIdx);
  }

  /** TODO: FILL DOC! */
  private List<String> getLines(Path path) {
    try {
      return Files.readAllLines(path);
    } catch (IOException ex) {
      throw new IORuntimeException(ex.getMessage(), ex);
    }
  }

  /** TODO: FILL DOC! */
  private List<Path> searchClasses() {
    try (Stream<Path> paths = Files.walk(Paths.get(directory))) {
      return paths.filter(path -> path.toFile().isFile()).collect(Collectors.toList());
    } catch (IOException ex) {
      throw new IORuntimeException(ex.getMessage(), ex);
    }
  }

  /** TODO: FILL DOC! */
  private boolean createIssueInterface() {
    String path = directory + defaultPackage.replaceAll("\\.", "/") + "/Issue.java";
    String clazz =
        "package "
            + defaultPackage
            + ";\npublic interface Issue {\n  org.gbif.pipelines.io.avro.IssueRecord getIssues();\n}";
    try {
      Path path1 = Paths.get(path);
      boolean exists = path1.toFile().exists();
      if (!exists) {
        Files.write(path1, clazz.getBytes(UTF_8));
      }
      return exists;
    } catch (IOException ex) {
      throw new IORuntimeException(ex.getMessage(), ex);
    }
  }
}
