# Contributing to GBIF Pipelines

The GBIF Pipelines development community welcomes contributions from anyone! Thank you.

If you have questions please open [an issue](https://github.com/gbif/pipelines/issues/new) or join the [mailing list](https://lists.gbif.org/mailman/listinfo/pipelines).

There are different ways you can contribute that help this project:
- Log [issues](https://github.com/gbif/pipelines/issues) and help document a bug or specify new functionality
- Improve documentation
- Provide code submissions that address issues or bring new functionalities
- Help test the functionalities offered in the project
- Help improve this guide

## Contributing code

Below is a tutorial for contributing code to Pipelines, covering our tools and typical process in detail.

### Prerequisites

To contribute code, you need

- a GitHub account
- a Linux, MacOS, or Microsoft Windows development environment with Java JDK 8 installed 
- Maven (version 3.6+)

### Connect with the Pipeline community and share your intent

- This is a very active project, with dependencies coming from those running in production.
- It is always worthwhile announcing your intention, so the community can offer guidance.
- Please always start with an issue to capture discussion

### Development setup

#### Command line

This project uses [Apache Maven](https://maven.apache.org/run.html) to build. The following should be enough to checkout and build the project.


```
git clone git@github.com:gbif/pipelines.git
cd pipelines
build.sh
```

Using maven commands, the project can be built with `mvn clean package` (optionally with `-DskipTests` to skip tests) and integration tests run with `mvn verify`.

We use [Google Java Format](https://plugins.jetbrains.com/plugin/8527-google-java-format) for code styling.

From the command line you can check the style using `mvn spotless:check` and fixup styling issues using `mvn spotless:apply`.

#### IDE Setup

We recommend the following toolset, and are able to answer questions on this configuration:

- Use [IntelliJ IDEA Community](https://www.jetbrains.com/idea/download/) (or better),
- Use the [Google Java Format](https://plugins.jetbrains.com/plugin/8527-google-java-format). Please do not reformat existing code, only changes and new code.
- The project uses [Project Lombok](https://projectlombok.org/). Please install the [Lombok plugin for IntelliJ IDEA](https://plugins.jetbrains.com/plugin/6317-lombok-plugin).
- Because the project uses [Error Prone](https://code.google.com/p/error-prone) you may have issues during the build process from IDEA.  To avoid these issues please install the [Error Prone compiler integration plugin](https://plugins.jetbrains.com/plugin/7349-error-prone-compiler-integration) and build the project using the [`error-prone java compiler`](https://code.google.com/p/error-prone) to catch common Java mistakes at compile-time. To use the compiler, go to _File_ → _Settings_ → _Compiler_ → _Java Compiler_ and select `Javac with error-prone` in the `Use compiler` box.
- Add a custom parameter to avoid a debugging problem.  To use the compiler, go to _File_ → _Settings_ → _Compiler_ → _Java Compiler_ → _Additional command line parameters_ and add `-Xep:ParameterName:OFF`
- Tests: please follow the conventions of the Maven Surefire plugin for [unit tests](https://maven.apache.org/surefire/maven-surefire-plugin/examples/inclusion-exclusion.html) and those of the Maven Failsafe plugin for [integration tests](https://maven.apache.org/surefire/maven-failsafe-plugin/examples/inclusion-exclusion.html). To run the integration tests just run the verify phase, e.g.: `mvn clean verify`

### Understanding branching

We follow a [GitFlow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow) approach to our project.
This can be summarized as:
1. `master` represents what is running in production, or is in the process of being released for a new deployment
2. `gbif-dev` represents the latest state of development, and is what is run by the [continuous build tools](https://builds.gbif.org/)
   1. Generally feature branches are made from here
3. `ala-dev` represents a large feature branch to bring in the `livingatlas` module and pipelines for the ALA work

If you are working on a new feature and not part of the core team please ask for guidance on where to start (most likely `gbif-dev`).

### Make your change

1. Checkout the branch you need (or fork the project and then checkout the branch)
2. Create a feature branch following a naming convention of `<issue_number>_my_new_feature`
3. Add unit tests for your change (please see testing style below)
4. Arrange your commits
    1. Consider merging commits. We favour fewer commits, but recognize this is not always desirable for large features (please squash small commits addressing typos etc. into one)
5. Use descriptive commit messages that make it easy to identify changes and provide a clear history.
    1. Please reference the issue you are working on unless it is a trivial change
    2. Examples of good commit messages:
        1. `#123 Enable occurrenceStatus interpretation to Avro`
        2. `Fixup: Addressing typos in JDoc`
    3. Examples of bad commit messages:
        1. ` `
        2. `Various fixes`
        3. `#123`
6. Check your code compiles, and all project unit tests pass (please be kind to reviewers)
7. Explore the `errorprone` warnings raised at compilation time. Please address issues you see as best you can.
8. Ensure that the code is spotless (`mvn spotless:check` and fixup styling issues using `mvn spotless:apply`)
9. Verify that the PR only changes the code necessary to address the issue (other fixes should be in separate PRs)
10. Prefer to create a pull request and have it reviewed for larger submissions. Committers are free to push *smaller* changes directly.
11. Committers are requested to delete branches once merged.

### Test code style

The following illustrates the preferred style for unit tests.

```java
@Test
public void allValuesNullTest() {
  // State
  String eventDate = null;
  String year = null;
  String month = null;
  String day = null;

  // When
  ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

  // Should
  assertFalse(result.getFromOpt().isPresent());
  assertFalse(result.getToOpt().isPresent());
  assertFalse(result.getYearOpt().isPresent());
  assertFalse(result.getMonthOpt().isPresent());
  assertFalse(result.getDayOpt().isPresent());
  assertFalse(result.getStartDayOfYear().isPresent());
  assertFalse(result.getEndDayOfYear().isPresent());
  assertTrue(result.getIssues().isEmpty());
}
```
