<h2>Ingestion platform to orchestrate the parsing and interpretation of biodiversity data</h2>

<h3>Project structure:</h3>
<ul>
    <li>buildSrc - Tools for building the project</li>
    <li>docs - Documents related to the project</li>
    <li>examples - Examples of using project API and base classes</li>
    <li>pipelines - Main pipelines module
        <ul>
            <li>base - Main transformations and pipelines for ingestion biodiversity data</li>
            <li>beam-common - Classes and API for using with Apache Beam</li>
            <li>mini-pipelines - Independed GBIF pipelines for ingestion biodiversity data</li>
        </ul>
    </li>
    <li>sdks - Main module contains common classes, such as data models, data format iterpretations, parsers, web services clients ant etc.
        <ul>
            <li>core - Main API classes, such as data interpretations, converters, DwCA reader and etc</li>
            <li>models - Data models represented in Avro binary format, generated from Avro schemas</li>
            <li>parsers - Data parsers and converters, mainly for internal usage inside of interpretations</li>
        </ul>
    </li>
    <li>tools - Module for different independent tools
        <ul>
            <li>archives-converters - Converters from DwCA/DWC 1.0/DWC 1.4/ABCD 1.2/ABCD 2.06 to *.avro format</li>
            <li>elasticsearch-tools - Tool for creating/deleting/swapping Elasticsearch indexes</li>
            <li>pipelines-maven-plugin - Maven plugin adds new annotations and interface to avro generated classes</li>
        </ul>
    </li>
</ul>