# GBIF Crawler CLI

The Crawler CLI (command-line interface) provides services that read and write RabbitMQ messages as well as talking
to Zookeeper in order to schedule, execute, and cleanup crawls. It uses the Crawler module to do the XML crawling and
does archive downloads directly. The occurrence fragments emitted as the last step of occurrence crawling are in turn
consumed by services in occurrence-cli. Checklists are not fragmented but on a successful crawl a message is sent which
then triggers work in the checklistbank-cli.

## To build the project
```
mvn clean install
```

## CLI targets
### scheduler
* Publishes `StartCrawlMessage` messages (routing key: "crawl.start").

### coordinator
* Listens to `StartCrawlMessage` messages.
* Creates and adds crawling jobs to crawling queues in ZooKeeper.

### crawlserver (XML)
* XML crawl server that listens to a Zookeeper queue ("xml").
* Publishes `CrawlResponseMessage` (routing key: "crawl.response").
* Publishes `CrawlFinishedMessage` (routing key: "crawl.finished") on finish.
* Publishes `CrawlErrorMessage` (routing key: "crawl.error" + errorType) on error.

### fragmenter (XML)
* Listens to `CrawlResponseMessage` messages.
* Publishes `OccurrenceFragmentedMessage` (routing key: "crawler.fragment.new") on success.

### downloader (DwC-A)
* DwC-A crawl server that listens to a Zookeeper queue ("dwca").
* Publishes `DwcaDownloadFinishedMessage` (routing key: "crawl.dwca.download.finished") on success.

### abcdadownloader (ABCD-A)
* ABCD-A crawl server that listens to a Zookeeper queue ("abcda").
* Publishes `AbcdaDownloadFinishedMessage` (routing key: "crawl.abcda.download.finished") on success.

### validator (DwC-A)
* Listens to `DwcaDownloadFinishedMessage` messages.
* Publishes `DwcaValidationFinishedMessage` (routing key: "crawl.dwca.validation.finished") on success.

### dwca-metasync (DwC-A)
* Listens to `DwcaValidationFinishedMessage` messages.
* Publishes `DwcaMetasyncFinishedMessage` (routing key: "crawl.dwca.metasync.finished") on success.

### dwcafragmenter (DwC-A)
* Listens to `DwcaMetasyncFinishedMessage` messages.
* Publishes `OccurrenceFragmentedMessage` (routing key: "crawler.fragment.new") on success.

### abcdapager (ABCD-A)
* Listens to `AbcdaDownloadFinishedMessage` messages.
* Publishes `CrawlResponseMessage` (routing key: "crawler.fragment.new") on success.

### metasynceverything
* Publishes `StartMetasyncMessage` messages (routing key: "metasync.start").

### metasync
* Listen to `StartMetasyncMessage` messages.
* Supported protocols are: DiGIR, TAPIR and BioCASe.

### pipelines
![pipelines crawler](docs/pipelines-crawler.png)
