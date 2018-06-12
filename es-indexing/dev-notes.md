## ES client
* The transport client was discarded since it's going to be deprecated in ES 7.
* The High Level Rest Client was also discarded because it's not going to be completed until ES 7.
* Reasons above led to use the Low Level Rest Client. When we upgrade to ES 7 it could be considered to migrate to the
High Level Rest Client.

## Embedded ES

For testing we are using this library due to its flexibility to configure an available port in Java:

https://github.com/allegro/embedded-elasticsearch
