> **Detailed plans:** See [plans/README.md](plans/README.md) for the master plan, individual feature plans, and ADRs.

# high level

we are going do to do a poc on how to collect contacts and customers from hubspot (the crm) and tripletex (a nordic erp).

we are using snowflake as data platform/hub.

we have 2 pipelines to fetch data from both systems (hubspot and tripletex) into hubspot and write back from snowflake into the systems.

this repo is only about the pipeline to fetch data from snowflake and write into hubspot and tripletex. the pipeline that transfers data from systems into snowflake is already developed.

archived-pipeline folder is a totally differnet pipeline developed before. it should not be touched or included in the decisions.

for simplicity we only care about the pipeline from snowflake into hubspot.


# features

- all the cleaning, normalization, merge, match and processes are done in snowflake. pipeline must only transfer data as it is in the format given to them.

- the pipelines should be mostly written with DLT Hub python library. DLT does have native connectors for Hubspot as a source and Tripletex as destination. but not vice versa.

- Pagination and Continuation should be supported.

- Batch processing should be supported when data is written back into Hubspot.


# ideas 

DLT Hubs does not have native support for Snowflake as source and Hubspot as destination. But it supports rest_api as source. we are not sure if rest_api is supported as destination.

The pipeline could fetch data from snowflake using rest_api connector and then write into Hubspot with regular api calls to Hubspot.