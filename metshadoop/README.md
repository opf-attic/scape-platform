Hadoop and METS
===============

The aim of this project is to make METS files usable within the Hadoop framework. The core of it are 

* MetsInputFormat with MetsRecordReader
* MetsOutputFormat with MetsRecordWriter

The first two read METS-elements from an input XML and unmarshal these into METS objects, e.g. MetsDocument or IntellectualEntity. The latter two write METS objects into one XML. [SCAPE-DTO](http://github.com/fasseg/scape-dto) and [METS-DTO](http://github.com/fasseg/mets-dto) are used for these transformations.

[DummyJob](http://github.com/scape-platform/metshadoop/src/main/java/eu/scape_project/pt/metshadoop/DummyJob.java) demonstrates a dummy Hadoop job using *MetsInputFormat* and *MetsOutputFormat*. Invoke it on a running Hadoop installation by following command:

``hadoop jar /path/to/metshadoop/target/metshadoop-0.1-SNAPSHOT-jar-with-dependencies.jar``

In preparation of the job two dummy intellectual entities are ingested into a mock-up repository using [SCAPE-TCK](http://github.com/fasseg/scape-tck). Then the XML representing both in a entity-list is retrieved and put to HDFS. The file reference is passed on to the Hadoop job. The Mapper of the job gets the Id of the entity and a [DTO](http://github.com/scape-platform/metshadoop/src/main/java/eu/scape_project/pt/metshadoop/DTO.java), a "digital transfer object" which wraps the METS object to be handable within Hadoop. The Mapper only posts a debug message and doesn't do anything else.

There is no need for reduce method in this dummy job. Map output is the Id and the DTO. All DTOs are collected by Hadoop and written to an output xml file (output/part-r-00000.xml by default) using *MetsOutputFormat* and *MetsRecordWriter*.
