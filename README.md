Extensible [xLiMe](http://xlime.eu) [Kafka](http://apache.kafka.org) Consumer

Allows you to consume xLiMe messages (adhering to the xLiMe data model) posted to some Kafka instance (typically by the various xLiMe partners) 
and let's you focus on what you want to do with the data, rather than how to define kafka consumers, how to parse the messages, how to handle errors, etc.

As the number of things you may want to do with each message is open-ended, this library only provides a framework for 
performing common tasks such as:
 * implementing a [Kafka consumer](https://kafka.apache.org/081/documentation.html#theconsumer)
 * configuring, launching, restarting (etc.) one or more Kafka consumers
 * for known xLiMe topics, for which we know the format of the messages (e.g. RDF TRIG), define configurable processors which take care of the 
 	common tasks such as parsing the specific RDF format and provide an interface for dealing with the actual RDF graph.  
 
Example processing can be:
 * monitoring of how many messages are posted, how many triples, etc.
 * converting to another format (e.g. JSONLD to push into document DB)
 * parsing and pushing into a triplestore

# To execute
  * build the project (`mvn install`)
  * Implement a new `DatasetProcessor` for one of the known topics (or use the example `JsonLDStorer`)
  * create a new `properties` (adapt the example in `src/main/resources/xlime-kafka-consumer-sample-config.properties`). In particular, 
  	* point to an xLiMe Kafka instance (contact the developers to request access to the xLiMe Kafka), 
  	* specify which topic(s) you want to consume, for how long and which `DatasetProcessor` to use.  
  * execute `RunExtractor`

Copyright 2016 The xLiMe Consortium
Licensed under the Apache Software License, Version 2.0

