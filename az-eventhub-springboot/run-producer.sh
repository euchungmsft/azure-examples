#!/bin/sh

#gradle build -x test

export CP1=build/libs/az-eventhub-springboot-0.0.1-SNAPSHOT.jar
export CP1=$CP1:build/libs/az-eventhub-springboot-0.0.1-SNAPSHOT-plain.jar

java -cp $CP1 com.eg.az.eh.springboot.TestProducer