package com.vanilla.poc.javadsl

import akka.NotUsed
import akka.stream.javadsl.Source
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration

object AvroParquetSource {

  def create(filePath:String,conf:Configuration,schema:Schema): Source[GenericRecord, NotUsed] = Source.fromGraph( new com.vanilla.poc.scaladsl.AvroParquetSource(filePath,conf))


}
