package com.vanilla.poc.javadsl

import akka.NotUsed
import akka.stream.javadsl.Source
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetReader

object AvroParquetSource {

  def create(reader:AvroParquetReader[GenericRecord]): Source[GenericRecord, NotUsed] = Source.fromGraph( new com.vanilla.poc.scaladsl.AvroParquetSource(reader))


}
