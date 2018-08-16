package com.vanilla.poc.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader

object StreamAvroParquetImplicits {

  implicit class AdditionalSources(source: Source.type) {
    def fromAvroParquet(reader:ParquetReader[GenericRecord]): Source[GenericRecord, NotUsed] =
      AvroParquetSource(reader)
  }

}
