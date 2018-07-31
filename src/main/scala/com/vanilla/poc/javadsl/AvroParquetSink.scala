package com.vanilla.poc.javadsl

import java.util.concurrent.CompletionStage
import akka.stream.javadsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import com.vanilla.poc.scaladsl.AvroParquetFlow
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter

object AvroParquetSink{

  def create(writer:ParquetWriter[GenericRecord]): Sink[GenericRecord, CompletionStage[Done]] = {

    Flow.fromGraph( new AvroParquetFlow(writer:ParquetWriter[GenericRecord])).toMat(Sink.ignore(),Keep.right[NotUsed, CompletionStage[Done]])
  }

}


