package com.vanilla.poc.javadsl

import java.util.concurrent.CompletionStage

import akka.stream.javadsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import com.vanilla.poc.scaladsl.AvroParquetFlow
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration

object AvroParquetSink{

  def create(path:String, schema: Schema, conf:Configuration): Sink[GenericRecord, CompletionStage[Done]] = {

    Flow.fromGraph( new AvroParquetFlow(path,schema,conf)).toMat(Sink.ignore(),Keep.right[NotUsed, CompletionStage[Done]])
  }

}


