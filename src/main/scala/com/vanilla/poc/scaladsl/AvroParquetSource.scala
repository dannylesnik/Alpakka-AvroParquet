package com.vanilla.poc.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader

object AvroParquetSource {

  def apply(filePath:String,conf:Configuration): Source[GenericRecord, NotUsed] = Source.fromGraph( new AvroParquetSource(filePath,conf))
}


class AvroParquetSource(filePath:String,conf:Configuration) extends GraphStage[SourceShape[GenericRecord]]{

  val out: Outlet[GenericRecord] = Outlet("AvroParquetSource")

  val reader: ParquetReader[GenericRecord] = AvroParquetReader.builder[GenericRecord](new Path(filePath)).withConf(conf).build()


  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    setHandler(out, new OutHandler {

      override def onDownstreamFinish(): Unit = {
        super.onDownstreamFinish()
        reader.close()
      }

      override def onPull(): Unit = {
        val record = reader.read()
        Option(record).fold{
          reader.close()
          complete(out)
        }(push(out, _))
      }
    })
  }
  override def shape: SourceShape[GenericRecord] =  SourceShape.of(out)
}
