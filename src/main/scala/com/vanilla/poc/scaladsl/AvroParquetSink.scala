package com.vanilla.poc.scaladsl

import akka.Done
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.stage._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter

import scala.concurrent.Future


object AvroParquetSink {

  def apply(path:String, schema: Schema,conf:Configuration): Graph[SinkShape[GenericRecord], Future[Done]] =
   Flow.fromGraph(new AvroParquetFlow(path, schema, conf)).toMat(Sink.ignore)(Keep.right)

}



class AvroParquetFlow(path:String, schema: Schema, conf:Configuration) extends GraphStage[FlowShape[GenericRecord,GenericRecord]] {

  val in:Inlet[GenericRecord] = Inlet("AvroParquetSink.in")
  val out:Outlet[GenericRecord] = Outlet("AvroParquetSink.out")
  override val shape: FlowShape[GenericRecord, GenericRecord] = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic = {
    val writer: ParquetWriter[GenericRecord] = AvroParquetWriter.builder[GenericRecord](new Path(path)).withConf(conf).withSchema(schema).build()

    new GraphStageLogic(shape) {
      setHandler(in, new InHandler {


        override def onUpstreamFinish(): Unit = {
          //super.onUpstreamFinish()
          writer.close()
          completeStage()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          super.onUpstreamFailure(ex)
          writer.close()
        }

        @scala.throws[Exception](classOf[Exception])
        override def onPush(): Unit = {
          val obtainedValue = grab(in)
          writer.write(obtainedValue)
          push(out,obtainedValue)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }
  }
}