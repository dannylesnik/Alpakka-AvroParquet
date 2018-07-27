package com.vanilla.poc.javadsl

import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.stream._
import akka.stream.javadsl.{Flow, Keep, Sink}
import akka.stream.stage._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter

object AvroParquetSink{

  def create(path:String, schema: Schema, conf:Configuration) = {

    Flow.fromGraph( new AvroParquetFlow(path,schema,conf)).toMat(Sink.ignore(),Keep.right[NotUsed, CompletionStage[Done]])


  }

}


  class AvroParquetFlow(path:String, schema: Schema, conf:Configuration) extends GraphStage[FlowShape[GenericRecord,GenericRecord]] {

    val in:Inlet[GenericRecord] = Inlet("AvroParquetSink.in")
    val out:Outlet[GenericRecord] = Outlet("AvroParquetSink.out")
    
    
    override val shape: FlowShape[GenericRecord, GenericRecord] = FlowShape.of(in, out)


    override def createLogic(attr: Attributes): GraphStageLogic = {
      val filename: String = path + ".parquet"
      val writer: ParquetWriter[GenericRecord] = AvroParquetWriter.builder[GenericRecord](new Path(filename)).withConf(conf).withSchema(schema).build()

      new GraphStageLogic(shape) {
        setHandler(in, new InHandler {


          override def onUpstreamFinish(): Unit = {
            //super.onUpstreamFinish()
            writer.close()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            super.onUpstreamFailure(ex)
            writer.close()
          }

          @scala.throws[Exception](classOf[Exception])
          override def onPush(): Unit = {
            val obtainedValue = grab(in)
            writer.write(obtainedValue)
            pull(in)
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