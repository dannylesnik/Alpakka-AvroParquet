package com.vanilla.poc.scaladsl

import akka.Done
import akka.stream._
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import scala.concurrent.{Future, Promise}


object AvroParquetSink {

  def apply(path:String, schema: Schema,conf:Configuration): Graph[SinkShape[GenericRecord], Future[Done]] =
    new AvroParquetSink(path,schema,conf)
}

 class AvroParquetSink(path:String, schema: Schema, conf:Configuration) extends GraphStageWithMaterializedValue[SinkShape[GenericRecord],Future[Done]] {

   val in: Inlet[GenericRecord] = Inlet("ParquetSink")


   override def shape: SinkShape[GenericRecord] = SinkShape.of(in)

   override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
     val promise: Promise[Done] = Promise[Done]

     val writer: ParquetWriter[GenericRecord] = AvroParquetWriter.builder[GenericRecord](new Path(path)).withConf(conf).withSchema(schema).build()
     val logic: GraphStageLogic = new GraphStageLogic(shape) {

       override def preStart(): Unit = pull(in)

       setHandler(in, new InHandler {


         override def onUpstreamFinish(): Unit = {
           //super.onUpstreamFinish()
           writer.close()
           promise.success(Done)
         }

         override def onUpstreamFailure(ex: Throwable): Unit = {
           promise.failure(ex)
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
     }
     (logic,promise.future)

   }
 }