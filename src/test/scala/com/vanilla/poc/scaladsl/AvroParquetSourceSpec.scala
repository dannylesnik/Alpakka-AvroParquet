package com.vanilla.poc.scaladsl

import java.util.concurrent.TimeUnit

import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.{Done, NotUsed}
import org.apache.avro.generic.GenericRecordBuilder
import org.specs2.mutable.Specification
import org.specs2.specification.{AfterAll, BeforeAll}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class AvroParquetSourceSpec extends Specification with AbstractAvroParquet  with AfterAll with BeforeAll{


  "AvroParquetSource" should {

    "read from parquet file" in {

     val (_,sink) = AvroParquetSource( folder+"/test.parquet",conf).toMat(TestSink.probe)(Keep.both).run()

      sink.toStrict(Duration(3,TimeUnit.SECONDS)).seq.length shouldEqual 3


    }

  }

  override def beforeAll(): Unit = {
    case class Document(id:String, body:String)

    val docs = List[Document](
      Document("id1", "data1"),
      Document("id1", "data2"),
      Document("id3", " data3"))

    val source: Source[Document, NotUsed] =  akka.stream.scaladsl.Source.fromIterator(() =>docs.iterator)

    val file = folder+"/test.parquet"
    val sink = AvroParquetSink(file,schema,conf )

    val result: Future[Done] = source.map { doc =>
      new GenericRecordBuilder(schema)
        .set("id", doc.id).set("body", doc.body).build()
    }.
      runWith(sink)
    Await.result[Done](result,Duration(5,TimeUnit.SECONDS))
  }
}
