package com.vanilla.poc.scaladsl

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroReadSupport

import scala.util.Random

trait AbstractAvroParquet {

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  val conf = new Configuration()
  conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)

  val schema: Schema =new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Document\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"body\",\"type\":\"string\"}]}")

  val folder: String = "./"+Random.alphanumeric.take(8).mkString("")


  def afterAll(): Unit = {
    import scala.reflect.io.Directory

    val directory = new Directory(new File(folder))
    directory.deleteRecursively()

  }

}
