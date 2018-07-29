# Alpakka-AvroParquet

Avro Parquets connector provides Akka Stream Sink and Source to read and write data from and to Apache Parquet files.

Usage
-----

Straight forward!!!

Scala:
```scala
implicit val system: ActorSystem = ActorSystem()
implicit val mat: ActorMaterializer = ActorMaterializer()

val conf = new Configuration()
conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)

val schema: Schema =new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Document\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"body\",\"type\":\"string\"}]}")

//Sink
val sink = AvroParquetSink("./sample.parquet",schema,conf )

val result: Future[Done] = source.map { doc =>
  new GenericRecordBuilder(schema)
    .set("id", doc.id).set("body", doc.body).build()
}.runWith(sink)


//Source
AvroParquetSource( folder+"/sample.parquet",conf).runWith(Sink.ignore)
``` 

Java:

```java
ActorSystem system = ActorSystem.create();
ActorMaterializer materializer = ActorMaterializer.create(system);

final Configuration conf = new Configuration();
conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true);

final Schema schema =new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Document\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"body\",\"type\":\"string\"}]}");

//Sink
Sink<GenericRecord, CompletionStage<Done>> sink = AvroParquetSink.create(file, schema, conf);
Source.from(records).runWith(sink, materializer);

//Source
AvroParquetSource.create("./sample.parquet", conf).runWith(Sink.ignore(),materializer);
```
