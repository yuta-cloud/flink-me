# Clonos

Clonos is a fault-tolerance method for distributed stream processors which enables high-availabiliy with exactly-once guarantees, without sacrificing expressiveness.

Previous approaches to high-availability restricted users to writing fully deterministic pipelines. 
This meant that you could not access the current time, use processing-time windowing, access external databases and services or even use timers.
But there is more to the story: input streams have to be buffered and sorted before processing, adding latency, while watermarks have to be generated less accurately and idle stream detection is not possible. 

The Clonos prototype is built on Apache Flink 1.7. Find out more at [http://flink.apache.org](http://flink.apache.org).

### Approach

Clonos leverages prior work in message-passing systems fault-tolerance to enable localized recovery as opposed to the global recovery employed by modern systems.

Instead of rolling back all participating processes to a previous checkpoint, Clonos replays lost epochs of data to a substitute process.
To replay nondeterministic actions performed during the processing of those epochs, Clonos maintains a log of such actions.
But unlike the logs generally maintained in database systems, this log can be maintained fully in-memory, as long as it is replicated by other processes.
This replication is continuously and transparently achieved through piggybacking on dataflow records.

To find out more about causal logging see [Message logging: Pessimistic, optimistic, causal, and optimal.](https://ieeexplore.ieee.org/abstract/document/666828)

### Features

* Standby tasks ready for fail-over continuously receive incremental state snapshots

* Causal services make it easy to use: just switch *System.currentTimeMillis()* to *getContext().getTimeService().currentTimeMillis()*.

* Configurable determinant sharing depth allows you to trade-off overhead for high-availability

* Spillable in-flight log ensures continued throughput even for small memory systems and large checkpoint intervals

* Configurable memory usage

* Handles different failure scenarios: frequent, concurrent and connected failures are no problem

* Automatically supports streaming libraries built for Flink. 


### Streaming Example

We modify Flink's wordcount example and introduce a filter which drops words on a banned list accessible through an external server

```scala
import scalaj.http.Http

case class WordWithCount(word: String, count: Long)

class ExternalBannedWordList(serverLocation: String) extends FilterFunction[String] with CheckpointedFunction = {

	var isBannedService: SerializableService[String, Boolean] = _	

  override def initializeState(context: FunctionInitializationContext): Unit = {
		// Building a causal service, sends http requests and records the response
    this.isBannedService = context.getSerializableServiceFactory
			.build((s: String) => { Http(serverLocation).param("word", s).asBoolean })
	}

	override def filter(x: String): Boolean = {
		isBannedService.apply(x) //Simply call the service, recovery is transparent
	}
}

val text = env.socketTextStream(host, port, '\n')

val windowCounts = text.flatMap { w => w.split("\\s") }
  .map { w => WordWithCount(w, 1) }
  .keyBy("word")
	.filter(ExternalBannedWordList("host:port/api/"))
  .timeWindow(Time.seconds(5))
  .sum("count")

windowCounts.print()
```

