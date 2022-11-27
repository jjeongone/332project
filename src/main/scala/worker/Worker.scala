package cs332.worker
import cs332.protos.sorting._


import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}

import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}
import cs332.protos.sorting.SorterGrpc._
import java.io.File
import java.io.InputStream
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import java.nio.file.{Files, Paths}

object Worker {
    def apply(host: String, port: Int): Worker = {
        val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
        val blockingStub = SorterGrpc.blockingStub(channel)
        val newStub = SorterGrpc.stub(channel)
        new Worker(channel, blockingStub, newStub)
    }

    def main(args: Array[String]): Unit = {
        val client = Worker("localhost", 50051)
        try {
            val user = args.headOption.getOrElse("world")
            client.greet(user)
            client.sendFile()
        } finally {
        client.shutdown()
        }
    }
}

class Worker private(
    private val channel: ManagedChannel, 
    private val blockingStub: SorterBlockingStub,
    private val newStub: SorterStub
) {
    private[this] val logger = Logger.getLogger(classOf[Worker].getName)

    def shutdown(): Unit = {
        channel.shutdown.awaitTermination(600, TimeUnit.SECONDS)
    }

    def greet(name: String): Unit = {
        logger.info("Will try to greet " + name + " ...")
        val request = HelloRequest(name = name)
        try {
            val response = blockingStub.sayHello(request)
            logger.info("Greeting: " + response.message)
        }
        catch {
            case e: StatusRuntimeException =>
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
        }
    } 

    def sendFile(): Unit = {
        logger.info("Will try to send file "  + " ...")
        val streamObserver: StreamObserver[PivotRequest] = newStub.getWorkerPivots(
            new StreamObserver[PivotResponse] {
                override def onNext(response: PivotResponse): Unit =  {
                    System.out.println(
                            "File upload status :: " + response.status
                    )
                }

                override def onError(throwaable: Throwable): Unit = {}

                override def onCompleted(): Unit = {}
            })  
        
        val path = Paths.get("src/main/scala/worker/input/sample/sample.1")


        val metadata = Metadata(fileName = "sample", fileType = "1")

        val inputStream: InputStream = Files.newInputStream(path)
        var bytes = Array.ofDim[Byte](2000000)
        var size: Int = 0
        size = inputStream.read(bytes)
        while (size > 0){
            val content = ByteString.copyFrom(bytes, 0 , size)
            val file = FileMessage(content = content)
            val fileRequest = PivotRequest(metadata = Option(metadata), file = Option(file))
            streamObserver.onNext(fileRequest)
            size = inputStream.read(bytes)
        }
        
        inputStream.close()
        streamObserver.onCompleted()
    }

}