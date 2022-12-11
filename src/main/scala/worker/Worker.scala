package cs332.worker

import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}

import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}

import java.util.concurrent.CountDownLatch
import cs332.protos.sorting.SorterGrpc._
import cs332.protos.sorting._
import cs332.common.Util.{getIPaddress, currentDirectory, splitEndpoint, readFilesfromDirectory}
import cs332.common.Util
import cs332.worker.WorkerJob._
import java.io.{File, InputStream}
import io.grpc.stub.StreamObserver
import java.nio.file.{Files, Paths}
import com.google.protobuf.ByteString

object Worker {
    def apply(host: String, port: Int): Worker = {
        val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
        val blockingStub = SorterGrpc.blockingStub(channel)
        val newStub = SorterGrpc.stub(channel)
        new Worker(channel, blockingStub, newStub)
    }


    def main(args: Array[String]): Unit = {
        val masterEndpoint = args.headOption
        val inputFileDirectory = args.slice(args.indexOf("-I")+1, args.indexOf("-O"))
        val outputFileDirectory = args.slice(args.indexOf("-O")+1, args.length)
        // val workerPort = args.last

        if (masterEndpoint.isEmpty) {
            System.out.println("Master endpoint is not ready")
        }
        else if (inputFileDirectory.isEmpty) {
            System.out.println("Input File Directory is not ready")
        }
        else if (outputFileDirectory.isEmpty) {
            System.out.println("Output File Directory is not ready")
        }
        else {
            val masterIPAddress = splitEndpoint(masterEndpoint.get)._1
            val masterPort = splitEndpoint(masterEndpoint.get)._2
            
            val client = Worker(masterIPAddress, masterPort)
            
            try {
                val succeedRegistration = client.register()
                if (!succeedRegistration){  
                    client.shutdown()

                }
                else {
                    client.externalSort(inputFileDirectory)
                    client.sample()
                    client.sendSample()
                    client.partitionByPivot()
                    client.shuffle()
                    client.mergeIntoSortedFile(outputFileDirectory.head)
                    client.mergeDone()
                }
                
            } finally {
                client.shutdown()
            }
        }
    }

    private val workerPort = 50060
}

class Worker private(
    private val channel: ManagedChannel, 
    private val blockingStub: SorterBlockingStub,
    private val newStub: SorterStub
) {
    type FileAddress = String
    type WorkerAddress = String

    private val workerDirectory = Util.makeSubdirectory(currentDirectory, "worker") + "/"

    private[this] val logger = Logger.getLogger(classOf[Worker].getName)
    // private val fileHandler = Util.createHandler(workerDirectory, "worker.log")
    private val myAddress: WorkerAddress = getIPaddress
    private val myEndpoint: String = myAddress + ":" + Worker.workerPort
    private var workerOrder:Int = -1
    private var workerPivots: List[cs332.protos.sorting.Worker]= null 
    
    private val sendSampleLatch: CountDownLatch = new CountDownLatch(1)
    private var shuffleCandidate: List[String] = null
    private var shufflePath: String = "shuffled"
    private val mergedOutput: String = "mergedOutput"
    private var partition: Map[String, List[File]] = null
    private var min = "" 
    private var max = ""
    private var beenFileServer: Boolean = false
    def shutdown(): Unit = {
        channel.shutdown.awaitTermination(600, TimeUnit.SECONDS)
    }

    def register(): Boolean = {
        
        // logger.addHandler(fileHandler)
        
        // val endpoint = myAddress 
        val request = RegisterRequest(address = myEndpoint)
        try {
            val response = blockingStub.registerWorker(request)
            var registerSuccess = false
            if (response.workerOrder != -1) {
                workerOrder = response.workerOrder
                logger.info("REGISTER : success as worker " + workerOrder)
                registerSuccess = true
            }
            else {
                logger.info("REGISTER : fail")
            }
            registerSuccess
        } 
        catch {
            case e: StatusRuntimeException =>
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
            false
        }
    }

    def getFilesFromDirectories(inputFileDirectory: Array[FileAddress]): List[File] = {
        var inputFiles = List[File]()
        for (address <- inputFileDirectory) {
            val d = new File(currentDirectory + address)
            
            if (d.exists && d.isDirectory) {
                val tmpFiles = d.listFiles.filter(_.isFile).toList
                inputFiles = inputFiles ::: tmpFiles
            } else {
                logger.info("Input File directories do not exist or are not directories") 
            }
        }
        assert(inputFiles != List[File]())
        inputFiles
    }
    
    def externalSort(inputFileDirectory: Array[FileAddress]): Unit = {
        val inputFiles = getFilesFromDirectories(inputFileDirectory)
        WorkerJob.externalMergeSort(workerDirectory, workerOrder.toString, inputFiles)
        logger.info("EXTERNAL MERGE SORT : done") 
    }

    def sample(): Unit = {
        WorkerJob.sampling(workerDirectory, workerOrder.toString)
        logger.info("SAMPLE : sampling is done") 
    }

    def sendSample(): Unit = {
        logger.info("Will try to send file "  + " ...")
        val streamObserver: StreamObserver[PivotRequest] = newStub.getWorkerPivots(
            new StreamObserver[PivotResponse] {
                override def onNext(response: PivotResponse): Unit =  {
                    workerPivots = response.workerPivots.toList
                    
                    // print("WORKERPIVOTS " + workerPivots )
                    // print("workerPivots " + workerOrder.toString + " : " + workerPivots(workerOrder) )
                    sendSampleLatch.countDown
                    assert(workerPivots != null)
                    logger.info(
                            "File upload status :: " + response.status
                    ) 
                }

                override def onError(throwaable: Throwable): Unit = {}

                override def onCompleted(): Unit = {
                }
            })  
        
        val path = Paths.get(workerDirectory + WorkerJob.sampleFile + "." + workerOrder)


        val metadata = Metadata(fileName = WorkerJob.sampleFile, fileType = workerOrder.toString)

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
        logger.info("SAMPLE : sending sample is done") 
        sendSampleLatch.await()
    }

    def partitionByPivot() = {
        partition = WorkerJob.partitionByPivot(workerPivots, workerOrder)
        // println("PARTITION LENGTH: " + partition.toList.length.toString + " PARTITION: " + partition )
    }

    def shuffle() = {
        val shuffledFiles:List[File] = partition(myEndpoint)
        shufflePath = shufflePath + workerOrder.toString
        val shuffledDir = Util.makeSubdirectory(workerDirectory, shufflePath ) + "/"
        val partitionDir = workerDirectory + "partition" + "/"
        shuffledFiles.foreach{file => Files.move(Paths.get(partitionDir + file.getName), Paths.get(shuffledDir + file.getName))}
        logger.info("SHUFFLE : shuffle is done")
    }   
 
    def mergeIntoSortedFile(outputFileDirectory: String) = {
        val shuffleFiles = readFilesfromDirectory(workerDirectory + shufflePath)
        
        assert(shuffleFiles != Nil)
        val finalOutputName = currentDirectory + outputFileDirectory + mergedOutput + workerOrder.toString
        WorkerJob.mergeIntoSortedFile(shuffleFiles, finalOutputName)
        val minMax = WorkerJob.extractMinMaxKey(new File(finalOutputName))
        min = minMax._1
        max = minMax._2
    }

    def mergeDone() = {
        assert(min != "" && max != "")
        val pivot = Option(Pivot(min = min, max = max))
        val request = DoneRequest(address = myEndpoint, pivot = pivot)
        try {
            val response = blockingStub.mergeDone(request)
            logger.info("TERMINATE")
            // fileHandler.close()
        } 
        catch {
            case e: StatusRuntimeException =>
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
        }
    }
} 