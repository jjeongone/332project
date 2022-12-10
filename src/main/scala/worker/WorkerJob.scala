package cs332.worker

import java.io.{BufferedReader,BufferedWriter,  File, FileReader, FileWriter}
import cs332.common.Util.{currentDirectory, makeSubdirectory}
import java.nio.file.{Files, Paths}
import cs332.protos.sorting._
import scala.io.Source
import java.util.logging.Logger
import java.nio.charset.Charset
import java.util.{Comparator, Scanner}
import scala.jdk.CollectionConverters._
import com.google.code.externalsorting.ExternalSort

object WorkerJob{
    type Key = String
  type Address = String
    val bytesPerLine = 100
  val maxPartitionSize = 104857600
    val samplingFactor = 100
    val externalSortPath = "externalSorted"
    val tmpPath4External = "tmp4External"
    val externalSortFile = "testOutput"
    val sampleFile = "sample"

    private[this] val logger = Logger.getLogger(classOf[Worker].getName)

    val outputPath = makeSubdirectory(currentDirectory, externalSortPath)
    val tmpfilesPath = makeSubdirectory(currentDirectory, tmpPath4External)

    def cmp: Comparator[String] = new Comparator[String] {
        override def compare(s1:String, s2:String): Int = {
            if(s1.slice(0, 10) > s2.slice(0, 10)) 1
            else if(s1.slice(0, 10) < s2.slice(0, 10)) -1
            else 0
        }
    }

    def mergeIntoSortedFile(files: List[File], path: String): Unit = {
        val testOutput = new File(path)
        ExternalSort.mergeSortedFiles(files.asJava, testOutput, cmp)
    }

    def externalMergeSort(files: List[File]): Unit = {
        val sortedFiles = files.foldLeft(List[File]())((acc, file) => {
        val batchFileList = ExternalSort.sortInBatch(new BufferedReader(new FileReader(file)), 100, cmp,
            ExternalSort.DEFAULTMAXTEMPFILES, ExternalSort.estimateAvailableMemory(), Charset.defaultCharset(),
            new File(tmpfilesPath), false, 0, false, true)
        val outputFile = new File(tmpfilesPath + acc.length.toString())
        ExternalSort.mergeSortedFiles(batchFileList, outputFile, cmp)
        outputFile :: acc
        })
        mergeIntoSortedFile(sortedFiles, outputPath + "/" + externalSortFile)
    }
    
    def sampling(): Unit = {
        val lines = Source.fromFile(new File(currentDirectory + externalSortPath +"/" +  externalSortFile)).getLines().toList
        val sampleFilename = new File(currentDirectory + sampleFile)
        val bw = new BufferedWriter(new FileWriter(sampleFilename))
        val samples = lines.foldLeft((List[Key](), 0))((acc, line) => {
        if (acc._2 % samplingFactor == 0) (acc._1 :+ line.slice(0, 10), acc._2 + 1)
        else (acc._1, acc._2 + 1)
        })._1
        for (sample <- samples) {
            bw.write(sample + "\n")
        }
        bw.close()
    }

    def partitionByPivot(workers: List[Worker], workerOrder: Int): Map[Address, List[File]] = {
        val file = new File(outputPath + "/" + externalSortFile)
        val maxNumLines = 10
        val scanner = new Scanner(file)
        def getLines(numLines: Int, lines: List[String]): List[String] = {
        if (numLines == 0 || !scanner.hasNextLine()) lines
        else getLines(numLines - 1, lines :+ scanner.nextLine())
        }

        def workerToFileName(workerAddress: Address, index: Int): String = outputPath + "/" +  "partition" + workerOrder.toString + "." + index.toString()
        def linesToFileContent(lines: List[String]): String = lines.foldRight("")((line, acc) => acc + line + "\n")
    //    val linesFromFile = Source.fromFile(file).getLines()

        def unionMap(map0: Map[Address, List[File]], map1: Map[Address, List[File]]): Map[Address, List[File]] = {
        map0.foldLeft(map1)((acc, keyval) => acc.get(keyval._1) match {
            case None => acc
            case Some(v) => acc ++ Map(keyval._1 -> (keyval._2 ::: v))
        })
        }

        // Map[Address, (file index, number of lines)]
        def initializeNamingIndex: Map[Address, (Int, Int)] = workers.foldLeft(Map[Address, (Int, Int)]())((acc, worker) => acc ++ Map(worker.address -> (0, 0)))

        def executePartitionByPivotAux(acc: Map[Address, List[File]], namingIndex: Map[Address, (Int, Int)]): Map[Address, List[File]] = {
        if (scanner.hasNextLine()) {
            val linesFromFile = getLines(maxNumLines, List[String]())
            val result = partitionByPivotAux(linesFromFile, namingIndex)
            val resultFiles = result.map((keyval: (Address, (List[File], (Int, Int)))) => (keyval._1, keyval._2._1))
            val resultNamingIndex = result.map((keyval: (Address, (List[File], (Int, Int)))) => (keyval._1, keyval._2._2))
            executePartitionByPivotAux(unionMap(acc, resultFiles), namingIndex ++ resultNamingIndex)
        }
        else {
            acc
        }
        }

        def partitionByPivotAux(linesFromFile: List[String], namingIndex: Map[Address, (Int, Int)]): Map[Address, (List[File], (Int, Int))] = {
        val mapWorkerToLines = linesFromFile.foldLeft(Map[Worker, List[(List[String], (Int, Int))]]())((acc, line) => {
            val workerCorrespondingToLine = workers.find(worker => worker.pivot.get.min <= line.slice(0, 10) && line.slice(0, 10) <= worker.pivot.get.max)
            workerCorrespondingToLine match {
            case None => acc
            case Some(worker) => {
                acc.get(worker) match {
                case None => {
                    val (recentFileIndex, recentNumLines) = namingIndex(worker.address)
                    if ((recentNumLines + 1) * bytesPerLine <= maxPartitionSize) acc ++ Map(worker -> List((List(line), (recentFileIndex, recentNumLines + 1))))
                    else acc ++ Map(worker -> List((List(line), (recentFileIndex + 1, 1))))
                }
                case Some(lines) => {
                    if ((lines.head._2._2 + 1) * bytesPerLine <= maxPartitionSize) acc ++ Map(worker -> ((line :: lines.head._1, (lines.head._2._1, lines.head._2._2 + 1)) :: lines.tail))
                    else acc ++ Map(worker -> ((List(line), (lines.head._2._1 + 1, 1)) :: lines))
                }
                }
            }
            }
        })

        workers.foldLeft(Map[Address, (List[File], (Int, Int))]())((acc, worker) => {
            mapWorkerToLines.get(worker) match {
            case None => acc
            case Some(listLines) => listLines match {
                case Nil => acc
                case _ => {
                val partitionFiles = listLines.foldRight(List[File]())((lines, acc) => {
                    val linesData = lines._1
                    val linesFileInfo = lines._2
                    val partitionFile = new File(workerToFileName(worker.address, linesFileInfo._1))
                    val writerToPartitionFile = new FileWriter(partitionFile, true)
                    writerToPartitionFile.write(linesToFileContent(linesData))
                    writerToPartitionFile.close()
                    acc :+ partitionFile
                })
                acc ++ Map(worker.address -> (partitionFiles, listLines.head._2))
                }
            }
            }
        })
        }

        executePartitionByPivotAux(Map[Address, List[File]](), initializeNamingIndex)
    }
    

    def merge() = ???
}