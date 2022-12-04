package cs332.worker

import java.io.{BufferedReader,BufferedWriter,  File, FileReader, FileWriter}
import cs332.common.Util.{currentDirectory, makeSubdirectory}
import java.nio.file.{Files, Paths}
import scala.io.Source
import java.util.logging.Logger
import java.nio.charset.Charset
import java.util.Comparator
import scala.jdk.CollectionConverters._
import com.google.code.externalsorting.ExternalSort

object WorkerJob{
    
    type Address = String
    type Key = String
    type Pivot = (String, String)
    type Worker = (Address, Pivot)
    
    val samplingFactor = 1000
    val externalSortPath = "externalSorted/"
    val tmpPath4External = "tmp4External/"
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

    def mergeIntoSortedFile(files: List[File]): Unit = {
        val testOutput = new File(outputPath + externalSortFile)
        ExternalSort.mergeSortedFiles(files.asJava, testOutput, cmp)
        // System.out.println("testOutput: " +  testOutput)
    }

    def externalMergeSort(files: List[File]): Unit = {
        // System.out.println("files: " +  files)
        val sortedFiles = files.foldLeft(List[File]())((acc, file) => {
        val batchFileList = ExternalSort.sortInBatch(new BufferedReader(new FileReader(file)), 100, cmp,
            ExternalSort.DEFAULTMAXTEMPFILES, ExternalSort.estimateAvailableMemory(), Charset.defaultCharset(),
            new File(tmpfilesPath), false, 0, false, true)
        val outputFile = new File(tmpfilesPath + acc.length.toString())
        ExternalSort.mergeSortedFiles(batchFileList, outputFile, cmp)
        outputFile :: acc
        })
        mergeIntoSortedFile(sortedFiles)
    }
    
    def sampling(): Unit = {
        val lines = Source.fromFile(new File(currentDirectory + externalSortPath + externalSortFile)).getLines().toList
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

    def partition() = ???

    def merge() = ???
}