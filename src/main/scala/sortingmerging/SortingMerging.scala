package sortingmerging

import java.io.{BufferedReader, File, FileReader}
import java.nio.charset.Charset
import java.util.Comparator
import scala.jdk.CollectionConverters._
import com.google.code.externalsorting.ExternalSort

object SortingMerging {
  val outputPath = "C:\\Users\\user\\Desktop\\2022-2\\SD\\Project\\332project\\src\\main\\scala\\test\\sortingmerging\\output\\"
  val tmpfilesPath = "C:\\Users\\user\\Desktop\\2022-2\\SD\\Project\\332project\\src\\main\\scala\\test\\sortingmerging\\sorting\\tmpfiles\\"

  def cmp: Comparator[String] = new Comparator[String] {
    override def compare(s1:String, s2:String): Int = {
      if(s1.slice(0, 10) > s2.slice(0, 10)) 1
      else if(s1.slice(0, 10) < s2.slice(0, 10)) -1
      else 0
    }
  }

  def externalMergeSort(files: List[File]): File = {
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

  def mergeIntoSortedFile(files: List[File]): File = {
    val testOutput = new File(outputPath + "testOutput")
    ExternalSort.mergeSortedFiles(files.asJava, testOutput, cmp)
    testOutput
  }
}
