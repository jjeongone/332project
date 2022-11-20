import java.io.File
import sortingmerging.SortingMerging._
import partitioning.Partitioning._

object Main extends App {
  println("Hello, World!")
//  sorting example
  val sortingTestFilePath = "C:\\Users\\user\\Desktop\\2022-2\\SD\\Project\\332project\\src\\main\\scala\\test\\sortingmerging\\sorting\\input\\"
  val sortingTestFiles = List(new File(sortingTestFilePath + "test0"), new File(sortingTestFilePath + "test1"), new File(sortingTestFilePath + "test2"))
  println(externalMergeSort(sortingTestFiles))

//  merging example
  val mergingTestFilePath = "C:\\Users\\user\\Desktop\\2022-2\\SD\\Project\\332project\\src\\main\\scala\\test\\sortingmerging\\merging\\input\\"
  val mergingTestFiles = List(new File(mergingTestFilePath + "0"), new File(mergingTestFilePath + "1"), new File(mergingTestFilePath + "2"))
  println(mergeIntoSortedFile(mergingTestFiles))

//  partitioning example
  val workers = List(("hi", (" +cYsra!|\\", "98Z\"lo?kCN")), ("hello", ("<*dUGxo|~A", "fqPP|6z3'/")), ("bye", ("lR+Oub\\gZ/", "}=DW;~<}P+")))
  val partitioningTestFilePath = "C:\\Users\\user\\Desktop\\2022-2\\SD\\Project\\332project\\src\\main\\scala\\test\\partitioning\\input\\"
  val partitioningTestFile = new File(partitioningTestFilePath + "test")
  println(partitionByPivot(partitioningTestFile, workers))
}