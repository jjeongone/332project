package partitioning

import java.io.File
import java.io.FileWriter
import scala.io.Source

object Partitioning {
  type Address = String
  type Pivot = (String, String)
  type Worker = (Address, Pivot)

  val outputPath = "C:\\Users\\user\\Desktop\\2022-2\\SD\\Project\\332project\\src\\main\\scala\\test\\partitioning\\output\\"
  val bytesPerLine = 100
  val maxPartitionSize = 1000

  def partitionByPivot(file: File, workers: List[Worker]): Map[Address, List[File]] = {
    def workerToFileName(workerAddress: Address, index: Int): String = outputPath + workerAddress + "_" + index.toString()
    def linesToFileContent(lines: List[String]): String = lines.foldRight("")((line, acc) => if (acc.nonEmpty) acc + "\n" + line else line)

    val linesFromFile = Source.fromFile(file).getLines()

    val mapWorkerToLines = linesFromFile.foldLeft(Map[Worker, List[List[String]]]())((acc, line) => {
      val workerCorrespondingToLine = workers.find(worker => worker._2._1 <= line.slice(0, 10) && line.slice(0, 10) <= worker._2._2)
      workerCorrespondingToLine match {
        case None => acc
        case Some(worker) => {
          acc.get(worker) match {
            case None => acc ++ Map(worker -> List(List(line)))
            case Some(lines) => {
              if ((lines.head.length + 1) * bytesPerLine <= maxPartitionSize) acc ++ Map(worker -> ((line :: lines.head) :: lines.tail))
              else acc ++ Map(worker -> (List(line) :: lines))
            }
          }
        }
      }
    })

    workers.foldLeft(Map[Address, List[File]]())((acc, worker) => {
      mapWorkerToLines.get(worker) match {
        case None => acc
        case Some(listLines) => {
          val partitionFiles = listLines.foldRight(List[File]())((lines, acc) => {
            val partitionFile = new File(workerToFileName(worker._1, acc.length))
            val writerToPartitionFile = new FileWriter(partitionFile)
            writerToPartitionFile.write(linesToFileContent(lines))
            writerToPartitionFile.close()
            acc :+ partitionFile
          })
          acc ++ Map(worker._1 -> partitionFiles)
        }
      }
    })
  }
}
