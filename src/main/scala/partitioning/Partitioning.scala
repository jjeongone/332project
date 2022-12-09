package partitioning

import java.io.File
import java.io.FileWriter
import scala.io.Source
import java.util.Scanner

object Partitioning {
  type Address = String
  type Pivot = (String, String)
  type Worker = (Address, Pivot)

  val outputPath = "C:\\Users\\user\\Desktop\\2022-2\\SD\\Project\\332project\\src\\main\\scala\\test\\partitioning\\output\\"
  val bytesPerLine = 100
  val maxPartitionSize = 1000

  // in the previous implementation, linesFromFile may exceed the memory space

  def partitionByPivot(file: File, workers: List[Worker]): Map[Address, List[File]] = {
    val maxNumLines = 10
    val scanner = new Scanner(file)

    def getLines(numLines: Int, lines: List[String]): List[String] = {
      if (numLines == 0 || !scanner.hasNextLine()) lines
      else getLines(numLines - 1, lines :+ scanner.nextLine())
    }

    def workerToFileName(workerAddress: Address, index: Int): String = outputPath + workerAddress + "_" + index.toString()
    def linesToFileContent(lines: List[String]): String = lines.foldRight("")((line, acc) => acc + line + "\n")
//    val linesFromFile = Source.fromFile(file).getLines()

    def unionMap(map0: Map[Address, List[File]], map1: Map[Address, List[File]]): Map[Address, List[File]] = {
      map0.foldLeft(map1)((acc, keyval) => acc.get(keyval._1) match {
        case None => acc ++ Map(keyval._1 -> keyval._2)
        case Some(v) => acc ++ Map(keyval._1 -> (keyval._2.toSet ++ v.toSet).toList)
      })
    }

    // Map[Address, (file index, number of lines)]
    def initializeNamingIndex: Map[Address, (Int, Int)] = workers.foldLeft(Map[Address, (Int, Int)]())((acc, worker) => acc ++ Map(worker._1 -> (0, 0)))

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
        val workerCorrespondingToLine = workers.find(worker => worker._2._1 <= line.slice(0, 10) && line.slice(0, 10) <= worker._2._2)
        workerCorrespondingToLine match {
          case None => acc
          case Some(worker) => {
            acc.get(worker) match {
              case None => {
                val (recentFileIndex, recentNumLines) = namingIndex(worker._1)
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
                val partitionFile = new File(workerToFileName(worker._1, linesFileInfo._1))
                val writerToPartitionFile = new FileWriter(partitionFile, true)
                writerToPartitionFile.write(linesToFileContent(linesData))
                writerToPartitionFile.close()
                acc :+ partitionFile
              })
              acc ++ Map(worker._1 -> (partitionFiles, listLines.head._2))
            }
          }
        }
      })
    }

    executePartitionByPivotAux(Map[Address, List[File]](), initializeNamingIndex)
  }
}
