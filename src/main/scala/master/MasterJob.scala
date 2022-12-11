package  cs332.master

import java.io.File
import java.util.Scanner
import scala.io.Source
import scala.annotation.tailrec
import cs332.protos.sorting._

object MasterJob{
    type Key = String
    type Address = String

    def setPivot(file: File, workers: List[Worker]): List[Worker] = {
        val scannerForSize = new Scanner(file)
        val scanner = new Scanner(file)
        
        @tailrec
        def sizeOfScanner(lineIndex: Int, prevStringOpt: Option[String]): Int = {
          if (scannerForSize.hasNextLine()) {
            val line = scannerForSize.nextLine()
            prevStringOpt match {
              case None => sizeOfScanner(lineIndex + 1, Some(line))
              case Some(prevString) => {
                if (prevString == line) sizeOfScanner(lineIndex, Some(line))
                else sizeOfScanner(lineIndex + 1, Some(line))
              }
            }
          } else {
            lineIndex
          }
        }

        val sizeOfFile = sizeOfScanner(0, None)
        var pivotingFactor = ((sizeOfFile.toDouble)/(workers.size.toDouble)).ceil.toInt
        if (pivotingFactor >= sizeOfFile) {
            pivotingFactor = sizeOfFile -1
        }

        @tailrec
        def setPivotAux(pivots: List[Key], lineIndex: Int, lastLineOpt: Option[String]): (List[Key], String) = {
          if (scanner.hasNextLine()) {
            val line = scanner.nextLine()
            if (!(lineIndex == 1) && (lineIndex % pivotingFactor == 0 || lineIndex % pivotingFactor == 1)) {
              setPivotAux(pivots :+ line.slice(0, 10), lineIndex + 1, Some(line))
            } else {
              setPivotAux(pivots, lineIndex + 1, Some(line))
            }
          } else {
            (pivots, lastLineOpt.get)
          }
        }

        val (rawPivots, lastLine) = setPivotAux(List[Key](), 0, None)

        var pivots = if (rawPivots.size % 2 == 0) rawPivots else rawPivots :+ lastLine.slice(0, 10)
        pivots.updated(pivots.length-1, lastLine)

        // println("LENGTH: " + pivots.length + "PIVOTING FACTOR: " + pivotingFactor.toString + "PIVOTS: " + pivots )
        assert(pivots.length == 2*workers.length)

        val result = workers.foldLeft(List[Worker](), pivots)((acc, worker) => {
        (acc._1 :+ Worker(worker.address, Option(Pivot(acc._2.head, acc._2.tail.head))), acc._2.tail.tail)
        })
        
        // println("PIVOT SETTT: " + result._1)

        result._1
    }

  def validationWorkerOrdering(workerToMinMaxKey: List[(Address, (String, String))], workers: List[Worker]): Boolean = {
    def validateMinMaxKeyDependency(prevMinMaxKeyOpt: Option[(String, String)], currMinMaxKey: (String, String)): Boolean = prevMinMaxKeyOpt match {
      case None => true
      case Some(prevMinMaxKey) => prevMinMaxKey._2 < currMinMaxKey._1
    }

    def validationWorkerOrderingAux(workerToMinMaxKeySortedListAux: List[(Address, (String, String))], workersAux: List[Worker
    
    ], prevMinMaxKeyOpt: Option[(String, String)]): Boolean = {
      (workerToMinMaxKeySortedListAux, workersAux) match {
        case (Nil, Nil) => true
        case (_, Nil) => false
        case (Nil, _) => false
        case _ => {
          val (currWorker, currMinMaxKey) = workerToMinMaxKeySortedListAux.head
          
          if (validateMinMaxKeyDependency(prevMinMaxKeyOpt, currMinMaxKey) && (currWorker == workersAux.head.address) &&
           (workersAux.head.pivot.get.min <= currMinMaxKey._1 && currMinMaxKey._2 <= workersAux.head.pivot.get.max)) {
            validationWorkerOrderingAux(workerToMinMaxKeySortedListAux.tail, workersAux.tail, Some(currMinMaxKey))
          }
          else {
            false
          }
        }
      }
    }
    print("workerToMinMaxKey: " + workerToMinMaxKey + "workers: "+ workers)
    // val workerToMinMaxKeyList = workerToMinMaxKey.toList
    val workerToMinMaxKeySortedList = workerToMinMaxKey.sortWith((elem0, elem1) => elem0._2._2 < elem1._2._2)
    validationWorkerOrderingAux(workerToMinMaxKeySortedList, workers, None)
  }
}