package  cs332.master

import java.io.File
import scala.io.Source
import cs332.protos.sorting._
object MasterJob{
    type Key = String
    type Address = String

    def setPivot(file: File, workers: List[Worker]): List[Worker] = {
        val lines = Source.fromFile(file).getLines().toList
        var pivotingFactor = ((lines.size.toDouble)/(workers.size.toDouble)).ceil.toInt
        if (pivotingFactor >= lines.length) {
            pivotingFactor = lines.length -1
        }
        val rawPivots = lines.foldLeft((List[Key](), 0))((acc, line) => {
        if (acc._2 % pivotingFactor == 0) (acc._1 :+ line.slice(0, 10), acc._2 + 1)
        else (acc._1, acc._2 +1)
        })._1

        var pivots = if (lines.size % workers.size == 0) rawPivots else rawPivots :+ lines.last.slice(0, 10)
        pivots.updated(pivots.length-1,  lines.last)
        workers.foldLeft(List[Worker](), pivots)((acc, worker) => {
        (acc._1 :+ Worker(worker.address, Option(Pivot(acc._2.head, acc._2.tail.head))), acc._2.tail)
        })._1
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
    // print("workerToMinMaxKey: " + workerToMinMaxKey + "workers: "+ workers)
    // val workerToMinMaxKeyList = workerToMinMaxKey.toList
    val workerToMinMaxKeySortedList = workerToMinMaxKey.sortWith((elem0, elem1) => elem0._2._2 < elem1._2._2)
    validationWorkerOrderingAux(workerToMinMaxKeySortedList, workers, None)
  }
}