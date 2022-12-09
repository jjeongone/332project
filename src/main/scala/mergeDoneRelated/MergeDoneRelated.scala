package mergeDoneRelated

import partitioning.Partitioning._

object MergeDoneRelated {
  // Assume that always Pivot._1 <= Pivot._2
  def validationWorkerOrdering(workerToMinMaxKey: Map[Worker, Pivot], workers: List[Worker]): Boolean = {
    def validateMinMaxKeyDependency(prevMinMaxKeyOpt: Option[Pivot], currMinMaxKey: Pivot): Boolean = prevMinMaxKeyOpt match {
      case None => true
      case Some(prevMinMaxKey) => prevMinMaxKey._2 < currMinMaxKey._1
    }

    def validationWorkerOrderingAux(workerToMinMaxKeySortedListAux: List[(Worker, Pivot)], workersAux: List[Worker], prevMinMaxKeyOpt: Option[Pivot]): Boolean = {
      (workerToMinMaxKeySortedListAux, workersAux) match {
        case (Nil, Nil) => true
        case (_, Nil) => false
        case (Nil, _) => false
        case _ => {
          val (currWorker, currMinMaxKey) = workerToMinMaxKeySortedListAux.head
          if (validateMinMaxKeyDependency(prevMinMaxKeyOpt, currMinMaxKey) && (currWorker == workersAux.head) && (workersAux.head._2._1 <= currMinMaxKey._1 && currMinMaxKey._2 <= workersAux.head._2._2)) {
            validationWorkerOrderingAux(workerToMinMaxKeySortedListAux.tail, workersAux.tail, Some(currMinMaxKey))
          }
          else {
            false
          }
        }
      }
    }

    val workerToMinMaxKeyList = workerToMinMaxKey.toList
    val workerToMinMaxKeySortedList = workerToMinMaxKeyList.sortWith((elem0, elem1) => elem0._2._2 < elem1._2._2)

    validationWorkerOrderingAux(workerToMinMaxKeySortedList, workers, None)
  }
}
