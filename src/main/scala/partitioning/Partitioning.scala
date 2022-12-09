package partitioning

object Partitioning {
  type Address = String
  type Pivot = (String, String)
  type Worker = (Address, Pivot)

  val workers = List(("hello", ("a", "b")), ("hi", ("c", "d")), ("bye", ("e", "f")))
  // workers: List[Worker] declared on Master-side
  def totalPartitionInfo(totalPartition: Map[Address, Map[Address, Int]]): Map[Address, List[Address]]= {
    def partitionInfoOf(worker: Worker): List[Address] = {
      workers.foldLeft(List[Address]())((acc, otherWorker) => { // otherWorker != worker
        if (worker._1 == otherWorker._1) {
          acc
        } else {
          totalPartition.get(otherWorker._1) match {
            case None => acc
            case Some(otherWorkerPartition) => otherWorkerPartition.get(worker._1) match {
              case None => acc
              case Some(partitionNum) => {
                if (partitionNum == 0) acc
                else otherWorker._1 :: acc
              }
            }
          }
        }
      })
    }

    workers.foldLeft(Map[Address, List[Address]]())((acc, worker) => acc ++ Map(worker._1 -> partitionInfoOf(worker)))
  }
}
