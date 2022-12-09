package  cs332.master

import java.io.File
import scala.io.Source
import cs332.protos.sorting._
object MasterJob{
    type Key = String

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

    // redistribute partition file info for each workers
    def partitionInfo() = ???
}