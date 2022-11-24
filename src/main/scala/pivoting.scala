import java.io.File
import scala.io.Source

object Pivoting {
  type Address = String
  type Key = String
  type Pivot = (String, String)
  type Worker = (Address, Pivot)

  val filePath = "src/main/scala/partition0"

  def setPivot(file: File, workers: List[Worker]): List[Worker] = {
    val lines = Source.fromFile(file).getLines().toList
    val pivotingFactor = ((lines.size.toDouble)/(workers.size.toDouble)).ceil.toInt
    val _ = print(pivotingFactor)
    val rawPivots = lines.foldLeft((List[Key](), 0))((acc, line) => {
      if (acc._2 % pivotingFactor == 0) (acc._1 :+ line.slice(0, 10), acc._2 + 1)
      else (acc._1, acc._2 +1)
    })._1
    val pivots = if (lines.size % workers.size == 0) rawPivots else rawPivots :+ lines.last.slice(0, 10)

    workers.foldLeft(List[Worker](), pivots)((acc, worker) => {
      (acc._1 :+ (worker._1, (acc._2.head, acc._2.tail.head)), acc._2.tail)
    })._1
  }
}