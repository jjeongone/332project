import java.io.File
import scala.io.Source

object Sampling{
  type Address = String
  type Key = String
  type Pivot = (String, String)
  type Worker = (Address, Pivot)

  val filePath = "src/main/scala/partition0"
  val samplingFactor = 2

  def sampling(file: File): List[Key] = {
    val lines = Source.fromFile(file).getLines().toList
    lines.foldLeft((List[Key](), 0))((acc, line) => {
      if (acc._2 % samplingFactor == 0) (acc._1 :+ line.slice(0, 10), acc._2 + 1)
      else (acc._1, acc._2 + 1)
    })._1
  }
}