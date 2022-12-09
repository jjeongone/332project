package mergeDoneRelated

import partitioning.Partitioning._
import java.io.File
import java.util.Scanner

object MergeDoneRelated {
  // Assume that the file is sorted
  def extractMinMaxKey(file: File): Pivot = {
    val scanner = new Scanner(file)
    def extractMinMaxKeyAux(preKey: String): String = {
      if (scanner.hasNextLine()) extractMinMaxKeyAux(scanner.nextLine())
      else preKey
    }
    assert(scanner.hasNextLine())
    val minKey = scanner.nextLine()
    val maxKey = extractMinMaxKeyAux(minKey)
    (minKey, maxKey)
  }
}
