import mergeDoneRelated.MergeDoneRelated._
import partitioning.Partitioning._

object Main extends App {
  println("Hello, World!")

  val address0 = "hello"
  val address1 = "hi"
  val address2 = "bye"

  val worker0 = (address0, ("a", "b"))
  val worker1 = (address1, ("c", "d"))
  val worker2 = (address2, ("e", "f"))

  val partition0 = Map(address0 -> 11, address2 -> 3)
  val partition1 = Map(address0 -> 0, address1 -> 1, address2 -> 4)
  val partition2 = Map(address0 -> 2, address2 -> 31)

  val totalPartition = Map(address0 -> partition0, address1 -> partition1, address2 -> partition2)
  println(totalPartitionInfo(totalPartition))
}