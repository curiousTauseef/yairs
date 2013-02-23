package yairs.util


/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/22/13
 * Time: 1:31 PM
 */
object BasicUtils {
    def memoryStatus(){
      val runtime = Runtime.getRuntime()

      val maxMemory = runtime.maxMemory()
      val allocatedMemory = runtime.totalMemory()
      val freeMemory = runtime.freeMemory()

      println("free memory: %s M".format( freeMemory / 1024 /1024) )
      println("allocated memory: %s M".format(allocatedMemory / 1024 /1024))
      println("max memory: %s M".format( maxMemory /1024 /1024) )
      println("total free memory: %s M".format ((freeMemory + (maxMemory - allocatedMemory)) / 1024 /1024))
    }
}
