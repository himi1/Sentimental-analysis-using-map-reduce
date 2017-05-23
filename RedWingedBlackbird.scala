package ebird

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import ebird.model.TreeModelConf
import ebird.model.Training
import ebird.model.Prediction

// driver program for training or prediction
object RedWingedBlackbird {

	// argument indices
	private val MASTER_INDEX = 0
	private val PARALLELISM_INDEX = 1
    private val INPUT_INDEX = 2
    private val OUTPUT_INDEX = 3
    private val MODEL_INDEX = 4

	// common Spark context
	private var sc: SparkContext = null

	def main(args: Array[String]) {

		// parse arguments
		val master = args(MASTER_INDEX)
		val parallelism = args(PARALLELISM_INDEX).toInt
		val input = args(INPUT_INDEX)
		val output = args(OUTPUT_INDEX)
        val model = if (args.length == 5) args(MODEL_INDEX) else null

		// initialize the Spark context
		val conf = new SparkConf()
			.setAppName("RedWingedBlackbird")
			.setMaster(master)
			.set("spark.default.parallelism", parallelism.toString)
		sc = new SparkContext(conf)

        // run the appropriate task
        if (model == null) Training.train(sc, parallelism, input, output)
        else Prediction.predict(sc, parallelism, input, output, model)

		// clean up the spark context
		sc.stop()
	}

	implicit def toShort(x: Int): Short = x.toShort
}
