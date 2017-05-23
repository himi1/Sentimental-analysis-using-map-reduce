package ebird.model

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.math.pow

import ebird.data.DataConf
import ebird.data.Formats

// class for holding a learner configuration and methods for tuning and testing
class TreeModelConf {
	var numTrees = 50
	var numClasses = 2
	var strategy = "auto"
	var impurity = "gini"
	var maxDepth = 10
	var maxBins = 100
	var continuousFeatures: Set[Short] = Set.empty
	var categoricalFeatures: Set[Short] = Set.empty
	var replace: Map[Short,Double] = null
	var categoryMap: Map[Int,Map[String,Double]] = null
	var trainData: RDD[Array[(Short,Double)]] = null
	var testData: RDD[Array[(Short,Double)]] = null
	val seed = System.currentTimeMillis().toInt
	var alpha = 0.2
	var epsilon = 0.0001
	def features = continuousFeatures ++ categoricalFeatures

    // select continuous features with filter method
    def filterContinuousFeatures(scores: Array[(Short,Double)], threshold: Double) {
        val candidates = scores.filter({case (_,score) => score > threshold})
            .map({case (feature,_) => feature})
        println(s"\n${candidates}\n")
        var bestAccr: Double = 0.5
        var bestNumFeatures = 0
        for (numFeatures <- 1 until candidates.length) {
            continuousFeatures = candidates.slice(0,numFeatures).toSet
            var accr = runTrial()
            println(s"numFeatures=${numFeatures},accr=${accr}")
            if (accr > bestAccr) {
                bestAccr = accr
                bestNumFeatures = numFeatures
            }
        }
        continuousFeatures = candidates.slice(0,bestNumFeatures).toSet
        println(s"bestNumFeatures=${bestNumFeatures},accr=${bestAccr}")
    }
    
    // select continuous features with modified wrapper method
    def selectContinuousFeatures(scores: Array[(Short,Double)], threshold: Double) {

        // create list of candidate features
        val candidates = ArrayBuffer(
            scores.filter({case (_,score) => score > threshold})
            .map({case (feature,_) => feature}) : _*
        )
        println("\n" + candidates + "\n")

        // greedily add next best among top candidates
        var bestAccr: Double = 0.5
        val bestFeatures = scala.collection.mutable.SortedSet[Short]()
        var done = false
        while (candidates.size > 0 && !done) {
            var bestCandidate: Short = -1
            for (candidate <- candidates) {
                continuousFeatures = (bestFeatures + candidate).toSet
                val accr = runTrial()
                println("candidate=" + candidate + ",accr=" + accr)
                if ((accr - bestAccr) > epsilon) {
                    bestAccr = accr
                    bestCandidate = candidate
                }
            }

            if (bestCandidate == -1) {
                done = true
                println()
            } else {
                candidates -= bestCandidate
                bestFeatures += bestCandidate
                println("feature=" + bestCandidate + ",accr=" + bestAccr + "\n")
            }        
        }
        continuousFeatures = bestFeatures.toSet
    }


	// tune configuration of tree
	def tuneTree(kind: String, minParam: Int, maxParam: Int): Double = {
		var bestAccr: Double = 0
		var bestParam = minParam

        for (param <- minParam to maxParam) {
			// update the appropriate param
			if (kind == "numTrees") numTrees = param
			else if (kind == "maxDepth") maxDepth = param
			else if (kind == "maxBins") maxBins = param
			else throw new Exception("bad param type")

			// run trial and check if accuracy improved
			val accr = runTrial()
			if (accr > bestAccr) {
				bestAccr = accr
				bestParam = param
			}

			println(s"${kind}=${param},accr=${accr}")
		}

		// update the parameter  and return accuracies
		if (kind == "numTrees") numTrees = bestParam
		else if (kind == "maxDepth") maxDepth = bestParam
		else if (kind == "maxBins") maxBins = bestParam
		println("best=" + bestParam + ",accr=" + bestAccr + "\n")
		return bestAccr
	}

    // access accuracy of model
    def runTrial(model: RandomForestModel): Double = {
        val testPoints = Formats.asLabeledPoints(testData, features, replace)
        val predictions = testPoints.map({ point => 
            val prediction = model.predict(point.features) 
            (point.label, prediction) 
        }) 
 
        val accr = predictions.filter({case (label,prediction) => 
            label == prediction 
        }).count.toDouble / testPoints.count 
 
        return accr 
    }

	// access accuracy of current configuration
	def runTrial(): Double = {
        return runTrial(trainModel())
    }

    // train a model with the current configuration
    def trainModel(): RandomForestModel = {

        // format data as labeled points
        val trainPoints = Formats.asLabeledPoints(trainData, features, replace)
		
        // build information about categorical features
        val categories = collection.mutable.Map[Int, Int]()
        var index = 0
        for (feature <- features.toSeq.sorted) {
            if (DataConf.discreteFeatures(feature)) {
                categories += ((index, categoryMap(feature).size))
            }
            index += 1
        }

		// train model with configuration
		return RandomForest.trainClassifier(
			trainPoints,
			numClasses,
			categories.toMap,
			numTrees,
			strategy,
			impurity,
			maxDepth,
			maxBins,
			seed
		)
    }

	// print accuracy stats with current features
	def printAccr(accr: Double) {
		println(
			s"numContinuousFeatures=${continuousFeatures.size} " +
			s"numCategoricalFeatures=${categoricalFeatures.size} " +
			s"numTrees=${numTrees} " +
			s"maxDepth=${maxDepth} " +
			s"accr=${accr}"
		)
	}
}
