package ebird.data

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext

import scala.collection.mutable.MutableList
import scala.collection.mutable.HashMap
import scala.math.sqrt
import scala.math.abs

// A collection of functions for estimating data measures useful in selecting
// features and replacing missing values.
object Measures {

	// pre-partition data by feature for estimating measures
	def partitionByFeature(data: RDD[Array[(Short,Double)]], partitions: Int):
	RDD[(Short,(Double,Double))] = {
		return data.flatMap({ values =>
			val label = DataParser.getLabel(values)
			val list = new MutableList[(Short,(Double,Double))]
            var i = 0
            for (feature <- 0 until Features.NUM_FEATURES) {
                if (i < values.length && values(i)._1 == feature) {
                    val value = values(i)._2
				    if (!value.isNaN) {
					    list += ((feature.toShort,(value,label)))
				    }
                    i += 1       
                }
 
                else {
                    list += ((feature.toShort,(0,label)))
                }
            }
            list.iterator
		}).partitionBy(new HashPartitioner(partitions))
	}

	// estimate means for continous values
	def means(data: RDD[(Short,(Double,Double))]): Map[Short,Double] = {
		
		// filter excluded and non-continuous values
		val meansRDD = data.filter({case (feature,(_,_)) =>
			!DataConf.excludedFeatures(feature) && DataConf.continuousFeatures(feature)
		})

		// map initial count of one and value
		.mapValues({case (value,label) => (1L,value)})

		// sum counts and values
		.reduceByKey({ (x,y) =>
			val (count1, sum1) = x
			val (count2, sum2) = y
			(count1 + count2, sum1 + sum2)
		})

		// calculate mean for each feature
		.mapValues({case (count, sum) => sum / count})
		.coalesce(1)

		// create and return a map of means
		val meansMap = new HashMap[Short,Double]
		for (pair <- meansRDD.collect()) meansMap += (pair)
		return meansMap.toMap
	}

	// estimate modes for discrete values
	def modes(data: RDD[(Short,(Double,Double))]): Map[Short,Double] = {
		
		// filter excluded and non-discrete values
		val modesRDD = data.filter({case (feature,(_,_)) =>
			!DataConf.excludedFeatures(feature) && DataConf.discreteFeatures(feature)
		})

		// map initial count for each feature and value of one
		.map({case (feature,(value,label)) =>
			((feature,value),1L)
		})

		// sum counts for each feature and value
		.reduceByKey(_+_)

		// group counts by feature
		.map({case ((feature,value),count) =>
			(feature,(value,count))
		}).groupByKey()

		// find modes
		.mapValues({valueCounts =>
			var highCount: Long = 0
			var mode: Double = 0
			for ((value,count) <- valueCounts) {
				if (count > highCount) {
					highCount = count
					mode = value
				}
			}
			mode
		})

		// coalesce
		.coalesce(1)

		// create and return a map of means
        val modesMap = new HashMap[Short,Double]
        for ((feature,mode) <- modesRDD.collect()) modesMap += ((feature,mode))
        return modesMap.toMap
	}

	// estimate Pearson correlation coefficients for continuous values
	def pearson(data: RDD[(Short,(Double,Double))], means: Map[Short,Double]):
		Array[(Short,Double)] = {

		// filter out non-continuous values
		val correlations = data.filter({case (feature,(_,_)) =>
			DataConf.continuousFeatures(feature) &&
			!DataConf.excludedFeatures(feature) &&
			feature != DataConf.labelFeature
		})

		// initialize sums, square sums and coproduct sums
		.mapValues({case (value,label) =>
			val squareValue = value * value
			val squareLabel = label * label
			val coproduct = value * label
			(1L, squareValue, squareLabel, coproduct)
		})

		// sum counts, values, square values and coproducts
		.reduceByKey({(sums1,sums2) =>
			val (count1, valSqrSum1, lblSqrSum1, coprodSum1) = sums1
			val (count2, valSqrSum2, lblSqrSum2, coprodSum2) = sums2
			(
				count1 + count2,
				valSqrSum1 + valSqrSum2,
				lblSqrSum1 + lblSqrSum2,
				coprodSum1 + coprodSum2
			)
		})

		// calculate the correlation
		.map({case (feature,sums) =>
			val (count, valSqrSum, lblSqrSum, coprodSum) = sums
			val valMean = means(feature)
			val lblMean = means(DataConf.labelFeature.toShort)
			val valStd = sqrt(valSqrSum / count - valMean * valMean)
			val lblStd = sqrt(lblSqrSum / count - lblMean * lblMean)
			val covar = coprodSum / count - valMean * lblMean
			val correlation: Double = covar / (valStd * lblStd)
			if (correlation.isNaN || correlation.isInfinity) (feature,0D)
			else (feature,abs(correlation))
		})
		
		// coalesce and sort 
		.coalesce(1).sortBy({case (_,score) => -score})

		// return as array
		return correlations.collect()
	}

	// estimate chi-squared score for discrete features
	def chiSquared(data: RDD[(Short,(Double,Double))]): Array[(Short,Double)] = {

		// filter out non-discrete values
		val scores = data.filter({case (feature,(_,_)) =>
			DataConf.discreteFeatures(feature) &&
			!DataConf.excludedFeatures(feature) &&
			feature != DataConf.labelFeature
		})

		// map counts for each feature and value
		.map({case (feature,(value,label)) =>
			val key = (feature, value.toInt)
			if (label == 0) (key,(1L,0L))
			else (key,(0L,1L))
		})

		// sum counts for each feature value
		.reduceByKey({(counts1,counts2) =>
			val (numFalse1,numTrue1) = counts1
			val (numFalse2,numTrue2) = counts2
			(numFalse1 + numFalse2, numTrue1+ numTrue2)
		})

		//group reduced counts by feature
		.map({case ((feature,value),(numFalse,numTrue)) =>
			(feature,(value,numFalse,numTrue))
		})
		.groupByKey()

		// calculate chi squared score for each feature
		.mapValues({ valueCounts =>
			var totalFalse: Double = 0
			var totalTrue: Double = 0
			var score: Double = 0

			// count total false and true
			for ((_,numFalse,numTrue) <- valueCounts) {
				totalFalse += numFalse
				totalTrue += numTrue
			}
			val total: Double = totalFalse + totalTrue

			// calculate score
			for ((value,numFalse,numTrue) <- valueCounts) {
				val valueProb: Double = (numFalse + numTrue) / total
				val expectedFalse: Double = valueProb * totalFalse
				val expectedTrue: Double = valueProb * totalTrue
				val falseDiff = numFalse - expectedFalse
				val trueDiff = numTrue - expectedTrue
				score += falseDiff * falseDiff / expectedFalse
				score += trueDiff * trueDiff / expectedTrue
			}
			score
		})

		// coalesce and sort
		.coalesce(1).sortBy({case (_,score) => -score})

		// return as array
		scores.collect()
	}
}
