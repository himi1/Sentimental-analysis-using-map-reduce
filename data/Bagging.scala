package ebird.data

import org.apache.spark.rdd.RDD

import scala.math.pow

// method for balancing imbalanced data
object Bagging {

    // balance the data by undersampling the majority label
	def undersample(data: RDD[Array[(Short,Double)]]): (RDD[Array[(Short,Double)]]) = {
		val falseData = data.filter({ values => DataParser.getLabel(values) == 0})
		val trueData = data.filter({ values => DataParser.getLabel(values) > 0})
		val falseCount = falseData.count().toDouble
		val trueCount = trueData.count().toDouble
		val ratio = trueCount / falseCount
		if (ratio < 1) {
			return trueData.union(falseData.sample(false, ratio))
		} else {
			return falseData.union(trueData.sample(false, pow(ratio, -1)))
		}
	}

    // balance the data by oversampling the minority label
	def oversample(data: RDD[Array[(Short,Double)]]): (RDD[Array[(Short,Double)]]) = {
		val falseData = data.filter({ values => DataParser.getLabel(values) == 0})
		val trueData = data.filter({ values => DataParser.getLabel(values) > 0})
		val falseCount = falseData.count().toDouble
		val trueCount = trueData.count().toDouble
		val ratio = falseCount / trueCount
		if (ratio > 1) {
			return falseData.union(trueData.sample(true, ratio))
		} else {
			return trueData.union(falseData.sample(true, pow(ratio, -1)))
		}
	}
}
