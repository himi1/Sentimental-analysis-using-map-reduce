package ebird.data

import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.SortedSet

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

// methods for transforming data into formats used by MLlib
object Formats {

    // transform raw data into unlabeled vector
    def asUnlabeledVector(values: Array[(Short,Double)], features: Set[Short],
        replace: Map[Short,Double]): Vector = {
        val vector = new ArrayBuffer[(Int,Double)]
        val featuresMap = Map(features.toSeq.sorted.zipWithIndex:_*)
        for ((feature,value) <- values) {
            if (features(feature) && feature != DataConf.labelFeature) {
                val i = featuresMap(feature)
                if (value.isNaN) vector += ((i, -1))
                else vector += ((i,value))
            }
        }
        return Vectors.sparse(features.size, vector.toArray)
    }

	// transform raw data formatted as RDD into RDD of labeled points
	def asLabeledPoints(data: RDD[Array[(Short,Double)]], features: Set[Short],
		replace: Map[Short,Double]): RDD[LabeledPoint] = {
		
		return data.map({ values =>
			Formats.mapValuesToLabeledPoints(values, features, replace)
		})
	}

	// transform iterator of raw data into iterator of labeled points
	def asLabeledPoints(data: Iterator[Array[(Short,Double)]], features: Set[Short],
		replace: Map[Short,Double]): Iterator[LabeledPoint] = {
		
		new Iterator[LabeledPoint] {
			def hasNext = data.hasNext
			def next = {
				val values = data.next
				Formats.mapValuesToLabeledPoints(values, features, replace)				
			}
		}		
	}

	// map raw values into labeled points
	def mapValuesToLabeledPoints(values: Array[(Short,Double)], features: Set[Short],
		replace: Map[Short,Double]): LabeledPoint = {
		
		val label = if (DataParser.getLabel(values) > 0) 1 else 0
        val vector = new ArrayBuffer[(Int,Double)]
        val featuresMap = Map(features.toSeq.sorted.zipWithIndex:_*)
        for ((feature,value) <- values) {
            if (features(feature) && feature != DataConf.labelFeature) {
                val i = featuresMap(feature)
                if (value.isNaN) vector += ((i, -1))
                else vector += ((i,value))
            }
        }
        return LabeledPoint(label, Vectors.sparse(features.size, vector.toArray))
	}
}
