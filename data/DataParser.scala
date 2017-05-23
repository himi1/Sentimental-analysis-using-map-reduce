package ebird.data

import org.apache.spark.rdd.RDD

import scala.collection.mutable.MutableList
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set

// methods for parsing input CSV into a sparse RDD representation
object DataParser {
	private val idPattern = "\\d+".r

	// map string category names to doubles
	def mapCategories(data: RDD[Array[String]]): Map[Int,Map[String,Double]] = {

		// key string feature values by feature
		val categories = data.flatMap({values =>
			val valuesList = new MutableList[(Short,Seq[String])]
			for ((value,feature) <- values.view.zipWithIndex) {
				if (!DataConf.excludedFeatures(feature) &&
					DataConf.discreteFeatures(feature) &&
					value != DataConf.missingValue) {
						valuesList += ((feature.toShort,Seq(value)))
				}
			}
			valuesList.iterator
		})

		// find unique string values
		.reduceByKey({(values1,values2) =>
			val valuesSet = Set[String]()
			for (value <- values1) valuesSet += (value)
			for (value <- values2) valuesSet += (value)
			valuesSet.toSeq
		})

		// collect into local array
		.collect()

		// map strings to numerical values
		val featureMap = new HashMap[Int,HashMap[String,Double]]
		for (feature <- DataConf.discreteFeatures) {
			featureMap += ((feature, new HashMap[String,Double]))
		}
		for ((feature,names) <- categories) {
			val nameMap = featureMap(feature)
			for (name <- names) {
				nameMap += ((name,nameMap.size))
			}
		}
		val stringMap = new HashMap[Int,Map[String,Double]]
		for (feature <- DataConf.discreteFeatures) {
			stringMap += ((feature,featureMap(feature).toMap))
		}
		return stringMap.toMap
	}

	// parse input line into labeled point
	def parse(data: RDD[Array[String]], stringMap: Map[Int,Map[String,Double]]):
	RDD[Array[(Short,Double)]] = {
		return data.map({values =>
            val parsedValues = new ArrayBuffer[(Short,Double)]

			for ((value, feature) <- values.view.zipWithIndex) {
				
				// check for a missing value
				if (DataConf.excludedFeatures(feature) || value == DataConf.missingValue) {
                    parsedValues += ((feature.toShort, Double.NaN))
				}

				// check if id
				else if (DataConf.idFeatures(feature)) {
					val patternMatch = DataParser.idPattern.findFirstMatchIn(value)
                    parsedValues += ((feature.toShort, patternMatch.get.group(0).toDouble))
				}

				// check if categorical
				else if (DataConf.discreteFeatures(feature)) {
					parsedValues += ((value.toShort, stringMap(feature)(value)))
				}
			
				// else parse as double
				else if (!DataConf.excludedFeatures(feature) && value != DataConf.nullValue) {
                    val parsedValue = value.toDouble
                    if (parsedValue != 0) parsedValues += ((feature.toShort, parsedValue))
				}
			}
			parsedValues.toArray
		})
	}

    // get label value from sparse representation
    def getLabel(values: Array[(Short,Double)]): Double = {
        for ((feature,value) <- values) {
            if (feature == DataConf.labelFeature) return value
        }
        return 0
    }
}
