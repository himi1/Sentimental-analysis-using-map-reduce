package ebird.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.tree.RandomForest

import ebird.data.DataConf
import ebird.data.DataParser
import ebird.data.Measures
import ebird.data.Bagging
import ebird.data.Features

// an object containing a method to train a learner
object Training {
    def train(sc: SparkContext, parallelism: Int, input: String, output: String) {

		// parse the input data
		val rawData = sc.textFile(input, parallelism)
			.mapPartitionsWithIndex({(idx, iter) => if (idx == 0) iter.drop(1) else iter})
			.map({line => line.split(",")})
		val categoryMap = DataParser.mapCategories(rawData)
		val data = DataParser.parse(rawData, categoryMap)

        // split the data into tuning and final training sets
        val split = data.randomSplit(Array(0.7,0.3))
        val trainData = Bagging.undersample(split(0))
        val testData = Bagging.undersample(split(1))
        trainData.persist(StorageLevel.MEMORY_AND_DISK)
        testData.persist(StorageLevel.MEMORY_AND_DISK)

        // replacements for missing data
		val replace = Map((5,0))
        sc.broadcast(replace)

		// create a model configuration
		val modelConf = new TreeModelConf
		modelConf.trainData = trainData
		modelConf.testData = testData
		modelConf.replace = Map()
		modelConf.categoryMap = categoryMap
        modelConf.continuousFeatures = Set(
            Features.TURDUS_MIGRATORIUS.toShort,
            Features.ARDEA_HERODIAS.toShort,
            Features.MONTH.toShort,
            Features.MELOSPIZA_MELODIA.toShort,
            Features.MOLOTHRUS_ATER.toShort,
            Features.CAUS_TEMP_MAX.toShort,
            Features.NLCD2006_FS_C82_7500_LPI.toShort,
            Features.ANAS_PLATYRHYNCHOS.toShort,
            Features.STURNUS_VULGARIS.toShort,
            Features.CAUS_TEMP_MIN06.toShort,
            Features.DIST_FROM_WET_VEG_FRESH.toShort,
            Features.TROGLODYTES_HIEMALIS.toShort,
            Features.EFFORT_HRS.toShort,
            Features.NLCD2011_FS_C95_75_LPI.toShort
        )

		// train the final model
		val model = modelConf.trainModel()  
        val accr = modelConf.runTrial(model)
        println(s"Final model accuracy: ${accr}")

        // save the model and configuration
		model.save(sc, output + "/forest")
		val featuresRDD = sc.parallelize(modelConf.features.toSeq)
		featuresRDD.saveAsObjectFile(output + "/features")
		val replaceRDD = sc.parallelize(replace.toSeq)
		replaceRDD.saveAsObjectFile(output + "/replace")
		val categoriesRDD = sc.parallelize(categoryMap.mapValues({ value => value.toSeq }).toSeq)
		categoriesRDD.saveAsObjectFile(output + "/categories")  
    }
}
