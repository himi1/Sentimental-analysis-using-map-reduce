package ebird.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.RandomForestModel

import ebird.data.DataParser
import ebird.data.Features
import ebird.data.Formats

// an object containing a method to predict using a saved learner
object Prediction {
    def predict(sc: SparkContext, parallelism: Int, input: String, output: String, modelDir: String) {

        // load and broadcast the model
        val model = RandomForestModel.load(sc, modelDir + "/forest")
        val replaceRDD: RDD[(Short,Double)] = sc.objectFile(modelDir + "/replace")
        val replace = Map[Short,Double](replaceRDD.collect() : _*)
        val featuresRDD: RDD[Short] = sc.objectFile(modelDir + "/features")
        val features: Set[Short] = featuresRDD.collect().toSet
        sc.broadcast(model)
        sc.broadcast(features)
        sc.broadcast(replace)

        // load the unlabeled data
        val rawData = sc.textFile(input, parallelism)
            .mapPartitionsWithIndex({(i,iter) => if (i == 0) iter.drop(1) else iter})
            .map({line => line.split(",")})
        val sampleIDs = rawData.map({ values => values(Features.SAMPLING_EVENT_ID) })
        val categoryMap = DataParser.mapCategories(rawData)
        val data = DataParser.parse(rawData, categoryMap)

        // make the predictions
        val predictions = data.map({ values =>
            val vector = Formats.asUnlabeledVector(values, features, replace)
            val prediction = model.predict(vector)
            model.predict(vector).toByte
        })

        // write out the results with their sampling event ids
        val header = sc.parallelize(Array("SAMPLING_EVENT_ID,SAW_AGELAIUS_PHOENICEUS"), 1)
        header.union(sampleIDs.zip(predictions)
            .map({case (id,prediction) => s"${id},${prediction}"}).coalesce(1)
        ).saveAsTextFile(output)           
    }
}
