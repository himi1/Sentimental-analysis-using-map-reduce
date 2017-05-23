package ebird.data

// configuration of data types, parsing and filtering
object DataConf {
	val labelFeature = Features.AGELAIUS_PHOENICEUS

	// features to parse as sring ID containing number
	val idFeatures = Set(
		Features.SAMPLING_EVENT_ID,
		Features.LOC_ID,
		Features.OBSERVER_ID,
		Features.GROUP_ID,
		Features.SAMPLING_EVENT_ID1,
		Features.LOC_ID1,
		Features.SAMPLING_EVENT_ID2,
		Features.LOC_ID2
	)

	// features to parse as string category name
	val discreteFeatures = Set(
		Features.COUNTRY,
		Features.STATE_PROVINCE,
		Features.COUNTY,
		Features.COUNT_TYPE,
		Features.BCR,
		Features.BAILEY_ECOREGION,
		Features.OMERNIK_L3_ECOREGION,
		Features.SUBNATIONAL2_CODE
	)

	// continuous numerical features
	def continuousFeatures(feature: Integer): Boolean = {
		val excluded = DataConf.excludedFeatures(feature)
		val discrete = DataConf.discreteFeatures(feature)
		return !(excluded || discrete)
	}

	// features to exclude from the learner
	val excludedFeatures = Set(
		Features.SAMPLING_EVENT_ID,
		Features.LOC_ID,
        Features.COUNTRY,
        Features.STATE_PROVINCE,
		Features.COUNTY,
        Features.COUNT_TYPE,
		Features.OBSERVER_ID,
		Features.GROUP_ID,
		Features.PRIMARY_CHECKLIST_FLAG,	
		Features.SAMPLING_EVENT_ID1,
		Features.LOC_ID1,
		Features.SAMPLING_EVENT_ID2,
        Features.BCR,
        Features.BAILEY_ECOREGION,
        Features.OMERNIK_L3_ECOREGION,
		Features.LOC_ID2,
		Features.SUBNATIONAL2_CODE
	)

	val missingValue = "?"
	val nullValue = "X"
    val numFeatures = Features.NUM_FEATURES - excludedFeatures.size
}
