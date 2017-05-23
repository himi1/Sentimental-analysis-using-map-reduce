MAIN = RedWingedBlackbird
JAR = ${MAIN}.jar

# local run settings
MASTER = local
PARALLELISM = 4
LABELED = labeled_sample.csv.bz2
UNLABELED = unlabeled_sample.csv.bz2
TRAIN_OUTPUT = train-output
PREDICT_OUTPUT = predict-output
OUTPUT = output

# remote run settings
BUCKET = s3://heisler.c.cs6240.project
REGION = us-east-1
SUBNET = subnet-4edc2e63
INSTANCE_TYPE = m4.large
AWS_MASTER = yarn-cluster
AWS_PARALLELISM = 60
AWS_LABELED = ${BUCKET}/labeled.csv.bz2
AWS_UNLABELED = ${BUCKET}/unlabeled.csv.bz2
AWS_TRAIN_OUTPUT = ${BUCKET}/train-output
AWS_PREDICT_OUTPUT = ${BUCKET}/predict-output
AWS_TRAIN_LOG = ${BUCKET}/train-logs
AWS_PREDICT_LOG = ${BUCKET}/predict-logs

.PHONY: jar

jar:
	sbt package
	cp target/scala*/redwingedblackbird_*.jar Main.jar
	cp data/target/scala*/data_*.jar Data.jar
	cp model/target/scala*/model_*.jar Model.jar
	jar -xf Main.jar
	jar -xf Data.jar
	jar -xf Model.jar
	jar -cf ${JAR} ebird/
	rm -rf Main.jar Data.jar Model.jar ebird META-INF

train: jar
	rm -rf ${TRAIN_OUTPUT}
	sbt "run ${MASTER} ${PARALLELISM} ${LABELED} ${TRAIN_OUTPUT}"

predict:
	rm -rf ${PREDICT_OUTPUT}
	sbt "run ${MASTER} ${PARALLELISM} ${UNLABELED} ${PREDICT_OUTPUT} ${TRAIN_OUTPUT}"

upload: jar
	aws s3 cp ${JAR} ${BUCKET}

train_remote: upload
	aws emr create-cluster \
		--name "Train 16 Machines" \
		--release-label emr-5.0.3 \
		--instance-groups '[{"InstanceCount":15,"InstanceGroupType":"CORE","InstanceType":"${INSTANCE_TYPE}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${INSTANCE_TYPE}"}]' \
		--applications Name=Hadoop Name=Spark \
		--steps '[{"Args":["spark-submit","--deploy-mode","cluster","--class","ebird.${MAIN}","${BUCKET}/${JAR}","${AWS_MASTER}","${AWS_PARALLELISM}","${AWS_LABELED}","${AWS_TRAIN_OUTPUT}"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"${MAIN}"}]' \
		--log-uri ${AWS_TRAIN_LOG} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${SUBNET} \
		--region ${REGION} \
		--enable-debugging \
		--auto-terminate

predict_remote: upload
	aws emr create-cluster \
        --name "Predict 16 Machines" \
        --release-label emr-5.0.3 \
        --instance-groups '[{"InstanceCount":15,"InstanceGroupType":"CORE","InstanceType":"${INSTANCE_TYPE}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${INSTANCE_TYPE}"}]' \
        --applications Name=Hadoop Name=Spark \
        --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--class","ebird.${MAIN}","${BUCKET}/${JAR}","${AWS_MASTER}","${AWS_PARALLELISM}","${AWS_UNLABELED}","${AWS_PREDICT_OUTPUT}","${AWS_TRAIN_OUTPUT}"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"${MAIN}"}]' \
        --log-uri ${AWS_PREDICT_LOG} \
        --service-role EMR_DefaultRole \
        --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${SUBNET} \
        --region ${REGION} \
        --enable-debugging \
        --auto-terminate

clean:
	rm -rf ${JAR} ${TRAIN_OUTPUT} ${PREDICT_OUTPUT} target project data/target model/target
