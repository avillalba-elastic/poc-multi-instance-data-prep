aws stepfunctions create-state-machine --name poc-multi-instance-data-prep-seq-sharded-by-key --definition file://pipelines/seq_sharded_by_key.json --role-arn arn:aws:iam::879381254630:role/service-role/StepFunctions-mvp_mlops_platform_data_preprocessi-role-bn368ux1z --profile ml
aws stepfunctions create-state-machine --name poc-multi-instance-data-prep-delta-batch-processing --definition file://pipelines/delta_batch_processing.json --role-arn arn:aws:iam::879381254630:role/service-role/StepFunctions-mvp_mlops_platform_data_preprocessi-role-bn368ux1z --profile ml

aws stepfunctions create-state-machine --name poc-multi-instance-data-prep-ray-s3-sagemaker-processing --definition file://pipelines/ray_s3_sagemaker_processing.json --role-arn arn:aws:iam::879381254630:role/service-role/StepFunctions-mvp_mlops_platform_data_preprocessi-role-bn368ux1z --profile ml
aws stepfunctions create-state-machine --name poc-multi-instance-data-prep-ray-sagemaker-processing --definition file://pipelines/ray_sagemaker_processing.json --role-arn arn:aws:iam::879381254630:role/service-role/StepFunctions-mvp_mlops_platform_data_preprocessi-role-bn368ux1z --profile ml

aws stepfunctions create-state-machine --name poc-multi-instance-data-prep-pyspark-sagemaker-processing --definition file://pipelines/hello_pyspark.json --role-arn arn:aws:iam::879381254630:role/service-role/StepFunctions-mvp_mlops_platform_data_preprocessi-role-bn368ux1z --profile ml

#aws stepfunctions update-state-machine --state-machine-arn {arn} --definition file://pipelines/{pipeline_definition}.json --profile ml
