# =============================================================================
# AWS INFRASTRUCTURE SETUP - NATIONAL BEACH VOLLEYBALL RANKING
# Subject: Computación en la Nube
# =============================================================================

# 1. ENVIRONMENT VARIABLES
$env:AWS_REGION = "us-east-1"
$env:ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text)
$env:BUCKET_NAME = "datalake-bvb-ranking-$($env:ACCOUNT_ID)"
$env:ROLE_ARN = (aws iam get-role --role-name LabRole --query 'Role.Arn' --output text)

Write-Host "--- Configuration Loaded ---" -ForegroundColor Green
Write-Host "Bucket: $env:BUCKET_NAME"
Write-Host "Role: $env:ROLE_ARN"

# 2. S3 STORAGE & KINESIS STREAM
Write-Host "Creating S3 Buckets and Folders..."
aws s3 mb "s3://$env:BUCKET_NAME"

# Create folder structure for the Data Lake
# 2. Create folders (keys)
aws s3api put-object --bucket $env:BUCKET_NAME --key raw/
aws s3api put-object --bucket $env:BUCKET_NAME --key processed/
aws s3api put-object --bucket $env:BUCKET_NAME --key config/
aws s3api put-object --bucket $env:BUCKET_NAME --key scripts/
aws s3api put-object --bucket $env:BUCKET_NAME --key errors/

Write-Host "Creating Kinesis Data Stream..."
aws kinesis create-stream --stream-name player-points-stream --shard-count 1

# 3. LAMBDA TRANSFORMATION (Kinesis Firehose Processor)
# Note: Ensure 'firehose.py' exists in your current directory
Write-Host "Deploying Lambda Function..."
Compress-Archive -Path firehose.py -DestinationPath firehose.zip -Force

aws lambda create-function `
    --function-name bvb-firehose-transform `
    --runtime python3.12 `
    --role $env:ROLE_ARN `
    --handler firehose.lambda_handler `
    --zip-file fileb://firehose.zip `
    --timeout 60 `
    --memory-size 128

# Get Lambda ARN for Firehose configuration
$env:LAMBDA_ARN = (aws lambda get-function --function-name bvb-firehose-transform --query 'Configuration.FunctionArn' --output text)

# 4. KINESIS DATA FIREHOSE DELIVERY STREAM
Write-Host "Creating Firehose Delivery Stream..."
aws firehose create-delivery-stream `
    --delivery-stream-name bvb-delivery-stream `
    --delivery-stream-type KinesisStreamAsSource `
    --kinesis-stream-source-configuration "KinesisStreamARN=arn:aws:kinesis:$($env:AWS_REGION):$($env:ACCOUNT_ID):stream/player-points-stream,RoleARN=$($env:ROLE_ARN)" `
    --extended-s3-destination-configuration "{
        `"BucketARN`": `"arn:aws:s3:::$($env:BUCKET_NAME)`",
        `"RoleARN`": `"$($env:ROLE_ARN)`",
        `"Prefix`": `"raw/player_points/processing_date=!{partitionKeyFromLambda:processing_date}/`",
        `"ErrorOutputPrefix`": `"errors/!{firehose:error-output-type}/`",
        `"BufferingHints`": { `"SizeInMBs`": 1, `"IntervalInSeconds`": 60 },
        `"DynamicPartitioningConfiguration`": { `"Enabled`": true },
        `"ProcessingConfiguration`": {
            `"Enabled`": true,
            `"Processors`": [
                {
                    `"Type`": `"Lambda`",
                    `"Parameters`": [
                        { `"ParameterName`": `"LambdaArn`", `"ParameterValue`": `"$($env:LAMBDA_ARN)`" }
                    ]
                }
            ]
        }
    }"

# 5. AWS GLUE CATALOG & CRAWLER
Write-Host "Configuring AWS Glue Catalog..."
aws glue create-database --database-input "{\"Name\":\"bvb_ranking_db\"}"

aws glue create-crawler `
    --name bvb-player-crawler `
    --role $env:ROLE_ARN `
    --database-name bvb_ranking_db `
    --targets "{\"S3Targets\": [{\"Path\": \"s3://$($env:BUCKET_NAME)/raw/player_points\"}]}"

Write-Host "Starting Crawler... (This may take a minute)"
aws glue start-crawler --name bvb-player-crawler

# 6. AWS GLUE ETL JOB
# Note: Ensure 'points_aggregation.py' exists in your current directory
Write-Host "Uploading ETL Script to S3..."
aws s3 cp points_aggregation.py "s3://$env:BUCKET_NAME/scripts/"

Write-Host "Creating Glue Job..."
aws glue create-job `
    --name bvb-ranking-aggregation `
    --role $env:ROLE_ARN `
    --command "{
        `"Name`": `"glueetl`",
        `"ScriptLocation`": `"s3://$($env:BUCKET_NAME)/scripts/bvb_ranking_normalization.py`",
        `"PythonVersion`": `"3`"
    }" `
    --default-arguments "{
        `"--database`": `"bvb_ranking_db`",
        `"--table`": `"player_points`",
        `"--output_path`": `"s3://$($env:BUCKET_NAME)/processed/national_ranking_summary/`"
    }" `
    --glue-version "4.0" `
    --number-of-workers 2 `
    --worker-type "G.1X"

Write-Host "--- Infrastructure Complete ---" -ForegroundColor Green
Write-Host "Next step: Run your 'kinesis.py' producer script to send data."