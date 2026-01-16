# =======================
# VARIABLE CONFIGURATION
# =======================
$env:AWS_REGION = "us-east-1"
$env:ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text)

$BASE_DIR = Split-Path -Parent $PSScriptRoot
$JOBS_DIR = "$BASE_DIR\jobs"

$BUCKET_NAME = "datalake-voley-ranking-$($env:ACCOUNT_ID)"
$JOB1_SCRIPT = "points_category_classification.py"
$JOB2_SCRIPT = "team_aggregation.py"

$JOB1_OUTPUT = "s3://$BUCKET_NAME/processed/points_category/"
$JOB2_OUTPUT = "s3://$BUCKET_NAME/processed/team_aggregation/"

$ROLE_NAME = "LabRole"
$GLUE_DB = "voley_db"

$GLUE_JOB_1 = "points_category_classification"
$GLUE_JOB_2 = "team_aggregation"

$S3_RAW_DATA = "s3://$BUCKET_NAME/raw/"
$CRAWLER_NAME = "glue_voley_crawler"

$S3_PROCESSED_DATA = "s3://$BUCKET_NAME/processed/"
$CRAWLER_PROCESSED_NAME = "glue_voley_processed_crawler"

Write-Host "=== CONFIGURATION LOADED ===" -ForegroundColor Cyan

# =======================
# HELPER FUNCTIONS
# =======================
function New-TempJson {
    param($Content)
    $Path = [System.IO.Path]::GetTempFileName()
    $Content | ConvertTo-Json -Depth 10 -Compress | Out-File -FilePath $Path -Encoding ASCII
    return $Path
}

function Wait-ForJob {
    param($JobName, $RunId)
    Write-Host "Waiting for Glue Job: $JobName..." -ForegroundColor Magenta
    do {
        Start-Sleep -Seconds 15
        $Status = (aws glue get-job-run --job-name $JobName --run-id $RunId --query 'JobRun.JobRunState' --output text).Trim()
        Write-Host "  -> Status: $Status" -ForegroundColor Gray
    } while ($Status -in @("STARTING", "RUNNING", "STOPPING"))
    return $Status
}

function Wait-ForCrawler {
    param($Name)
    Write-Host "Waiting for Crawler: $Name..." -ForegroundColor Magenta
    do {
        Start-Sleep -Seconds 10
        $Status = (aws glue get-crawler --name $Name --query 'Crawler.State' --output text)
        Write-Host "  -> State: $Status" -ForegroundColor Gray
    } while ($Status -ne "READY")
}

function Wait-ForDeletion {
    param($Name)
    while ($true) {
        $null = aws glue get-crawler --name $Name 2>$null
        if ($LASTEXITCODE -ne 0) { break }
        Start-Sleep -Seconds 5
    }
}

# =======================
# [1/4] UPLOAD SCRIPTS
# =======================
Write-Host "`n[1/4] Uploading Spark scripts to S3..." -ForegroundColor Cyan
aws s3 cp "$JOBS_DIR\$JOB1_SCRIPT" "s3://$BUCKET_NAME/scripts/$JOB1_SCRIPT"
aws s3 cp "$JOBS_DIR\$JOB2_SCRIPT" "s3://$BUCKET_NAME/scripts/$JOB2_SCRIPT"

# =======================
# [2/4] CATALOG CONFIGURATION (RAW DATA)
# =======================
Write-Host "`n[2/4] Configuring Data Catalog..." -ForegroundColor Cyan

# Create Database
$null = aws glue create-database --database-input "{\"Name\":\"$GLUE_DB\"}" 2>$null

# Recreate RAW Crawler
$null = aws glue get-crawler --name $CRAWLER_NAME 2>$null
if ($LASTEXITCODE -eq 0) {
    aws glue delete-crawler --name $CRAWLER_NAME
    Wait-ForDeletion $CRAWLER_NAME
}

$CRAWLER_TARGETS = "{\`"S3Targets\`": [{\`"Path\`": \`"$S3_RAW_DATA\`"}]}"
aws glue create-crawler --name $CRAWLER_NAME --role $ROLE_NAME --database-name $GLUE_DB --targets $CRAWLER_TARGETS

# Run RAW Crawler to detect tables
Write-Host "Running RAW Crawler..." -ForegroundColor Yellow
$null = aws glue start-crawler --name $CRAWLER_NAME
Wait-ForCrawler $CRAWLER_NAME

$TABLE_NAME = (aws glue get-tables --database-name $GLUE_DB --query "TableList[0].Name" --output text).Trim().Replace('"','')
Write-Host "Table detected: $TABLE_NAME" -ForegroundColor Green

# =======================
# [3/4] GLUE JOB PROVISIONING
# =======================
Write-Host "`n[3/4] Creating Glue ETL Jobs..." -ForegroundColor Cyan

function Create-GlueJob($JobName, $ScriptName, $OutputPath) {
    $null = aws glue delete-job --job-name $JobName 2>$null

    $JobCommand = @{ Name = "glueetl"; ScriptLocation = "s3://$BUCKET_NAME/scripts/$ScriptName"; PythonVersion = "3" }
    $JobArgs = @{ "--job-language" = "python"; "--output_path" = $OutputPath; "--database" = $GLUE_DB; "--table_name" = $TABLE_NAME }

    $CmdFile  = New-TempJson $JobCommand
    $ArgsFile = New-TempJson $JobArgs

    aws glue create-job --name $JobName --role $ROLE_NAME --command "file://$CmdFile" --default-arguments "file://$ArgsFile" --glue-version "4.0" --worker-type "G.1X" --number-of-workers 2
    Remove-Item $CmdFile, $ArgsFile
}

Create-GlueJob $GLUE_JOB_1 $JOB1_SCRIPT $JOB1_OUTPUT
Create-GlueJob $GLUE_JOB_2 $JOB2_SCRIPT $JOB2_OUTPUT

# =======================
# [4/4] SEQUENTIAL EXECUTION
# =======================

Write-Host "`n[4/4] Executing Pipeline..." -ForegroundColor Cyan

# Job 1
$jobRun1 = aws glue start-job-run --job-name $GLUE_JOB_1 --query 'JobRunId' --output text
$status1 = Wait-ForJob $GLUE_JOB_1 $jobRun1

# Job 2 (Only if Job 1 Succeeded)
if ($status1 -eq "SUCCEEDED") {
    $jobRun2 = aws glue start-job-run --job-name $GLUE_JOB_2 --query 'JobRunId' --output text
    $status2 = Wait-ForJob $GLUE_JOB_2 $jobRun2
} else {
    Write-Host "Job 1 Failed. Skipping Job 2." -ForegroundColor Red
    $status2 = "SKIPPED"
}

# =======================
# POST-PROCESSING: CATALOG PROCESSED DATA
# =======================
Write-Host "`n=== REGISTERING PROCESSED TABLES ===" -ForegroundColor Cyan

$null = aws glue get-crawler --name $CRAWLER_PROCESSED_NAME 2>$null
if ($LASTEXITCODE -eq 0) {
    aws glue delete-crawler --name $CRAWLER_PROCESSED_NAME
    Wait-ForDeletion $CRAWLER_PROCESSED_NAME
}

$PROCESSED_TARGETS = "{\`"S3Targets\`": [{\`"Path\`": \`"$S3_PROCESSED_DATA\`"}]}"
aws glue create-crawler --name $CRAWLER_PROCESSED_NAME --role $ROLE_NAME --database-name $GLUE_DB --targets $PROCESSED_TARGETS

Write-Host "Running Processed Data Crawler..." -ForegroundColor Yellow
$null = aws glue start-crawler --name $CRAWLER_PROCESSED_NAME
Wait-ForCrawler $CRAWLER_PROCESSED_NAME

Write-Host "`n=== PIPELINE FINISHED ===" -ForegroundColor Cyan
Write-Host "$GLUE_JOB_1 : $status1"
Write-Host "$GLUE_JOB_2 : $status2"
Write-Host "Processed tables are now available in Glue Catalog." -ForegroundColor Green