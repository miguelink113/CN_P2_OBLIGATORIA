# =======================
# CONFIGURACIÓN DE VARIABLES
# =======================
$env:AWS_REGION = "us-east-1"

# S3 bucket y rutas
$BUCKET_NAME = "mi-bucket-glue"       # <- reemplaza con tu bucket
$JOB1_SCRIPT = "points_category_classification.py"
$JOB2_SCRIPT = "team_aggregation.py"
$JOB1_OUTPUT = "s3://$BUCKET_NAME/processed/points_category/"
$JOB2_OUTPUT = "s3://$BUCKET_NAME/processed/team_aggregation/"

# Glue Role ARN
$ROLE_ARN = "arn:aws:iam::123456789012:role/MyGlueRole"  # <- reemplaza con tu rol

# Nombres de jobs Glue
$GLUE_JOB_1 = "points_category_classification"
$GLUE_JOB_2 = "team_aggregation"

# Nombre de base de datos Glue
$GLUE_DB = "energy_db"

Write-Host "=== VARIABLES CONFIGURADAS ===" -ForegroundColor Cyan

# =======================
# FUNCIONES
# =======================
function New-TempJson {
    param($Content)
    $Path = [System.IO.Path]::GetTempFileName()
    $Content | ConvertTo-Json -Depth 5 -Compress | Out-File -FilePath $Path -Encoding ASCII
    return $Path
}

function Wait-ForJob {
    param($JobName, $RunId)
    Write-Host "Esperando a que termine el job $JobName (Run ID: $RunId)..." -ForegroundColor Magenta
    do {
        Start-Sleep -Seconds 15
        $Status = (aws glue get-job-run --job-name $JobName --run-id $RunId --query 'JobRun.JobRunState' --output text).Trim()
        Write-Host "  -> $JobName Status: $Status" -ForegroundColor Gray
    } while ($Status -in "STARTING", "RUNNING", "STOPPING")
    return $Status
}

# =======================
# SUBIR SCRIPTS A S3
# =======================
Write-Host "`n[1/3] Subiendo scripts Spark a S3..." -ForegroundColor Cyan
aws s3 cp "$PSScriptRoot/jobs/$JOB1_SCRIPT" "s3://$BUCKET_NAME/scripts/$JOB1_SCRIPT"
aws s3 cp "$PSScriptRoot/jobs/$JOB2_SCRIPT" "s3://$BUCKET_NAME/scripts/$JOB2_SCRIPT"

# =======================
# CREAR BASE DE DATOS GLUE
# =======================
Write-Host "`n[2/3] Creando base de datos Glue..." -ForegroundColor Cyan
$DbInput = @{ Name = $GLUE_DB }
$DbFile = New-TempJson -Content $DbInput
aws glue create-database --database-input "file://$DbFile" 2>&1 | Out-Null
Remove-Item $DbFile

# =======================
# CREAR JOBS GLUE
# =======================
Write-Host "`n[3/3] Creando jobs Glue..." -ForegroundColor Cyan

function Create-GlueJob($JobName, $ScriptPath, $OutputPath) {
    aws glue delete-job --job-name $JobName 2>&1 | Out-Null

    $JobCommand = @{
        Name           = "glueetl"
        ScriptLocation = "s3://$BUCKET_NAME/scripts/$ScriptPath"
        PythonVersion  = "3"
    }
    $JobArgs = @{
        "--database"     = $GLUE_DB
        "--table_name"   = "energy_consumption"
        "--output_path"  = $OutputPath
        "--job-language" = "python"
    }

    $CmdFile = New-TempJson -Content $JobCommand
    $ArgsFile = New-TempJson -Content $JobArgs

    aws glue create-job `
        --name $JobName `
        --role $ROLE_ARN `
        --command "file://$CmdFile" `
        --default-arguments "file://$ArgsFile" `
        --glue-version "4.0" `
        --number-of-workers 2 `
        --worker-type "G.1X"

    Remove-Item $CmdFile, $ArgsFile
}

Create-GlueJob -JobName $GLUE_JOB_1 -ScriptPath $JOB1_SCRIPT -OutputPath $JOB1_OUTPUT
Create-GlueJob -JobName $GLUE_JOB_2 -ScriptPath $JOB2_SCRIPT -OutputPath $JOB2_OUTPUT

# =======================
# EJECUCIÓN SECUENCIAL DE JOBS
# =======================
Write-Host "`n=== EJECUTANDO JOB GLUE: $GLUE_JOB_1 ===" -ForegroundColor Yellow
$jobRun1 = (aws glue start-job-run --job-name $GLUE_JOB_1 --query 'JobRunId' --output text).Trim()
if (-not $jobRun1) { Write-Host "No se pudo iniciar $GLUE_JOB_1" -ForegroundColor Red; exit }

$status1 = Wait-ForJob -JobName $GLUE_JOB_1 -RunId $jobRun1
Write-Host "Primer job finalizado con estado: $status1" -ForegroundColor Green

if ($status1 -eq "SUCCEEDED") {
    Write-Host "`n=== EJECUTANDO JOB GLUE: $GLUE_JOB_2 ===" -ForegroundColor Yellow
    $jobRun2 = (aws glue start-job-run --job-name $GLUE_JOB_2 --query 'JobRunId' --output text).Trim()
    if (-not $jobRun2) { Write-Host "No se pudo iniciar $GLUE_JOB_2" -ForegroundColor Red; exit }

    $status2 = Wait-ForJob -JobName $GLUE_JOB_2 -RunId $jobRun2
    Write-Host "Segundo job finalizado con estado: $status2" -ForegroundColor Green
} else {
    Write-Host "Primer job falló. Se omite la ejecución del segundo job." -ForegroundColor Red
    $status2 = "SKIPPED"
}

Write-Host "`n=== PIPELINE FINALIZADO ===" -ForegroundColor Cyan
Write-Host "$GLUE_JOB_1: $status1"
Write-Host "$GLUE_JOB_2: $status2"
