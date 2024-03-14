
# Connect to subscription
Connect-AzAccount

# Initialize
$date = date
$SubscriptionID = "<Add Here>"
$ResGrp = "myDataEngineerResources"
# $DataFactory = "adf20210625" # Need to be same as in ARM template parameter
$DataFactory = "adfsuchita$date".Replace("/","").Replace(" ","").Replace(":","")  # Need to be same as in ARM template parameter
$StorageAcctName = "mystorage20210625"
$FileShareName = "adf-data"

$DataFactoryParamFile = "D:\Azure\DP-203\Data Factory\ARM template\parameters.json"

# Create Azure SQL Server and Database
New-AzResourceGroupDeployment -ResourceGroupName $ResGrp -maintenanceConfigurationId "/subscriptions/$SubscriptionID/providers/Microsoft.Maintenance/publicMaintenanceConfigurations/SQL_Default" `
    -TemplateFile "D:\Azure\DP-203\Data Factory\Copy Activity\SQL Server and DB\template.json" `
    -TemplateParameterFile "D:\Azure\DP-203\Data Factory\Copy Activity\SQL Server and DB\parameters.json"


# Create Storage account
New-AzResourceGroupDeployment -ResourceGroupName $ResGrp -Name $StorageAcctName `
    -TemplateFile "D:\Azure\DP-203\Data Factory\Copy Activity\storageaccount\template.json" `
    -TemplateParameterFile "D:\Azure\DP-203\Data Factory\Copy Activity\storageaccount\parameters.json"

# Create File Share "adf-data"
$acccontext = (Get-AzStorageAccount -ResourceGroupName $ResGrp -Name $StorageAcctName).Context
$fileshare = New-AzStorageShare -Context $acccontext -Name $FileShareName
$fileshare
# $fileshare = Get-AzStorageShare -Context $acccontext -Name $FileShareName

# upload data files
# New-AzStorageDirectory -Context $acccontext -ShareName $FileShareName -Path "ML Data"
Set-AzStorageFileContent -Context $acccontext -ShareName $FileShareName -Source "D:\Azure\DP-203\data\CustomerSource.csv" -Path "/"
Set-AzStorageFileContent -Context $acccontext -ShareName $FileShareName -Source "D:\Azure\DP-203\data\CustomerSource-Schema.csv" -Path "/"


# Change Data Factory name in json parameter file
<#
$temp = Get-Content $DataFactoryParamFile -raw | ConvertFrom-Json
$temp.parameters.name.value = $DataFactory
$temp | ConvertTo-Json -depth 2 | set-content $DataFactoryParamFile
#>
$temp = Get-Content $DataFactoryParamFile -raw
$temp = $temp.Replace("suchitaadf", $DataFactory)
$temp | set-content $DataFactoryParamFile

# Create Azure Data Factory (ADF)
New-AzResourceGroupDeployment -ResourceGroupName $ResGrp -Name $DataFactory `
    -TemplateFile "D:\Azure\DP-203\Data Factory\ARM template\template.json" `
    -TemplateParameterFile $DataFactoryParamFile

# Restore Data Factory name "suchitaadf" for next time changes
$temp = Get-Content $DataFactoryParamFile -raw
$temp = $temp.Replace($DataFactory, "suchitaadf")
$temp | set-content $DataFactoryParamFile


# CREATE TABLES MANUALLY USING QUERY IN AZURE
CREATE_TABLES MANUALLY USING QUERY IN AZURE


# Create Linked Service
Set-AzDataFactoryV2LinkedService -ResourceGroupName $ResGrp -DataFactoryName $DataFactory -Name "ADFDataFileShareLink" -File "D:\Azure\DP-203\Data Factory\Copy Activity\linkedService\ADFDataFileShareLink.json" | Format-List
Set-AzDataFactoryV2LinkedService -ResourceGroupName $ResGrp -DataFactoryName $DataFactory -Name "AzureSqlDatabase1" -File "D:\Azure\DP-203\Data Factory\SCD Type 1\Dataflow - UpdateCustomerDimension\linkedService\AzureSqlDatabase1.json" | Format-List
Set-AzDataFactoryV2LinkedService -ResourceGroupName $ResGrp -DataFactoryName $DataFactory -Name "AzureDatabricks" -File "D:\Azure\DP-203\Data Factory\Copy Activity\linkedService\AzureDatabricks.json" | Format-List


# EDIT LINKED SERVICES MANUALLY WITH PASSWORDS AND OTHER PARAMS IF ANY AND TEST CONNECTION
EDIT_LINKED_SERVICES MANUALLY WITH PASSWORDS AND OTHER PARAMS IF ANY AND TEST CONNECTION


# Create Datasets
Set-AzDataFactoryV2Dataset -DataFactoryName $DataFactory -ResourceGroupName $ResGrp -Name "adf_share_input_dataset" -DefinitionFile "D:\Azure\DP-203\Data Factory\Copy Activity\dataset\adf_share_input_dataset.json"
Set-AzDataFactoryV2Dataset -DataFactoryName $DataFactory -ResourceGroupName $ResGrp -Name "CustomerSource" -DefinitionFile "D:\Azure\DP-203\Data Factory\SCD Type 1\Dataflow - UpdateCustomerDimension\dataset\CustomerSource.json"
Set-AzDataFactoryV2Dataset -DataFactoryName $DataFactory `
    -ResourceGroupName $ResGrp -Name "DimCustomer" `
    -DefinitionFile "D:\Azure\DP-203\Data Factory\SCD Type 1\Dataflow - UpdateCustomerDimension\dataset\DimCustomer.json"

# Create Runtime
Set-AzDataFactoryV2IntegrationRuntime -ResourceGroupName $ResGrp -DataFactoryName $DataFactory -Name "MyAzureRuntime" `
    -Location "Norway East" -Type Managed -DataFlowComputeType General -DataFlowCoreCount 8 -DataFlowTimeToLive 0

# Create Copy Pipeline
Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResGrp -DataFactoryName $DataFactory -Name "copyPSVtoSQL" -DefinitionFile "D:\Azure\DP-203\Data Factory\Copy Activity\pipeline\copyPSVtoSQL.json"

# DEBUG PIPELINE MANUALLY
DEBUG_PIPELINE MANUALLY

# Create DataFlow
Set-AzDataFactoryV2DataFlow -ResourceGroupName $ResGrp -DataFactoryName $DataFactory -Name "UpdateCustomerDimension" -DefinitionFile "D:\Azure\DP-203\Data Factory\SCD Type 1\Dataflow - UpdateCustomerDimension\dataflow\UpdateCustomerDimension.json"

# Create Combined Copy & dataflow Pipeline
Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResGrp -DataFactoryName $DataFactory -Name "CopyPSVAndUpdateDim" -DefinitionFile "D:\Azure\DP-203\Data Factory\Copy PSV and SCD Type 1 pipeline\CopyPSVAndUpdateDim.json"

# Create Databricks Combined Copy & data transformation Pipeline
Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResGrp -DataFactoryName $DataFactory -Name "ADB-CopyPSVAndUpdateDim" -DefinitionFile "D:\Azure\DP-203\Data Factory\Copy PSV and SCD Type 1 Data Brick pipeline\ADB-CopyPSVAndUpdateDim.json"
