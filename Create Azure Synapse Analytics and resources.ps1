
# Connect to subscription
Connect-AzAccount

# Initialize
$date = date
$SubscriptionID = "<Add Here>"
$ResGrp = "myDataEngineerResources"
# $SynapseAnalyicsName = "adf20210625" # Need to be same as in ARM template parameter
$SynapseAnalyicsName = "synapse-ws-$date".Replace("/","").Replace(" ","").Replace(":","")  # Need to be same as in ARM template parameter
$StorageAcctName = "mystorage20210625"
$FileShareName = "adf-data"

$SynapseParamFile = "D:\Azure\DP-203\Synapse WS\blank\parameters.json"

# Create Azure SQL Server and Database
New-AzResourceGroupDeployment -ResourceGroupName $ResGrp -maintenanceConfigurationId "/subscriptions/$SubscriptionID/providers/Microsoft.Maintenance/publicMaintenanceConfigurations/SQL_Default" `
    -TemplateFile "D:\Azure\DP-203\Copy Activity\SQL Server and DB\template.json" `
    -TemplateParameterFile "D:\Azure\DP-203\Copy Activity\SQL Server and DB\parameters.json"


# Create Storage account
New-AzResourceGroupDeployment -ResourceGroupName $ResGrp -Name $StorageAcctName `
    -TemplateFile "D:\Azure\DP-203\Copy Activity\storageaccount\template.json" `
    -TemplateParameterFile "D:\Azure\DP-203\Copy Activity\storageaccount\parameters.json"

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
$temp = Get-Content $SynapseParamFile -raw | ConvertFrom-Json
$temp.parameters.name.value = $SynapseAnalyicsName
$temp | ConvertTo-Json -depth 2 | set-content $SynapseParamFile
#>
$temp = Get-Content $SynapseParamFile -raw
$temp = $temp.Replace("suchitaadf", $SynapseAnalyicsName)
$temp | set-content $SynapseParamFile

# Create Azure Synapse Analytics
New-AzResourceGroupDeployment -ResourceGroupName $ResGrp -Name $SynapseAnalyicsName `
    -TemplateFile "D:\Azure\DP-203\Synapse WS\blank\template.json" `
    -TemplateParameterFile $SynapseParamFile

# Restore Data Factory name "suchitaadf" for next time changes
$temp = Get-Content $SynapseParamFile -raw
$temp = $temp.Replace($SynapseAnalyicsName, "suchitaadf")
$temp | set-content $SynapseParamFile


# CREATE TABLES MANUALLY USING QUERY IN AZURE
CREATE_TABLES MANUALLY USING QUERY IN AZURE


# Create Linked Service
Set-AzDataFactoryV2LinkedService -ResourceGroupName $ResGrp -DataFactoryName $SynapseAnalyicsName -Name "ADFDataFileShareLink" -File "D:\Azure\DP-203\Copy Activity\linkedService\ADFDataFileShareLink.json" | Format-List
Set-AzDataFactoryV2LinkedService -ResourceGroupName $ResGrp -DataFactoryName $SynapseAnalyicsName -Name "AzureSqlDatabase1" -File "D:\Azure\DP-203\SCD Type 1\Dataflow - UpdateCustomerDimension\linkedService\AzureSqlDatabase1.json" | Format-List


# EDIT LINKED SERVICES MANUALLY WITH PASSWORDS AND OTHER PARAMS IF ANY AND TEST CONNECTION
EDIT_LINKED_SERVICES MANUALLY WITH PASSWORDS AND OTHER PARAMS IF ANY AND TEST CONNECTION


# Create Datasets
Set-AzDataFactoryV2Dataset -DataFactoryName $SynapseAnalyicsName -ResourceGroupName $ResGrp -Name "adf_share_input_dataset" -DefinitionFile "D:\Azure\DP-203\Copy Activity\dataset\adf_share_input_dataset.json"
Set-AzDataFactoryV2Dataset -DataFactoryName $SynapseAnalyicsName -ResourceGroupName $ResGrp -Name "CustomerSource" -DefinitionFile "D:\Azure\DP-203\SCD Type 1\Dataflow - UpdateCustomerDimension\dataset\CustomerSource.json"
Set-AzDataFactoryV2Dataset -DataFactoryName $SynapseAnalyicsName `
    -ResourceGroupName $ResGrp -Name "DimCustomer" `
    -DefinitionFile "D:\Azure\DP-203\SCD Type 1\Dataflow - UpdateCustomerDimension\dataset\DimCustomer.json"

# Create Runtime
Set-AzDataFactoryV2IntegrationRuntime -ResourceGroupName $ResGrp -DataFactoryName $SynapseAnalyicsName -Name "MyAzureRuntime" `
    -Location "Norway East" -Type Managed -DataFlowComputeType General -DataFlowCoreCount 8 -DataFlowTimeToLive 0

# Create Copy Pipeline
Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResGrp -DataFactoryName $SynapseAnalyicsName -Name "copyPSVtoSQL" -DefinitionFile "D:\Azure\DP-203\Copy Activity\pipeline\copyPSVtoSQL.json"

# DEBUG PIPELINE MANUALLY
DEBUG_PIPELINE MANUALLY

# Create DataFlow
Set-AzDataFactoryV2DataFlow -ResourceGroupName $ResGrp -DataFactoryName $SynapseAnalyicsName -Name "UpdateCustomerDimension" -DefinitionFile "D:\Azure\DP-203\SCD Type 1\Dataflow - UpdateCustomerDimension\dataflow\UpdateCustomerDimension.json"

# Create Combined Copy & dataflow Pipeline
Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResGrp -DataFactoryName $SynapseAnalyicsName -Name "CopyPSVAndUpdateDim" -DefinitionFile "D:\Azure\DP-203\Copy PSV and SCD Type 1 pipeline\CopyPSVAndUpdateDim.json"
