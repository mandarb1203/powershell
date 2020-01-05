#$Global:SCCMSQLSERVER = $PrinceDBServer
#$Global:DBNAME = $DatabaseName

$Global:SCCMSQLSERVER = "AZSAW0816"
$Global:DBNAME = "PRINCE"
$Global:PATH="C:\work\jobs\swagscript\final"
$Global:connection = New-Object System.Data.SQLClient.SQLConnection
$Global:connection.ConnectionString ="server=$SCCMSQLSERVER;database=$DBNAME;Integrated Security=True;"

#Input Jobs

# 1 update-swag-hh-weather
$Param1_update_hh_weather='%Received task: orion.tasks.swag.data.task_update_hh_weather%'
$Param2_update_hh_weather='%Task orion.tasks.swag.data.task_update_hh_weather%'

# 2 update-swag-sunshine-hours
$Param1_sunshine_hours='%Received task: orion.tasks.swag.data.task_update_sunshine_hours%'
$Param2_sunshine_hours='%Task orion.tasks.swag.data.task_update_sunshine_hours%'

# 3 update-swag-gas-ci-acq
$Param1_gas_ci_gate_acq='%Received task: orion.tasks.swag.data.task_update_gas_ci_acq%'
$Param2_gas_ci_gate_acq='%Task orion.tasks.swag.data.task_update_gas_ci_acq%'

# 4 update-swag-daytype-data
$Param1_update_daytype_data='%Received task: orion.tasks.swag.data.task_update_daytype_data%'
$Param2_update_daytype_data='%Task orion.tasks.swag.data.task_update_daytype_data%'

# 5 update-swag-gas-customer-numbers
$Param1_update_gas_customer_numbers='%Received task: orion.tasks.swag.data.task_update_gas_customer_numbers%'
$Param2_update_gas_customer_numbers='%Task orion.tasks.swag.data.task_update_gas_customer_numbers%'

# 6 update-swag-gas-sttm-totals
$Param1_update_gas_sttm_totals='%Received task: orion.tasks.swag.data.task_update_gas_sttm_totals%'
$Param2_update_gas_sttm_totals='%Task orion.tasks.swag.data.task_update_gas_sttm_totals%'

# 7 update-swag-gas-ci-demand
$Param1_update_gas_ci_demand='%Received task: orion.tasks.swag.data.task_update_gas_ci_demand%'
$Param2_update_gas_ci_demand='%Task orion.tasks.swag.data.task_update_gas_ci_demand%'

# 8 update-swag-gas-mm-demand
$Param1_update_gas_mm_demand='%Received task: orion.tasks.swag.data.task_update_gas_mm_demand%'
$Param2_update_gas_mm_demand='%Task orion.tasks.swag.data.task_update_gas_mm_demand%'

# 9 update-swag-gas-holidayfactor
$Param1_update_gas_holidayfactor='%Received task: orion.tasks.swag.data.task_update_gas_holidayfactor%'
$Param2_update_gas_holidayfactor='%Task orion.tasks.swag.data.task_update_gas_holidayfactor%'

# 10 update-swag-gas-default-customer-acq
$Param1_update_gas_default_customer_acq='%Received task: orion.tasks.swag.data.task_update_gas_default_customer_acq%'
$Param2_update_gas_default_customer_acq='%Task orion.tasks.swag.data.task_update_gas_default_customer_acq%'

# 11 update-swag-gas-ci-gate-station-acq
$Param1_update_gas_ci_gate_station_acq='%Received task: orion.tasks.swag.data.task_update_gas_ci_gate_station_acq%'
$Param2_update_gas_ci_gate_station_acq='%Task orion.tasks.swag.data.task_update_gas_ci_gate_station_acq%'

# 12 update-swag-elec-customer-numbers
$Param1_update_elec_customer_numbers='%Received task: orion.tasks.swag.data.task_update_elec_customer_numbers%'
$Param2_update_elec_customer_numbers='%Task orion.tasks.swag.data.task_update_elec_customer_numbers%'

# 13 update-swag-elec-system-demand
$Param1_update_elec_system_demand='%Received task: orion.tasks.swag.data.task_update_elec_system_demand%'
$Param2_update_elec_system_demand='%Task orion.tasks.swag.data.task_update_elec_system_demand%'

# 14 update-swag-elec-ci-acq
$Param1_update_elec_ci_acq='%Received task: orion.tasks.swag.data.task_update_elec_ci_acq%'
$Param2_update_elec_ci_acq='%Task orion.tasks.swag.data.task_update_elec_ci_acq%'

# 15 update-swag-elec-demand
$Param1_update_elec_system_demand='%Received task: orion.tasks.swag.data.task_update_elec_system_demand%'
$Param2_update_elec_system_demand='%Task orion.tasks.swag.data.task_update_elec_system_demand%'

# 16 update-gas-separate-customer-demand
$Param1_update_gas_separate_customer_forecast='%Received task: orion.tasks.swag.data.task_update_gas_separate_customer_forecast%'
$Param2_update_gas_separate_customer_forecast='%Task orion.tasks.swag.data.task_update_gas_separate_customer_forecast%'

# 17 update-elec-separate-customer-demand
$Param1_update_elec_separate_customer_forecast='%Received task: orion.tasks.swag.data.task_update_elec_separate_customer_forecast%'
$Param2_update_elec_separate_customer_forecast='%Task orion.tasks.swag.data.task_update_elec_separate_customer_forecast%'

# 18 update-gas-separate-customer-provided-forecast
$Param1_update_gas_separate_customer_provided_forecast='%Received task: orion.tasks.swag.data.task_update_gas_separate_customer_provided_forecast%'
$Param2_update_gas_separate_customer_provided_forecast='%Task orion.tasks.swag.data.task_update_gas_separate_customer_provided_forecast%'


# Schedule Names
function Get-TimeStamp {
    
    return "[{0:MM/dd/yy} {0:HH:mm:ss}]" -f (Get-Date)
    
}
Function JobRun{
Param($Param1)
    Try
    {
       	$CurrentDate = Get-Date
        $CurrentDate = $CurrentDate.ToString('yyyy-MM-dd')
 	$connection.Open()
        write-output $Param1
	Write-Output "$(Get-TimeStamp) Database connection is successful for JobRun function $dc" | Out-file $PATH\SWAG_$CurrentDate.log -append
        write-output ("Database connection is successful")
        $Sql=“update [PRINCE].[orion].[Tbl_Schedule_Master] SET [Run_Adhoc]='1' where Schedule_Name='$Param1'”
        #write-output $Sql
	Write-Output "$(Get-TimeStamp) $Sql $dc" | Out-file $PATH\SWAG_$CurrentDate.log -append
        $sqlcmd = new-object "System.data.sqlclient.sqlcommand"
        $sqlcmd.connection = $connection
        $sqlcmd.CommandTimeout = 300
        $sqlcmd.CommandText =$Sql 
        $sqlcmd.ExecuteNonQuery()
	$connection.Close()
    }catch
    {
       Write-Error  $_
	
    }Finally
    {
    	$connection.Close()
    }
}


Function GetJobData{
Param($Param1,$Param2,$Param3)
    Try
    {
        Write-Output "$(Get-TimeStamp) entered in to GetJobData function $dc" | Out-file $PATH\SWAG_$CurrentDate.log -append
	$query = " SELECT  top 1 q.[LogID],q.jobname,q.[Message] as MessageDescription,q.pDataDate as Starttime,q.Log_Timestamp as finishtime, DATEDIFF(SECOND, pDataDate, [Log_Timestamp]) as responsetime
FROM    (
        SELECT  *,
                LAG([Log_Timestamp]) OVER (ORDER BY log_timestamp) pDataDate ,'$Param3' as jobname
        FROM    [PRINCE].[Logger].[Log]
where Log_Timestamp >= dateadd(hh, -2, getdate()) and ([Message] like '$Param1') or ([Message] like '$Param2')
        ) q
WHERE   pDataDate IS NOT NULL
and [Message] like '$Param2' 
order by logid desc"

        $connection.Open()
	Write-Output "$(Get-TimeStamp) $query $dc" | Out-file $PATH\SWAG_$CurrentDate.log -append
 	Write-Output "$(Get-TimeStamp) Database connection is successful for Get data function $dc" | Out-file $PATH\SWAG_$CurrentDate.log -append
        write-output ("Database connection is successful")
	write-output ($Param1)
	write-output ($Param2)
	write-output ($query)
	$command = $connection.CreateCommand()
        $command.CommandText = $query
        $command.CommandTimeout = 400
       $result = $command.ExecuteReader()
       $table = new-object “System.Data.DataTable”
       $table.Load($result)
       $CurrentDate = Get-Date
       $CurrentDate = $CurrentDate.ToString('yyyy-MM-dd')
        write-output PATH\SWAG_InPutJobs_$CurrentDate.csv
       $table | Export-csv "$PATH\SWAG_InPutJobs_$CurrentDate.csv" -NoTypeInformation -Append
       write-output $table
       write-output "Script ran Successfully"	
       Write-Output "$(Get-TimeStamp) Script ran Successfully $dc" | Out-file $PATH\SWAG_$CurrentDate.log -append
       $connection.Close()
    }
    catch
    {
        #[System.Windows.Forms.MessageBox]::Show("Failed to connect SQL Server:") 
        Write-Error  $_	
    }
}
#update adhoc run flag and get response time
write-output ("Running Input jobs")
#1 update-swag-hh-weather
JobRun 'update-swag-hh-weather'
Start-Sleep -s 600
GetJobData $Param1_update_hh_weather $Param2_update_hh_weather update_hh_weather

#2 update-swag-sunshine-hours
JobRun 'update-swag-sunshine-hours'
Start-Sleep -s 600
GetJobData $Param1_sunshine_hours $Param2_sunshine_hours update-swag-sunshine-hours

#3 update-swag-gas-ci-acq
JobRun 'update-swag-gas-ci-acq'
Start-Sleep -s 600
GetJobData $Param1_gas_ci_gate_acq $Param2_gas_ci_gate_acq update-swag-gas-ci-acq

#4 update-swag-daytype-data
JobRun 'update-swag-daytype-data'
Start-Sleep -s 600
GetJobData $Param1_update_daytype_data $Param2_update_daytype_data update_daytype_data

#5 update-swag-gas-customer-numbers
JobRun 'update-swag-gas-customer-numbers'
Start-Sleep -s 600
GetJobData $Param1_update_gas_customer_numbers $Param2_update_gas_customer_numbers update_gas_customer_numbers

#6 update-swag-gas-sttm-totals
JobRun 'update-swag-gas-sttm-totals'
Start-Sleep -s 600
GetJobData $Param1_update_gas_sttm_totals $Param2_update_gas_sttm_totals update_gas_sttm_totals

#7 update-swag-gas-ci-demand
JobRun 'update-swag-gas-ci-demand'
Start-Sleep -s 600
GetJobData $Param1_update_gas_ci_demand $Param2_update_gas_ci_demand update_gas_ci_demand

#8 update-swag-gas-mm-demand
JobRun 'update-swag-gas-mm-demand'
Start-Sleep -s 600
GetJobData $Param1_update_gas_mm_demand $Param2_update_gas_mm_demand update_gas_mm_demand

#9 update-swag-gas-holidayfactor
JobRun 'update-swag-gas-holidayfactor'
Start-Sleep -s 600
GetJobData $Param1_update_gas_holidayfactor $Param2_update_gas_holidayfactor update_gas_holidayfactor

#10 update-swag-gas-default-customer-acq
JobRun 'update-swag-gas-default-customer-acq'
Start-Sleep -s 600
GetJobData $Param1_update_gas_default_customer_acq $Param2_update_gas_default_customer_acq update_gas_default_customer_acq

#11 update-swag-gas-ci-gate-station-acq
JobRun 'update-swag-gas-ci-gate-station-acq'
Start-Sleep -s 600
GetJobData $Param1_update_gas_ci_gate_station_acq $Param2_update_gas_ci_gate_station_acq update-swag-gas-ci-gate-station-acq

#12 update-swag-elec-customer-numbers
JobRun 'update-swag-elec-customer-numbers'
Start-Sleep -s 600
GetJobData $Param1_update_elec_customer_numbers $Param2_update_elec_customer_numbers update_elec_customer_numbers

#13 update-swag-elec-system-demand
JobRun 'update-swag-elec-system-demand'
Start-Sleep -s 600
GetJobData $Param1_update_elec_system_demand $Param2_update_elec_system_demand update_elec_system_demand

#14 update-swag-elec-ci-acq
JobRun 'update-swag-elec-ci-acq'
Start-Sleep -s 600
GetJobData $Param1_update_elec_ci_acq $Param2_update_elec_ci_acq update_elec_ci_acq

#15 update-swag-elec-demand
JobRun 'update-gas-separate-customer-demand'
Start-Sleep -s 600
GetJobData $Param1_update_gas_separate_customer_demand $Param2_update_gas_separate_customer_demand update_gas_separate_customer_demand

#16 update-gas-separate-customer-demand
JobRun 'update-elec-separate-customer-demand'
Start-Sleep -s 600
GetJobData $Param1_update_elec_separate_customer_demand $Param2_update_elec_separate_customer_demand update_elec_separate_customer_demand

#17 update-elec-separate-customer-demand
JobRun 'update-gas-separate-customer-provided-forecast'
Start-Sleep -s 600
GetJobData $Param1_update_gas_separate_customer_provided_forecast $Param2_update_gas_separate_customer_provided_forecast update_gas_separate_customer_provided_forecast

#18 update-gas-separate-customer-provided-forecast
JobRun 'update-gas-separate-customer-provided-forecast'
GetJobData $Param1_update_gas_separate_customer_provided_forecast $Param2_update_gas_separate_customer_provided_forecast update-gas-separate-customer-provided-forecast




