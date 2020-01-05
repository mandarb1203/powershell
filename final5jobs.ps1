#$Global:SCCMSQLSERVER = $PrinceDBServer
#$Global:DBNAME = $DatabaseName

$Global:SCCMSQLSERVER = "AZSAW0816"
$Global:DBNAME = "PRINCE"

$Global:PATH="C:\work\jobs\swagscript\final"
$Global:connection = New-Object System.Data.SQLClient.SQLConnection
$Global:connection.ConnectionString ="server=$SCCMSQLSERVER;database=$DBNAME;Integrated Security=True;"

#gas_production_forecast
$Param1_gas_production_forecast='%Update adhoc run flag for: orion.tasks.swag.forecast.task_run_gas_production_forecast%'
$Param2_gas_production_forecast='%Task orion.tasks.swag.forecast.task_run_gas_production_forecast%'

$Param1_elec_production_forecast='%Received task: orion.tasks.swag.forecast.task_run_elec_production_forecast%'
$Param2_elec_production_forecast='%Task orion.tasks.swag.forecast.task_run_elec_production_forecast%'

$Param1_elec_portfolio_forecast='%Received task: orion.tasks.swag.forecast.task_run_elec_portfolio_forecast%'
$Param2_elec_portfolio_forecast='%Task orion.tasks.swag.forecast.task_run_elec_portfolio_forecast%'

$Param1_gas_separate_customer_forecast='%Received task: orion.tasks.swag.forecast.task_run_gas_separate_customer_forecast%'
$Param2_gas_separate_customer_forecast='%Task orion.tasks.swag.forecast.task_run_gas_separate_customer_forecast%'

$Param1_elec_separate_customer_forecast='%Received task: orion.tasks.swag.forecast.task_run_elec_separate_customer_forecast%'
$Param2_elec_separate_customer_forecast='%Task orion.tasks.swag.forecast.task_run_elec_separate_customer_forecast%'


function Get-TimeStamp {
    
    return "[{0:MM/dd/yy} {0:HH:mm:ss}]" -f (Get-Date)
    
}
Function JobRun{
Param($Param1)
    Try
    {
        $connection.Open()
        write-output $Param1
	write-Output "$(Get-TimeStamp) JobRun Started $dc" | Out-file $PATH\SWAG_$CurrentDate.log -append
        write-output ("Job Run Started")
        $Sql=“update [PRINCE].[orion].[Tbl_Schedule_Master] SET [Run_Adhoc]='1' where Schedule_Name='$Param1'”
        write-output $Sql
        $sqlcmd = new-object "System.data.sqlclient.sqlcommand"
        $sqlcmd.connection = $connection
        $sqlcmd.CommandTimeout = 600
        $sqlcmd.CommandText =$Sql 
        $sqlcmd.ExecuteNonQuery()
	
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
	
	Write-Output "$(Get-TimeStamp) Database connection is successful for GetJobData $dc" | Out-file $PATH\SWAG_$CurrentDate.log -append
	write-output ("GetJob Started")
	Write-Output "$(Get-TimeStamp) GetJob Started $dc" | Out-file $PATH\SWAG_$CurrentDate.log -append
	write-output ("Database connection is successful")
	#write-output ($Param1)
	#write-output ($Param2)
	#write-output ($query)
	Write-Output "$(Get-TimeStamp) $query $dc" | Out-file $PATH\SWAG_$CurrentDate.log -append
        $command = $connection.CreateCommand()
        $command.CommandText = $query
        $command.CommandTimeout = 500
       $result = $command.ExecuteReader()
       $table = new-object “System.Data.DataTable”
       $table.Load($result)
       $CurrentDate = Get-Date
       $CurrentDate = $CurrentDate.ToString('yyyy-MM-dd')
        write-output PATH\SWAG_$CurrentDate.csv
       $table | Export-csv "$PATH\SWAG_$CurrentDate.csv" -NoTypeInformation -Append
       #write-output $table
       Write-Output "$(Get-TimeStamp) Script ran Successfully for GetJob data $dc" | Out-file $PATH\SWAG_$CurrentDate.log -append
       #write-output "Script ran Successfully"
       $connection.Close()
    }
    catch
    {
        #[System.Windows.Forms.MessageBox]::Show("Failed to connect SQL Server:") 
        Write-Error  $_	
    }
}
write-output ("Running 5 jobs")

#update adhoc run flag and get response time
JobRun 'swag-gas-production-forecast'
Start-Sleep -s 600
GetJobData $Param1_gas_production_forecast $Param2_gas_production_forecast gas_production_forecast

JobRun 'swag-elec-production-forecast'
Start-Sleep -s 800
GetJobData $Param1_elec_production_forecast $Param2_elec_production_forecast elec_production_forecast

JobRun 'swag-elec-portfolio-forecast'
Start-Sleep -s 600
GetJobData $Param1_elec_portfolio_forecast $Param2_elec_portfolio_forecast elec_portfolio_forecast

JobRun 'swag-gas-separate-customer-forecast'
Start-Sleep -s 600
GetJobData $Param1_gas_separate_customer_forecast $Param2_gas_separate_customer_forecast gas_separate_customer_forecast

JobRun 'swag-elec-separate-customer-forecast'
Start-Sleep -s 600
GetJobData $Param1_elec_separate_customer_forecast $Param2_elec_separate_customer_forecast elec_separate_customer_forecast


