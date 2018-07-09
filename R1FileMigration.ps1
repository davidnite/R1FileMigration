Function R1FileMigration {
    
    param (
        [parameter(ParameterSetName='Sync',Mandatory = $false)][Switch]$Sync,
        [parameter(ParameterSetName='Sync',Mandatory=$true)][String]$SQLHostName,
        [parameter(ParameterSetName='Transfer',Mandatory = $false)][Switch]$Transfer,
        [parameter(ParameterSetName='Transfer',Mandatory=$true)][String]$SQLServerName,
        [parameter(ParameterSetName='Transfer',Mandatory=$true)][String]$targetPath,
        [parameter(Mandatory = $false)][Switch]$Report
        
    )

    $scriptRoot = "$PSScriptRoot"
    $csvRoot = "$PSScriptRoot\Csv"
    $dbRoot = "$PSScriptRoot\Db"
    $reportRoot = "$PSScriptRoot\Report"

    # Basic log settings
    $logFile = "$PSScriptRoot\applog.log"
    $logLevel = "DEBUG" # ("DEBUG","INFO","WARN","ERROR","FATAL")
    $logSize = 1mb # 30kb
    $logCount = 10

    # Log functions
    function Write-Log-Line ($line) {
        Add-Content $logFile -Value $Line
        Write-Host $Line
    }

    Function Write-Log {
        [CmdletBinding()]
        Param(
        [Parameter(Mandatory=$True)][string]$Message,
        [Parameter(Mandatory=$False)][String]$Level = "DEBUG"
        )

        $levels = ("DEBUG","INFO","WARN","ERROR","FATAL")
        $logLevelPos = [array]::IndexOf($levels, $logLevel)
        $levelPos = [array]::IndexOf($levels, $Level)
        $Stamp = (Get-Date).toString("yyyy/MM/dd HH:mm:ss:fff")

        if ($logLevelPos -lt 0){
            Write-Log-Line "$Stamp ERROR Wrong logLevel configuration [$logLevel]"
        }
        
        if ($levelPos -lt 0){
            Write-Log-Line "$Stamp ERROR Wrong log level parameter [$Level]"
        }

        # if level parameter is wrong or configuration is wrong I still want to see the message in log
        if ($levelPos -lt $logLevelPos -and $levelPos -ge 0 -and $logLevelPos -ge 0){
            return
        }

        $Line = "$Stamp $Level $Message"
        Write-Log-Line $Line
    }

    function Reset-Log { 
        # function checks to see if file in question is larger than the paramater specified 
        # if it is it will roll a log and delete the oldes log if there are more than x logs. 
        param([string]$fileName, [int64]$filesize = 1mb , [int] $logcount = 5) 
        
        $logRollStatus = $true 
        if(test-path $filename) 
        { 
            $file = Get-ChildItem $filename 
            if((($file).length) -ige $filesize) #this starts the log roll 
            { 
                $fileDir = $file.Directory 
                #this gets the name of the file we started with 
                $fn = $file.name
                $files = Get-ChildItem $filedir | ?{$_.name -like "$fn*"} | Sort-Object lastwritetime 
                #this gets the fullname of the file we started with 
                $filefullname = $file.fullname
                #$logcount +=1 #add one to the count as the base file is one more than the count 
                for ($i = ($files.count); $i -gt 0; $i--) 
                {  
                    #[int]$fileNumber = ($f).name.Trim($file.name) #gets the current number of 
                    # the file we are on 
                    $files = Get-ChildItem $filedir | ?{$_.name -like "$fn*"} | Sort-Object lastwritetime 
                    $operatingFile = $files | ?{($_.name).trim($fn) -eq $i} 
                    if ($operatingfile) 
                    {$operatingFilenumber = ($files | ?{($_.name).trim($fn) -eq $i}).name.trim($fn)} 
                    else 
                    {$operatingFilenumber = $null} 
    
                    if(($operatingFilenumber -eq $null) -and ($i -ne 1) -and ($i -lt $logcount)) 
                    { 
                        $operatingFilenumber = $i 
                        $newfilename = "$filefullname.$operatingFilenumber" 
                        $operatingFile = $files | ?{($_.name).trim($fn) -eq ($i-1)} 
                        write-host "moving to $newfilename" 
                        move-item ($operatingFile.FullName) -Destination $newfilename -Force 
                    } 
                    elseif($i -ge $logcount) 
                    { 
                        if($operatingFilenumber -eq $null) 
                        {  
                            $operatingFilenumber = $i - 1 
                            $operatingFile = $files | ?{($_.name).trim($fn) -eq $operatingFilenumber} 
                            
                        } 
                        write-host "deleting " ($operatingFile.FullName) 
                        remove-item ($operatingFile.FullName) -Force 
                    } 
                    elseif($i -eq 1) 
                    { 
                        $operatingFilenumber = 1 
                        $newfilename = "$filefullname.$operatingFilenumber" 
                        write-host "moving to $newfilename" 
                        move-item $filefullname -Destination $newfilename -Force 
                    } 
                    else 
                    { 
                        $operatingFilenumber = $i +1  
                        $newfilename = "$filefullname.$operatingFilenumber" 
                        $operatingFile = $files | ?{($_.name).trim($fn) -eq ($i-1)} 
                        write-host "moving to $newfilename" 
                        move-item ($operatingFile.FullName) -Destination $newfilename -Force    
                    } 
                } 
            } 
            else { $logRollStatus = $false } 
        } 
        else { $logrollStatus = $false } 
        $LogRollStatus 
    } 

    # to null to avoid output
    $Null = @(
        Reset-Log -fileName $logFile -filesize $logSize -logcount $logCount
    )

    # SQLite Settings

    Add-Type -Path "$PSScriptRoot\Bin\System.Data.SQLite.dll"
    $sldb = "$dbRoot\R1Mig.sqlite"
    $slcon = New-Object -TypeName System.Data.SQLite.SQLiteConnection
    $slcon.ConnectionString = "Data Source=$sldb"
    $slcon.Open()
    $slDbTest = Test-Path -Path "$dbRoot\R1Mig.sqlite"
    if ($slDbTest -eq "False") {
        $createTable = $slcon.CreateCommand() #This will create the database and MigrationData table if they havent been created yet
        $createTable.CommandText = "CREATE TABLE IF NOT EXISTS MigrationData (CaseID text PRIMARY KEY, Priority text, TotalFiles text, TotalBytes text, LastDocumentID text, MigratedFiles text, MigratedBytes text, LastMigratedID text)"
        try { $createTable.ExecuteNonQuery() }
        catch {
            Write-Log "Couldn't create the SQLite MigrationData table" "ERROR"
            Break
        }
        Write-Log "Sucessfully created the SQLite database at $dbRoot\R1Mig.sqlite" "INFO"
    }
    else { Write-Log "Found SQLite database at $dbRoot\R1Mig.sqlite" "INFO"}


    # SQL Server Settings
    if (!(Get-Module sqlserver)) {
        try {
            Import-Module sqlserver -Force -ErrorAction Stop
        } catch {
            Write-Log "could not load sqlserver module, check the powershell log" "ERROR"
            Break
        }
    }
    $cred = Get-Credential
    # Get a list of all EDDS******* databases in the SQL instance

    if (!$SQLServerName) { $SQLHost = $SQLHostName }
    else { $SQLHost = $SQLServerName }

    try { $dbs = Get-SqlDatabase -ServerInstance $SQLHost -Credential $cred | Where { $_.Name -match 'EDD\D\d{7}$' } -ErrorAction Stop }
    catch {
        Write-Log "Unable to get the list of databases, check your SQL server host and credentials" "ERROR"
        Break
    }
    Write-Log "Successfully connected to SQL and gathered the list of databases" "INFO"
    
    function Sync { 
        
        # Function for syncing sql database to sqlite
       
        foreach ($db in $dbs.Name) {

            # Check if Sync has run before
            $check = $slcon.CreateCommand()
            $check.CommandText = "select * from MigrationData where CaseID = '$db'"
            $adapter = New-Object -TypeName System.Data.SQLite.SQLiteDataAdapter $check
            $table = New-Object System.Data.DataTable
            try { [void]$adapter.Fill($table) }
            catch {
                Write-Log "Unable to check the state of the of workspace ID $db" "ERROR"
                Break
            }
        
            $fileQuery = "SELECT COUNT(*) FROM [$db].[EDDSDBO].[File]"
            $sizeQuery = "SELECT SUM(Size) FROM [$db].[EDDSDBO].[File]"
            $lastDocQuery = "SELECT MAX(DocumentArtifactID) from [$db].[EDDSDBO].[File]"
            
            try { $fileCount = Invoke-Sqlcmd -ServerInstance $SQLHost -Credential $cred -Query $fileQuery -ErrorVariable $sqlError}
            catch {
                Write-Log "Couldn't get the total file count from [$db].[EDDSDBO].[File] `r`n $sqlError" "WARN"
            }
    
            if ($fileCount.Column1 -gt 0) {
    
                try { $fileSize = Invoke-Sqlcmd -ServerInstance $SQLHost -Credential $cred -Query $sizeQuery -ErrorVariable $sqlError}
                catch {
                    Write-Log "Couldn't get the total file size from [$db].[EDDSDBO].[File] `r`n $sqlError" "WARN"
                }
                try { $lastDoc = Invoke-Sqlcmd -ServerInstance $SQLHost -Credential $cred -Query $lastDocQuery -ErrorVariable $sqlError}
                catch {
                    Write-Log "Couldn't get the last document artifact ID from [$db].[EDDSDBO].[File] `r`n $sqlError" "WARN"
                }
    
                $f = $fileCount.Column1; $s = $fileSize.Column1; $d = $lastDoc.Column1
                
                if ($table.CaseID -ne $db) {
                    #First time sync
                    Write-Host "Adding $db to Migration DB :: Total Files: $f  Total File Size: $s (b)  Last document ID: $d"
                    Write-Log "Adding $db to Migration DB :: Total Files:$f  Total File Size:$s(bytes)  Last document ID:$d" "INFO"
                    
                    $insert = $slcon.CreateCommand()
                    $insert.CommandText = "INSERT INTO MigrationData (CaseID, TotalFiles, TotalBytes, LastDocumentID) VALUES ('$db', '$f', '$s', '$d')"
                    try { $insert.ExecuteNonQuery() }
                    catch {
                        Write-Log "Unable to sync data for workspace ID $db to the local database" "WARN"
                    }
                }
                else {
                    #Delta sync
                    Write-Host "Updating $db in Migration DB :: Total Files: $f  Total File Size: $s (b)  Last document ID: $d"
                    Write-Log "Updating $db in Migration DB :: Total Files:$f  Total File Size:$s(bytes)  Last document ID:$d" "INFO"

                    $insert = $slcon.CreateCommand()
                    $insert.CommandText = "UPDATE MigrationData 
                                           SET TotalFiles = '$f',
                                               TotalBytes = '$s',
                                               LastDocumentID = '$d'
                                            WHERE CaseID = '$db'"
                    try { $insert.ExecuteNonQuery() }
                    catch {
                        Write-Log "Unable to update data for workspace ID $db in the local database" "WARN"
                    }
                }
            }
        }   
    }

    function Transfer {

        # Function for creating csv files

        foreach ($db in $dbs.Name) {

            # Create a SQL query that gathers the necessary data from file table in each database

            $sqlQuery = "SELECT Location AS ""source-path"", '$targetPath' AS ""target-path"", Filename AS ""targetfilename"", DocumentArtifactID FROM [$db].[EDDSDBO].[File]"
      
            # Check to see if a csv already exists, INSERT DELTA SYNC STUFF HERE

            if (!(Test-Path -Path "$csvRoot\$db.csv")) {

                # Run query and output results to a DataTable
                Write-Log "Getting file data from workspace $db" "INFO"
                try { $dataSet = Invoke-Sqlcmd -ServerInstance $SQLHost -Credential $cred -Query $sqlQuery -OutputAs DataTables }
                catch { Write-Log "Unable to gather file table data from workspace $db.  Check your SQL server host and credentials" "ERROR"}
     
                # Check that the table is not empty (ensures that only workspaces containing files are migrated) and write the csv
            
                try {
                    if ($dataSet.Rows.Count -gt 0) {
                        $dataSet | Export-Csv $csvRoot\$db.csv -NoTypeInformation
                    }
                    else {
                        Write-Log "Workspace $db has no records in the file table, csv will not be created" "WARN"
                    }
                    if (Test-Path -Path "$csvRoot\$db.csv") {
                        Write-Log "Csv for workspace $db successfully created at $csvRoot\$db.csv" "INFO"
                    }
                }
                catch { Write-Log "Unable to export workspace $db dataset to csv" "WARN" }

            }
            else { 
                Write-Log "Csv file for workspace $db already exists, checking for new files..." "INFO" 
                $check = $slcon.CreateCommand()
                $check.CommandText = "select * from MigrationData where CaseID = '$db'"
                $adapter = New-Object -TypeName System.Data.SQLite.SQLiteDataAdapter $check
                $table = New-Object System.Data.DataTable
                try { [void]$adapter.Fill($table) }
                catch {
                    Write-Log "Unable to get the last document artifact id of workspace ID $db" "ERROR"
                }
                $lastDocID = $table.LastDocumentID

                $sqlQuery = "SELECT Location AS ""source-path"", 
                                    '$targetPath' AS ""target-path"", 
                                    Filename AS ""targetfilename"", 
                                    DocumentArtifactID FROM [$db].[EDDSDBO].[File]
                             WHERE DocumentArtifactID>'$lastDocID'"

                try { $dataSet = Invoke-Sqlcmd -ServerInstance $SQLHost -Credential $cred -Query $sqlQuery -OutputAs DataTables }
                catch { Write-Log "Unable to gather file table data from workspace $db.  Check your SQL server host and credentials" "ERROR"}
                
                try {
                    if ($dataSet.Rows.Count -gt 0) {
                        $lastDocQuery = "SELECT MAX(DocumentArtifactID) from [$db].[EDDSDBO].[File]"
                        try { $lastDoc = Invoke-Sqlcmd -ServerInstance $SQLHost -Credential $cred -Query $lastDocQuery -ErrorVariable $sqlError}
                        catch {
                            Write-Log "Couldn't get the last document artifact ID from [$db].[EDDSDBO].[File] `r`n $sqlError" "WARN"
                        }
                        $d = $lastDoc.Column1
                        $dataSet | Export-Csv "$csvRoot\$db-delta-$d.csv" -NoTypeInformation
                    }
                    else {
                        Write-Log "Workspace $db has no new file records, delta csv will not be generated" "WARN"
                    }
                    if (Test-Path -Path "$csvRoot\$db-delta-$d.csv") {
                        Write-Log "New file were found.  Delta Csv for workspace $db successfully created at $csvRoot\$db-delta-$d.csv" "INFO"
                    }
                }
                catch { Write-Log "Unable to create delta csv for workspace $db dataset" "WARN" }
            }

        }
    }

    function Report { <# Function for creating report from sqlite#> }

    if ($Sync.IsPresent) { Sync }

    if ($Transfer.IsPresent) { Transfer }

    if ($Report.IsPresent) { Report }
}
