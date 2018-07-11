Function R1FileMigration {
    [CmdletBinding()]
    param (
        [parameter(ParameterSetName='Sync')]
        [Switch]$Sync,
        [parameter(ParameterSetName='Sync',Mandatory=$true)]
        [parameter(ParameterSetName='Transfer',Mandatory=$true)]
        [String]$SQLHostName,
        [parameter(ParameterSetName='Sync',Mandatory=$false)]
        [parameter(ParameterSetName='Transfer',Mandatory=$false)]
        [Switch]$Workspace,
        [parameter(ParameterSetName='Transfer',Mandatory = $false)]
        [Switch]$Transfer,
        [parameter(ParameterSetName='Transfer',Mandatory=$true)]
        [String]$targetPath,
        [parameter(ParameterSetName='Report')]
        [Switch]$Report
        
    )

    $csvRoot = "$PSScriptRoot\Csv"
    $dbRoot = "$PSScriptRoot\Db"
    $reportRoot = "$PSScriptRoot\Report"

    # Basic log settings
    $logFile = "$PSScriptRoot\applog.log"
    $logLevel = "DEBUG" # ("DEBUG","INFO","WARN","ERROR","FATAL")
    $logSize = 1mb # 30kb
    $logCount = 10
    $Stamp = (Get-Date).toString("yyyy/MM/dd HH:mm:ss:fff")

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
                $files = Get-ChildItem $filedir | Where-Object{$_.name -like "$fn*"} | Sort-Object lastwritetime 
                #this gets the fullname of the file we started with 
                $filefullname = $file.fullname
                #$logcount +=1 #add one to the count as the base file is one more than the count 
                for ($i = ($files.count); $i -gt 0; $i--) 
                {  
                    #[int]$fileNumber = ($f).name.Trim($file.name) #gets the current number of 
                    # the file we are on 
                    $files = Get-ChildItem $filedir | Where-Object{$_.name -like "$fn*"} | Sort-Object lastwritetime 
                    $operatingFile = $files | Where-Object{($_.name).trim($fn) -eq $i} 
                    if ($operatingfile) 
                    {$operatingFilenumber = ($files | Where-Object{($_.name).trim($fn) -eq $i}).name.trim($fn)} 
                    else 
                    {$operatingFilenumber = $null} 
    
                    if(($operatingFilenumber -eq $null) -and ($i -ne 1) -and ($i -lt $logcount)) 
                    { 
                        $operatingFilenumber = $i 
                        $newfilename = "$filefullname.$operatingFilenumber" 
                        $operatingFile = $files | Where-Object{($_.name).trim($fn) -eq ($i-1)} 
                        write-host "moving to $newfilename" 
                        move-item ($operatingFile.FullName) -Destination $newfilename -Force 
                    } 
                    elseif($i -ge $logcount) 
                    { 
                        if($operatingFilenumber -eq $null) 
                        {  
                            $operatingFilenumber = $i - 1 
                            $operatingFile = $files | Where-Object{($_.name).trim($fn) -eq $operatingFilenumber} 
                            
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
                        $operatingFile = $files | Where-Object{($_.name).trim($fn) -eq ($i-1)} 
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
    # Only prep SQL settings if Sync or Transfer are called

    if($Sync.IsPresent -OR $Transfer.IsPresent) {
        if (!(Get-Module sqlserver)) {
            try {
                Import-Module sqlserver -Force -ErrorAction Stop
            } catch {
                Write-Log "could not load sqlserver module, check the powershell log" "ERROR"
                Break
            }
        }

        $cred = Get-Credential
        # Only get selected workspaces if the workspace switch is set
        foreach ($db in $dbs) {
            $dbName += $dbs.Name
        }
        if ($Workspace.IsPresent) {
            Add-Type -AssemblyName System.Windows.Forms
            Add-Type -AssemblyName System.Drawing

            $form = New-Object System.Windows.Forms.Form
            $form.Text = 'Data Entry Form'
            $form.Size = New-Object System.Drawing.Size(300,800)
            $form.StartPosition = 'CenterScreen'

            $OKButton = New-Object System.Windows.Forms.Button
            $OKButton.Location = New-Object System.Drawing.Point(75,720)
            $OKButton.Size = New-Object System.Drawing.Size(75,23)
            $OKButton.Text = 'OK'
            $OKButton.DialogResult = [System.Windows.Forms.DialogResult]::OK
            $form.AcceptButton = $OKButton
            $form.Controls.Add($OKButton)

            $CancelButton = New-Object System.Windows.Forms.Button
            $CancelButton.Location = New-Object System.Drawing.Point(150,720)
            $CancelButton.Size = New-Object System.Drawing.Size(75,23)
            $CancelButton.Text = 'Cancel'
            $CancelButton.DialogResult = [System.Windows.Forms.DialogResult]::Cancel
            $form.CancelButton = $CancelButton
            $form.Controls.Add($CancelButton)

            $label = New-Object System.Windows.Forms.Label
            $label.Location = New-Object System.Drawing.Point(10,20)
            $label.Size = New-Object System.Drawing.Size(280,20)
            $label.Text = 'Please make a selection from the list below:'
            $form.Controls.Add($label)

            $listBox = New-Object System.Windows.Forms.Listbox
            $listBox.Location = New-Object System.Drawing.Point(10,40)
            $listBox.Size = New-Object System.Drawing.Size(260,20)

            $listBox.SelectionMode = 'MultiExtended'

            foreach ($db in $dbName) {
                [void] $listBox.Items.Add($db)
            }

            $listBox.Height = 650
            $form.Controls.Add($listBox)
            $form.Topmost = $true

            $result = $form.ShowDialog()

            if ($result -eq [System.Windows.Forms.DialogResult]::OK) {
                $dbs = $listBox.SelectedItems
            }
            else {
                Break
            }
        }
        else {
            # Get a list of all EDDS******* databases in the SQL instance
            try { $dbs = Get-SqlDatabase -ServerInstance $SQLHostName -Credential $cred | Where-Object { $_.Name -match 'EDD\D\d{7}$' } -ErrorAction Stop }
            catch {
                Write-Log "Unable to get the list of databases, check your SQL server host and credentials" "ERROR"
                Break
            }
        }
        Write-Log "Successfully connected to SQL and gathered the list of databases" "INFO"
        
    }
    
    function Sync { 
        
        # Function for syncing sql database to sqlite
       
        foreach ($db in $dbs) {

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
            
            # Trim the database name down to match CaseArtifactID
            $dbID = $db.TrimStart("E","D","S")
            
            #SQL queries to gather data for import into SQLite
            $statusQuery = "SELECT StatusName  FROM (
                                SELECT CaseArtifactID,
                                       timestamp,
                                       StatusName,
                                       ROW_NUMBER() OVER (PARTITION BY CaseArtifactID ORDER BY timestamp DESC) col
                                FROM [EDDS].[eddsdbo].[CaseStatisticsStatus]) x
                            WHERE x.col = 1
                            AND CaseArtifactID = $dbID"
            $fileQuery = "SELECT COUNT(*) FROM [$db].[EDDSDBO].[File]"
            $sizeQuery = "SELECT SUM(Size) FROM [$db].[EDDSDBO].[File]"
            $lastDocQuery = "SELECT MAX(DocumentArtifactID) from [$db].[EDDSDBO].[File]"
            
            # Get the current number of rows in the database file table
            try { $fileCount = Invoke-Sqlcmd -ServerInstance $SQLHostName -Credential $cred -Query $fileQuery -ErrorVariable $sqlError}
            catch {
                Write-Log "Couldn't get the total file count from [$db].[EDDSDBO].[File] `r`n $sqlError" "WARN"
            }
            
            # Check that the database file table actually has data in it (No need to migrate empty cases)
            if ($fileCount.Column1 -gt 0) {
                
                # Get the case status
                try { $status = Invoke-Sqlcmd -ServerInstance $SQLHostName -Credential $cred -Query $statusQuery -ErrorVariable $sqlError}
                catch {
                    Write-Log "Couldn't get the case Status from [EDDS].[eddsdbo].[CaseStatisticsStatus] `r`n $sqlError" "WARN"
                }
                # Get the total size of all data in the file table
                try { $fileSize = Invoke-Sqlcmd -ServerInstance $SQLHostName -Credential $cred -Query $sizeQuery -ErrorVariable $sqlError}
                catch {
                    Write-Log "Couldn't get the total file size from [$db].[EDDSDBO].[File] `r`n $sqlError" "WARN"
                }
                # Get the artifact ID of the most recent file in the table (for delta sync)
                try { $lastDoc = Invoke-Sqlcmd -ServerInstance $SQLHostName -Credential $cred -Query $lastDocQuery -ErrorVariable $sqlError}
                catch {
                    Write-Log "Couldn't get the last document artifact ID from [$db].[EDDSDBO].[File] `r`n $sqlError" "WARN"
                }
    
                $f = $fileCount.Column1; $s = $fileSize.Column1; $d = $lastDoc.Column1; $st = $status.StatusName
                
                # Check if this is the first import into SQLite
                if ($table.CaseID -ne $db) {
                    #First time sync
                    Write-Host "Adding $db to Migration DB :: Total Files: $f  Total File Size: $s (b)  Last document ID: $d"
                    Write-Log "Adding $db to Migration DB :: Total Files:$f  Total File Size:$s(bytes)  Last document ID:$d" "INFO"
                    
                    $insert = $slcon.CreateCommand()
                    $insert.CommandText = "INSERT INTO MigrationData (CaseID, Priority, TotalFiles, TotalBytes, LastDocumentID) 
                                          VALUES ('$db', '$st', '$f', '$s', '$d')"
                    try { $insert.ExecuteNonQuery() }
                    catch {
                        Write-Log "Unable to sync data for workspace ID $db to the local database" "WARN"
                    }
                }
                # Run a delta sync and update only the necessary fields
                else {
                    #Delta sync
                    Write-Host "Updating $db in Migration DB :: Total Files: $f  Total File Size: $s (b)  Last document ID: $d"
                    Write-Log "Updating $db in Migration DB :: Total Files:$f  Total File Size:$s(bytes)  Last document ID:$d" "INFO"

                    $insert = $slcon.CreateCommand()
                    $insert.CommandText = "UPDATE MigrationData 
                                           SET TotalFiles = '$f',
                                               Priority = '$st',
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

        New-Item -Path "$PSScriptRoot\Csv\Active" -ItemType Directory -ErrorAction SilentlyContinue
        New-Item -Path "$PSScriptRoot\Csv\Inactive" -ItemType Directory -ErrorAction SilentlyContinue

        # Function for creating csv files
        foreach ($db in $dbs) {

            # Create a SQL query that gathers the necessary data from file table in each database
            $sqlQuery = "SELECT Location AS ""source-path"", 
                                '$targetPath' AS ""target-path"", 
                                Filename AS ""targetfilename"", 
                                DocumentArtifactID 
                        FROM [$db].[EDDSDBO].[File]"

            # Get the case priority from SQLite
            $priorityQuery = $slcon.CreateCommand()
            $priorityQuery.CommandText = "SELECT Priority 
                                         FROM MigrationData
                                         WHERE CaseID = '$db'"
            $adapter = New-Object -TypeName System.Data.SQLite.SQLiteDataAdapter $priorityQuery
            $priority = New-Object System.Data.DataTable
            # Dump the Priority into a powershell DataTable
            try { [void]$adapter.Fill($priority) }
            catch {
                Write-Log "Unable to get Priotity for $db from the SQLite table 'MigrationData'" "ERROR"
            }
      
            # Check to see if a csv already exists
            if (!(Test-Path -Path "$csvRoot\Active\$db.csv") -AND !(Test-Path -Path "$csvRoot\Inactive\$db.csv")) {
                # Run query and output results to a DataTable
                Write-Log "Getting file data from workspace $db" "INFO"
                try { $dataSet = Invoke-Sqlcmd -ServerInstance $SQLHostName -Credential $cred -Query $sqlQuery -OutputAs DataTables }
                catch { Write-Log "Unable to gather file table data from workspace $db.  Check your SQL server host and credentials" "ERROR"}
     
                # Check that the table is not empty (ensures that only workspaces containing files are migrated) and write the csv
                try {
                    if ($dataSet.Rows.Count -gt 0) {
                        if ($priority.Priority -eq 'Active') {
                            $dataSet | Export-Csv "$csvRoot\Active\$db.csv" -NoTypeInformation
                        }
                        elseif($priority.Priority -eq 'Inactive') {
                            $dataSet | Export-Csv "$csvRoot\Inactive\$db.csv" -NoTypeInformation
                        }
                        else {
                            $dataSet | Export-Csv "$csvRoot\$db - NO STATUS.csv" -NoTypeInformation
                        }
                    }
                    else {
                        Write-Log "Workspace $db has no records in the file table, csv will not be created" "WARN"
                    }
                   if ((Test-Path -Path "$csvRoot\Active\$db.csv") -OR (Test-Path -Path "$csvRoot\Active\$db.csv")) {
                        Write-Log "Csv for workspace $db successfully created" "INFO"
                   }
                }
                catch { Write-Log "Unable to export workspace $db dataset to csv" "WARN" }

            }
            # Perform a delta sync, create new csv labelled "delta"
            else { 
                Write-Log "Csv file for workspace $db already exists, checking for new files..." "INFO" 
                $check = $slcon.CreateCommand()
                $check.CommandText = "select * from MigrationData where CaseID = '$db'"
                $adapter = New-Object -TypeName System.Data.SQLite.SQLiteDataAdapter $check
                $table = New-Object System.Data.DataTable
                # Get the last document artifact ID from SQLite
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

                # Get all new document information since the last csv
                try { $dataSet = Invoke-Sqlcmd -ServerInstance $SQLHostName -Credential $cred -Query $sqlQuery -OutputAs DataTables }
                catch { Write-Log "Unable to gather file table data from workspace $db.  Check your SQL server host and credentials" "ERROR"}
                
                try {
                    if ($dataSet.Rows.Count -gt 0) {
                        $lastDocQuery = "SELECT MAX(DocumentArtifactID) from [$db].[EDDSDBO].[File]"
                        # Get the new max document artifact ID
                        try { $lastDoc = Invoke-Sqlcmd -ServerInstance $SQLHostName -Credential $cred -Query $lastDocQuery -ErrorVariable $sqlError}
                        catch {
                            Write-Log "Couldn't get the last document artifact ID from [$db].[EDDSDBO].[File] `r`n $sqlError" "WARN"
                        }
                        $d = $lastDoc.Column1

                        # Update SQLite with the new last document artifact ID
                        $insert = $slcon.CreateCommand()
                        $insert.CommandText = "UPDATE MigrationData 
                                              SET LastDocumentID = '$d'
                                              WHERE CaseID = '$db'"
                        try { $insert.ExecuteNonQuery() }
                        catch {
                            Write-Log "Unable to update document ID for workspace ID $db in the local database" "WARN"
                        }
                        
                        # Export the delta sync dataset to csv
                        if ($priority.Priority -eq 'Active') {
                            $dataSet | Export-Csv "$csvRoot\Active\$db-delta-$d.csv" -NoTypeInformation
                        }
                        elseif($priority.Priority -eq 'Inactive') {
                            $dataSet | Export-Csv "$csvRoot\Inactive\$db-delta-$d.csv" -NoTypeInformation
                        }
                        else {
                            $dataSet | Export-Csv "$csvRoot\$db-delta-$d - NO STATUS.csv" -NoTypeInformation
                        }
                    }
                    else {
                        Write-Log "Workspace $db has no new file records, delta csv will not be generated" "WARN"
                    }
                    if ((Test-Path -Path "$csvRoot\Active\$db-delta-$d.csv") -OR (Test-Path -Path "$csvRoot\Inactive\$db-delta-$d.csv")) {
                        Write-Log "New file were found.  Delta Csv for workspace $db successfully created" "INFO"
                    }
                }
                catch { Write-Log "Unable to create delta csv for workspace $db dataset" "WARN" }
            }

        }
    }

    function Report { 
        
        #Function for creating a report from sqlite

            $reportQuery = $slcon.CreateCommand()
            $reportQuery.CommandText = "SELECT * FROM MigrationData"
            $adapter = New-Object -TypeName System.Data.SQLite.SQLiteDataAdapter $reportQuery
            $report = New-Object System.Data.DataTable
            # Dump the MigrationData SQLite table into a powershell DataTable
            try { [void]$adapter.Fill($report) }
            catch {
                Write-Log "Unable to get data from the SQLite table 'MigrationData'" "ERROR"
                Break
            }

        #Export the DataTable to csv in the Reports folder
        $report | Export-Csv "$reportRoot\Migration Report - $Stamp.csv" -NoTypeInformation
    }

    if ($Sync.IsPresent) { Sync }

    if ($Transfer.IsPresent) { Transfer }

    if ($Report.IsPresent) { Report }
}