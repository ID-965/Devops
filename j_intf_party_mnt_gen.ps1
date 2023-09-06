try {
	#Connection Details
    $serverName = "LI-LT662-HPG8"
    $databaseName = "tempdb"
	$username = "logic_temp"
	$password = "Logic_Wattyl321!"
    $tableName = "J_INTF_PARTY"
    $currentDate = Get-Date
    $dateStamp = $currentDate.ToString("ddMMyy.HHmmss")

	#Output file path
    $output1 = "D:\Loading_poc\MNT\Output\global.aup11.PARTY.99999.${dateStamp}.mnt"
	$output2 = "D:\Loading_poc\MNT\Output\global.nzp39.PARTY.99999.${dateStamp}.mnt"
	
$connectionString = "Data Source=$serverName;Initial Catalog=$databaseName;Integrated Security=SSPI;"
    # Create the SQL connection string=

    # Create the SQL connection
    $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
    $connection.Open()

    # Start transcript for logging
    $logFilePath = "D:\Loading_poc\MNT\Log\${tableName}_${dateStamp}.log"
    Start-Transcript -Path $logFilePath

    Write-Host "Script started."
	Write-Host "Connected to the: $serverName"

    # Retrieve the table1 data as delimited text
    $selectQuery1 = "select 'INSERT' Action_Code,
							'PARTY' Record_Identifier,
							PARTY_ID,
							CUST_ID,
							'CUSTOMER' PARTY_TYPE,
							null Salutation,
							FIRST_NAME,
							MIDDLE_NAME,
							LAST_NAME,
							null Suffix,
							null Gender,
							null Social_Security_Nbr,
							null Birth_Date,
							null Anniversary_Date,
							ORGANIZATION_NAME,
							'' Organization_Typcode,
							FEDERAL_TAX_ID,
							'' Blank1,
							'' Blank2,
							ADDRESS1,
							ADDRESS2,
							'' Apartment,
							CITY,
							STATE,
							POSTAL_CODE,
							COUNTRY,
							TELEPHONE_NUMBER,
							Null Telephone2,
							Null Telephone3,
							TELEPHONE4,
							EMAIL_ADDRESS,
							Null Sign_Up_Rtl_Loc_Id,
							null Allegiance_Rtl_Loc_Id,
							'' Blank3,
							'' Mailing_List_Flag,
							'' Employee_Id,
							COMMERCIAL_CUSTOMER_FLAG,
							'' Picture_Uri,
							'' State_Tax_ID,
							0 Email_Contact_Flag,
							0 Privacy_Card_Flag,
							'' Contact_Preference,
							0 Void_Flag,
							'' Field44,
							'' Field45,
							'' Field46,
							'' Field47,
							B2BPreferredLocale,
							''Field49,
							'' Field50,
							1 SwpActive,
							'' Field52,
							'' Field53,
							'' Field54,
							B2BPARENTCUSTOMERID ,
							B2BTENDERDEFAULT,
							B2BSalesRep1TerritoryCode,
							100 B2BSalesRep1Percentage,
							'' B2BSalesRep2TerritoryCode,
							null B2BSalesRep2Percentage,
							'' b2bSalesRep3TerritoryCode,
							null b2bSalesRep3Percentage,
							'' b2bSalesRep4TerritoryCode,
							null b2bSalesRep4Percentage,
							B2BPoRequired,
							0 B2BprojectPromptFlag,
							0 B2bProjectRequired,
							B2BPreferredLocale,
							1 B2BCustomerVolume,
							B2B_SUPPRESS_PRICE_FLAG B2BSuppressPrice,
							B2BCustomerTypeCode,
							B2BCustomerStatusCode,
							null B2BTitle,
							1 B2BNumberOfPainters,
							0 B2BPoReleaseAgreement,
							1 B2BSameShipToAddress,
							null B2BRemitToAddress,
							B2B_Credit_Limit_Type,
							SwpChannelCode,
							SwpProfileClass,
							SwpClassification,
							SwpBusinessPurpose,
							0 SwpSameAsBillTo,
							0 swpCustAccountId,
							0 B2BGSACustomer,
							DIRECT_SHIP_SALES_ORDER SWP_DIRECT_SHIP_FLAG FROM $tableName WHERE ORGANIZATION_ID='11'  AND MNT_IND is NULL"

    $command1 = New-Object System.Data.SqlClient.SqlCommand($selectQuery1, $connection)
    $reader1 = $command1.ExecuteReader()

    # Create an array to store the delimited data
    $data1 = @()

    # Process each row from the table
    while ($reader1.Read()) {
        $row1 = ""
        for ($i = 0; $i -lt $reader1.FieldCount; $i++) {
            $value = $reader1.GetValue($i)
            if ($value -is [bool]) {
                # Convert boolean values to "1" and "0"
                $value = [int]$value
            }
            $row1 += $value.ToString() + "|"
        }

        # Remove only the last pipe symbol from the row
        $row1 = $row1 -replace '\|$', ''  # Remove the last pipe symbol

        $data1 += $row1
    }

    $rowCount1 = $data1.Length

    # Close the reader and SQL command
    $reader1.Close()

    Write-Host "Retrieved the ${tableName}_1 data"

    # Retrieve the table2 data as delimited text
    $selectQuery2 = "SELECT distinct 'RUN_SQL|DELETE FROM crm_party_locale_information where party_id='''+convert(varchar(20),party_id)+''' and organization_id='''+convert(varchar(20),ORGANIZATION_ID)+''' and party_locale_seq=''1'' and address_type=''Home''' 
	FROM $tableName WHERE ORGANIZATION_ID='11'  AND MNT_IND is NULL"
    $command2 = New-Object System.Data.SqlClient.SqlCommand($selectQuery2, $connection)
    $reader2 = $command2.ExecuteReader()

    # Create an array to store the delimited data
    $data2 = @()

    # Process each row from the table
    while ($reader2.Read()) {
        $row2 = ""
        for ($i = 0; $i -lt $reader2.FieldCount; $i++) {
            $value = $reader2.GetValue($i)
            if ($value -is [bool]) {
                # Convert boolean values to "1" and "0"
                $value = [int]$value
            }
            $row2 += $value.ToString() + "|"
        }

        # Remove only the last pipe symbol from the row
        $row2 = $row2 -replace '\|$', ''  # Remove the last pipe symbol

        $data2 += $row2
    }

    $rowCount2 = $data2.Length

    # Close the reader and SQL command
    $reader2.Close()

	Write-Host "Retrieved the ${tableName}_2 data"

    $totalRowCount = $rowCount1 + $rowCount2

    # Create the writer for the output1 file
    $writer = [System.IO.File]::CreateText($output1)

    # Write the header line with the dynamic line count
    $header = "<Header line_count=`"$totalRowCount`" target_org_node=`"*:*`" download_time=`"IMMEDIATE`" apply_immediately=`"true`" />"
    $writer.WriteLine($header)

    # Write the delimited data from $data1
    foreach ($row1 in $data1) {
        $writer.WriteLine($row1)
    }

    # Write the delimited data from $data2
    foreach ($row2 in $data2) {
        $writer.WriteLine($row2)
    }

    # Close the writer and SQL connection
    $writer.Close()
  
	Write-Host "MNT File generated successfully."

########################################

    Write-Host "Script started."


    # Retrieve the table3 data as delimited text
    $selectQuery1 = "select 'INSERT' Action_Code,
							'PARTY' Record_Identifier,
							PARTY_ID,
							CUST_ID,
							'CUSTOMER' PARTY_TYPE,
							null Salutation,
							FIRST_NAME,
							MIDDLE_NAME,
							LAST_NAME,
							null Suffix,
							null Gender,
							null Social_Security_Nbr,
							null Birth_Date,
							null Anniversary_Date,
							ORGANIZATION_NAME,
							'' Organization_Typcode,
							FEDERAL_TAX_ID,
							'' Blank1,
							'' Blank2,
							ADDRESS1,
							ADDRESS2,
							'' Apartment,
							CITY,
							STATE,
							POSTAL_CODE,
							COUNTRY,
							TELEPHONE_NUMBER,
							Null Telephone2,
							Null Telephone3,
							TELEPHONE4,
							EMAIL_ADDRESS,
							Null Sign_Up_Rtl_Loc_Id,
							null Allegiance_Rtl_Loc_Id,
							'' Blank3,
							'' Mailing_List_Flag,
							'' Employee_Id,
							COMMERCIAL_CUSTOMER_FLAG,
							'' Picture_Uri,
							'' State_Tax_ID,
							0 Email_Contact_Flag,
							0 Privacy_Card_Flag,
							'' Contact_Preference,
							0 Void_Flag,
							'' Field44,
							'' Field45,
							'' Field46,
							'' Field47,
							B2BPreferredLocale,
							''Field49,
							'' Field50,
							1 SwpActive,
							'' Field52,
							'' Field53,
							'' Field54,
							B2BPARENTCUSTOMERID ,
							B2BTENDERDEFAULT,
							B2BSalesRep1TerritoryCode,
							100 B2BSalesRep1Percentage,
							'' B2BSalesRep2TerritoryCode,
							null B2BSalesRep2Percentage,
							'' b2bSalesRep3TerritoryCode,
							null b2bSalesRep3Percentage,
							'' b2bSalesRep4TerritoryCode,
							null b2bSalesRep4Percentage,
							B2BPoRequired,
							0 B2BprojectPromptFlag,
							0 B2bProjectRequired,
							B2BPreferredLocale,
							1 B2BCustomerVolume,
							B2B_SUPPRESS_PRICE_FLAG B2BSuppressPrice,
							B2BCustomerTypeCode,
							B2BCustomerStatusCode,
							null B2BTitle,
							1 B2BNumberOfPainters,
							0 B2BPoReleaseAgreement,
							1 B2BSameShipToAddress,
							null B2BRemitToAddress,
							B2B_Credit_Limit_Type,
							SwpChannelCode,
							SwpProfileClass,
							SwpClassification,
							SwpBusinessPurpose,
							0 SwpSameAsBillTo,
							0 swpCustAccountId,
							0 B2BGSACustomer,
							DIRECT_SHIP_SALES_ORDER SWP_DIRECT_SHIP_FLAG FROM $tableName WHERE ORGANIZATION_ID='39' AND MNT_IND is NULL"

    $command1 = New-Object System.Data.SqlClient.SqlCommand($selectQuery1, $connection)
    $reader1 = $command1.ExecuteReader()

    # Create an array to store the delimited data
    $data1 = @()

    # Process each row from the table
    while ($reader1.Read()) {
        $row1 = ""
        for ($i = 0; $i -lt $reader1.FieldCount; $i++) {
            $value = $reader1.GetValue($i)
            if ($value -is [bool]) {
                # Convert boolean values to "1" and "0"
                $value = [int]$value
            }
            $row1 += $value.ToString() + "|"
        }

        # Remove only the last pipe symbol from the row
        $row1 = $row1 -replace '\|$', ''  # Remove the last pipe symbol

        $data1 += $row1
    }

    $rowCount1 = $data1.Length

    # Close the reader and SQL command
    $reader1.Close()

    Write-Host "Retrieved the ${tableName}_3 data"

    # Retrieve the table4 data as delimited text
    $selectQuery2 = "SELECT distinct 'RUN_SQL|DELETE FROM crm_party_locale_information where party_id='''+convert(varchar(20),party_id)+''' and organization_id='''+convert(varchar(20),ORGANIZATION_ID)+''' and party_locale_seq=''1'' and address_type=''Home''' 
	FROM $tableName WHERE ORGANIZATION_ID='39' AND MNT_IND is NULL"
    $command2 = New-Object System.Data.SqlClient.SqlCommand($selectQuery2, $connection)
    $reader2 = $command2.ExecuteReader()

    # Create an array to store the delimited data
    $data2 = @()

    # Process each row from the table
    while ($reader2.Read()) {
        $row2 = ""
        for ($i = 0; $i -lt $reader2.FieldCount; $i++) {
            $value = $reader2.GetValue($i)
            if ($value -is [bool]) {
                # Convert boolean values to "1" and "0"
                $value = [int]$value
            }
            $row2 += $value.ToString() + "|"
        }

        # Remove only the last pipe symbol from the row
        $row2 = $row2 -replace '\|$', ''  # Remove the last pipe symbol

        $data2 += $row2
    }

    $rowCount2 = $data2.Length

    # Close the reader and SQL command
    $reader2.Close()

	Write-Host "Retrieved the ${tableName}_4 data"

    $totalRowCount = $rowCount1 + $rowCount2

    # Create the writer for the output2 file
    $writer = [System.IO.File]::CreateText($output2)

    # Write the header line with the dynamic line count
    $header = "<Header line_count=`"$totalRowCount`" target_org_node=`"*:*`" download_time=`"IMMEDIATE`" apply_immediately=`"true`" />"
    $writer.WriteLine($header)

    # Write the delimited data from $data1
    foreach ($row1 in $data1) {
        $writer.WriteLine($row1)
    }

    # Write the delimited data from $data2
    foreach ($row2 in $data2) {
        $writer.WriteLine($row2)
    }

    # Close the writer and SQL connection
    $writer.Close()
  
	Write-Host "MNT File generated successfully."
	
######################################
	
	Write-Host "Updating MNT_IND and MNT_CREATE_DATE"
    # Update the MNT FLAG
    $updateQuery = "UPDATE $tableName SET MNT_IND = 'Y', MNT_CREATE_DATE = GETDATE() WHERE mnt_ind is null"
    $updateCommand = New-Object System.Data.SqlClient.SqlCommand($updateQuery, $connection)
    $rowsAffected = $updateCommand.ExecuteNonQuery()
    $updateCommand.Dispose()
	
	Write-Host "Rows updated: $rowsAffected."

}
catch {
    $errorMessage = $_.Exception.Message
    Write-Host "An error occurred: $errorMessage"
    # Write the error message to the error file
    $errorFilePath = "D:\Loading_poc\MNT\Error\${tableName}_error_${dateStamp}.txt"
    $errorMessage | Out-File -FilePath $errorFilePath
}
finally {
    # Clean up resources
    if ($reader1) { $reader1.Dispose() }
    if ($reader2) { $reader2.Dispose() }
    if ($writer) { $writer.Dispose() }
    if ($connection) { $connection.Dispose() }

    # Stop the transcript and save log to file
    Stop-Transcript
}
