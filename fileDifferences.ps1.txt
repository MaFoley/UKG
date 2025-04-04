
# Import CSV files
$File1Contents = Import-Csv -Path "Time.csv"
$File2Contents = Import-Csv -Path "Time - Copy.csv"
 
# Compare the CSV files
$ComparisonResult = Compare-Object -ReferenceObject $File1Contents -DifferenceObject $File2Contents -Property ID,EmpId,WorkDate
 
# Output the comparison results
echo $comparisonResult
