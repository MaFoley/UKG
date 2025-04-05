param ($directory=".\DataFiles", $logdir=".",[string[]]$properties=@(
    'Id',
    'Code'
    )
)

$sharepath  = $logdir
$username   = $env:USERNAME
$hostname   = hostname
$version    = $PSVersionTable.PSVersion.ToString()
$datetime   = Get-Date -f 'yyyyMMddHHmmss'
$filename   = "Transcript-${username}-${hostname}-${version}-${datetime}.txt"
$logfile = (Join-Path -Path $sharepath -ChildPath $filename).ToString()

Start-Transcript -Path $logfile -Append

write-host "Begin comparison script"
$files = Get-ChildItem -Attributes !Directory -Path $directory
$latest2 = $files | Sort-Object -Descending -Property LastWriteTime | Select-Object -first 2
Format-Table -InputObject $latest2 -Property Name, LastWriteTime, Extension, Length

$objects = @{
    ReferenceObject = (Import-Csv -path $latest2[1])
    DifferenceObject = (Import-csv -path $latest2[0])
}
Write-Host Begin difference log:
Compare-Object -PassThru @objects -Property $properties
Write-Host End difference log:
Stop-Transcript
