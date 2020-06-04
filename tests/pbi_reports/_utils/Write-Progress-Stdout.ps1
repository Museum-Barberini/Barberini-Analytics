function Write-Progress {
    [cmdletbinding()]
    param(
        [Parameter(Position = 0)]
        [string]$Activity,
        [Parameter(Position = 1)]
        [string]$Status,
        [Parameter(Position = 2)]
        [ValidateRange(0, [int]::MaxValue)]
        [int]$Id = 0,
        [int]$PercentComplete,
        [int]$SecondsRemaining,
        [string]$CurrentOperation,
        [int]$ParentId = -1,
        [int]$SourceId,
        [switch]$Completed
    )

    $change = $false
    
    $change = $change -or $Activity -ne $global:latestActivity
    if ($Activity -and $change) {
        $global:latestActivity = $Activity
        Write-Host "$Activity"
    }

    $counters = @()
    if ($Status) {
        $counters += $Status
    }
    if ($Position) {
        $counters += "$Position"
        if ($MaxValue) {
            $counters[-1] += "/$MaxValue"
        }
    }
    if ($PercentComplete) {
        $counters += "$PercentComplete%"
    }
    $change = $change -or $counters -ne $global:latestCounters
    if ($counters -and $change) {
        $global:latestCounters = $counters
        $first, $rest = $counters
        Write-Host -NoNewline $first
        if ($rest) {
            Write-Host -NoNewline " " ($rest -join ", ")
        }
        Write-Host
    }
    
    if ($CurrentOperation) {
        Write-Host "$CurrentOperation"
    }

    if ($Completed) {
        Write-Host "Done."
    }
}
