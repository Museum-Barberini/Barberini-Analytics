<#
    Test Runner for Power BI report tests
    This script will try to load each Power BI report found in the repository
    and detect whether Power BI can load it. If any error messages are found or
    the loading times out, a failure is detected and reported. Additionally, all
    opened Power BI windows are screenshotted and stored under output/test_pbi.
#>

if ($env:CI) {
    . "tests/pbi_reports/_utils/Write-Progress-Stdout.ps1"
}

$csharpProvider = (&"tests/pbi_reports/_utils/csharp_provider.ps1")[-1]
$drawingLib = "nuget\NETStandard.Library.2.0.3\build\netstandard2.0\ref\System.Drawing.dll"
Add-Type -CodeDomProvider $csharpProvider -ReferencedAssemblies ([System.Reflection.Assembly]::LoadFrom($drawingLib)) -TypeDefinition (Get-Content -Path tests/pbi_reports/test_pbi_reports.cs | Out-String)
if (!$?) {
    exit 2
}

# Settings
$pbi = "${env:LOCALAPPDATA}\Microsoft\WindowsApps\PBIDesktopStore.exe"
$timeout = [timespan]::FromSeconds(300)
$interval = [timespan]::FromSeconds(10)
$loadDelay = [timespan]::FromSeconds(20)


$global:runs = $global:passes = $global:failures = $global:errors = 0

function Invoke-Test([MuseumBarberini.Analytics.Tests.PbiReportTestCase]$test) {
    Write-Progress -Id 2 -Activity "Testing report" -CurrentOperation "Opening report file"
    $test.Start()

    try {
        $startTime = Get-Date
        for ($i = 1;; $i++) {
            $elapsed = (Get-Date) - ($startTime)
            if ($elapsed -gt $timeout) {
                Write-Error "⚠ TIMEOUT: $test"
                $global:errors++
                return
            }

            Write-Progress -Id 2 -Activity "Testing report" -CurrentOperation "Waiting for report file to load... ($i)"
            Start-Sleep -Seconds $interval.Seconds
            $test.Check()

            if ($test.HasPassed) {
                Write-Output "✅ PASS: $test"
                $global:passes++
                return
            } elseif ($test.HasFailed) {
                Write-Error "❌ FAILED: $test"
                if ($test.ResultReason) {
                    Write-Error $test.ResultReason
                }
                $global:failures++
                return
            }
        }
    } finally {
        $test.SaveResults("output/test_pbi")
        Write-Progress -Id 2 -Activity "Testing report" -CurrentOperation "Closing report file"
        $test.Stop()
        Write-Progress -Id 2 -Completed "Testing report"
    }
}


# Prepare test cases
$reports = Get-ChildItem power_bi/*.pbit
$tests = $reports | ForEach-Object {[MuseumBarberini.Analytics.Tests.PbiReportTestCase]::new($_, $pbi, $loadDelay)}
mkdir -Force output/test_pbi | Out-Null

# Run tests
foreach ($test in $tests) {
    Write-Progress -Id 1 -Activity "Testing Power BI reports" -CurrentOperation $test -PercentComplete (($runs++).Length / $tests.Length)
    Invoke-Test $test
}
Write-Progress -Id 1 -Completed "Testing Power BI reports"

# Summary
Write-Output "Power BI Test summary: $passes passes, $failures failures, $errors errors."
$unknown = $runs - ($passes + $failures + $errors)
if ($unknown) {
    Write-Error "Warning: $unknown tests have an unknown result!"
}
$unsuccessful = $failures + $errors
Write-Output "Screenshots of all opened reports have been stored in output/test_pbi."

exit !!($unsuccessful)
