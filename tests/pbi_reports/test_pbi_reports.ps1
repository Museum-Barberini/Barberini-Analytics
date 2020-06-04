$csharpProvider = &"tests/pbi_reports/csharp_provider.ps1"
$drawingLib = "nuget/System.Drawing.Common.4.7.0/lib/net461/System.Drawing.Common.dll"
Add-Type -CodeDomProvider $csharpProvider -ReferencedAssemblies ([System.Reflection.Assembly]::LoadFrom($drawingLib)) -TypeDefinition (Get-Content -Path tests/pbi_reports/test_pbi_reports.cs | Out-String)
if (!$?) {
    exit 2
}

# Settings
$pbi = "C:\Users\Christoph\AppData\Local\Microsoft\WindowsApps\PBIDesktopStore.exe"
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


$reports = Get-ChildItem power_bi/template.pbit #power_bi/*.pbit # testing
$tests = $reports | ForEach-Object {[MuseumBarberini.Analytics.Tests.PbiReportTestCase]::new($_, $pbi, $loadDelay)}
mkdir -Force output/test_pbi | Out-Null

foreach ($test in $tests) {
    Write-Progress -Id 1 -Activity "Testing Power BI reports" -CurrentOperation $test -PercentComplete (($runs++).Length / $tests.Length)
    Invoke-Test $test
}

Write-Progress -Id 1 -Completed "Testing Power BI reports"
Write-Output "Power BI Test summary: $passes passes, $failures failures, $errors errors."
$unknown = $runs - ($passes + $failures + $errors)
if ($unknown) {
    Write-Error "Warning: $unknown tests have an unknown result!"
}

exit !($failures + $errors)
