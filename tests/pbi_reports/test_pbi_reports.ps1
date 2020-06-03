$csharpProvider = &"tests/pbi_reports/csharp_provider.ps1"
$drawingLib = "C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.0\System.Drawing.dll"
Add-Type -CodeDomProvider $csharpProvider -ReferencedAssemblies ([System.Reflection.Assembly]::LoadFrom($drawingLib)) -TypeDefinition (Get-Content -Path tests/pbi_reports/test_pbi_reports.cs | Out-String)
if (!$?) {
    exit 2
}

# Settings
$pbi = "C:\Users\Christoph\AppData\Local\Microsoft\WindowsApps\PBIDesktopStore.exe"
$timeout = [timespan]::FromSeconds(300)
$interval = [timespan]::FromSeconds(10)


$runs = $passes = $failures = $errors = 0

function Invoke-Test([MuseumBarberini.Analytics.Tests.PbiReportTestCase]$test) {
    Write-Progress -Id 2 -Activity "Testing report" -CurrentOperation "Opening report file"
    $test.Start()

    try {
        $startTime = Get-Date
        for ($i = 1;; $i++) {
            $elapsed = (Get-Date) - ($startTime)
            if ($elapsed -gt $timeout) {
                Write-Error "⚠ TIMEOUT: $test"
                $errors++
                return
            }

            Write-Progress -Id 2 -Activity "Testing report" -CurrentOperation "Waiting for report file to load... ($i)"
            Start-Sleep -Seconds $interval.Seconds
            $test.Check

            if ($test.HasPassed) {
                Write-Output "✅ PASS: $test"
                $passes++
                return
            } elseif ($test.HasFailed) {
                Write-Error "❌ FAILED:"
                $failures++
                return
            }
        }
    } finally {
        $test.SaveResults("test_pbi")
        Write-Progress -Id 2 -Activity "Testing report" -CurrentOperation "Closing report file"
        $test.Stop()
        Write-Progress -Id 2 -Completed "Testing report"
    }
}


$reports = Get-ChildItem power_bi/*.pbi?
$tests = $reports | ForEach-Object {[MuseumBarberini.Analytics.Tests.PbiReportTestCase]::new($_, $pbi)}

foreach ($test in $tests) {
    Write-Progress -Id 1 -Activity "Testing Power BI reports" -CurrentOperation $test -PercentComplete (($runs++).Length / $tests.Length)
    Invoke-Test $test
}

Write-Progress -Id 1 -Completed "Testing Power BI reports"
Write-Output "Power BI Test summary: $passes passes, $failures failures, $errors errors."

exit !!($failures + $errors)
