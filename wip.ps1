$pbi = "C:\Users\Christoph\AppData\Local\Microsoft\WindowsApps\PBIDesktopStore.exe"
$reports = Get-ChildItem power_bit/*.pbi?

foreach ($report in $reports) {
	$process = [Diagnostics.Process]::Start($pbi, $report)
	
}

Get-Process | Where-Object {$_.MainWindowTitle -match '.*'} | Select-Object MainWindowTitle

# next steps would include:
# - [api.apidef]::EnumerateProcessWindowHandles(24164)
# - .net get hwnd title
# - maybe write it all in csharp
# - loop for it, timeout 5 min
