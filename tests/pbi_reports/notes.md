## puzzle pieces:
- [x] start pbi with report file
- [x] find all hwnds of process
- [x] get bounds, visibility, and title of hwnd
- [x] take screenshot of hwnd
- [x] check screenshot is black

## the plan:
- foreach report:
  - open pbi
  - wait X seconds (X=10?)
  - find all visible windows with title, bounds, and screenshot
    - don't make black screenshot of course
	- skip: `rc.Equals(default(RECT))`
  - if there is only main window, success
  - otherwise, wait even longer
  - on timeout error
  - fixtures:
    - all recorded screenshots as `test_pbi/my_report/{index}[_{title}].png`
- modularization:
  - powershell: test runner, does loop (`power_bi/test_pbi_reports.ps1`/`Test-PBI-Reports`)
  - c#: test case, per report check (`power_bi/test_pbi_reports.cs`/`PbiReportTestCase`, interface: )


## todo:
- console logging funktioniert nicht!
- completeness checks funktionieren nicht!
- tostring for testcase
