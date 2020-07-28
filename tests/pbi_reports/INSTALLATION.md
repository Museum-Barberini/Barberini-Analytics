# Installation of GitLab Runner for PBI tests

## Software

- Set up a virtual machine with Windows 10
- Install git: https://git-scm.com/download/win
- Install gitlab-runner: https://docs.gitlab.com/runner/install/windows.html
- Install Power BI Desktop from Windows Store

## Configuration

- Use the `gitlab-runner` CLI to connect the runner to the repo
- Set PowerShell execution policy (in an administrative shell):
  ```powershell
  Set-ExecutionPolicy Unrestricted
  ```
- Manually clone the repo and open any report manually. Type in database username and password when requested.
- To enable the automatical GitLab runner, **don't register it as a Windows service** but start a PowerShell session from conhost and leave this window opened permanently. This is necessary to load the visual environment for the UI tests.
