Invoke-WebRequest https://dist.nuget.org/win-x86-commandline/latest/nuget.exe -OutFile Nuget.exe

&.\nuget.exe install Microsoft.CodeDom.Providers.DotNetCompilerPlatform -Version 2.0.1
