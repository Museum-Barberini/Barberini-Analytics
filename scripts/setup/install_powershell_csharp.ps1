Remove-Item -Force -Recurse -ErrorAction Ignore nuget
mkdir nuget

Invoke-WebRequest https://dist.nuget.org/win-x86-commandline/latest/nuget.exe -OutFile nuget/Nuget.exe

&nuget/Nuget.exe install -OutputDirectory nuget Microsoft.CodeDom.Providers.DotNetCompilerPlatform -Version 2.0.1
&nuget/Nuget.exe install -OutputDirectory nuget System.Drawing.Common -Version 4.7.0
