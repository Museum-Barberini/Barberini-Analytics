Remove-Item -Force -Recurse nuget
mkdir nuget

Invoke-WebRequest https://dist.nuget.org/win-x86-commandline/latest/nuget.exe -OutFile nuget/Nuget.exe

&nuget/Nuget.exe install Microsoft.CodeDom.Providers.DotNetCompilerPlatform -Version 2.0.1 -OutputDirectory nuget

# https://www.microsoft.com/de-DE/download/details.aspx?id=48145
# &nuget/Nuget.exe install Tesseract -Version 3.0.2 -OutputDirectory nuget