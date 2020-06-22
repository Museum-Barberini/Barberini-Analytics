Remove-Item -Force -Recurse -ErrorAction Ignore nuget
mkdir nuget | Out-Null

# Install nuget
Invoke-WebRequest https://dist.nuget.org/win-x86-commandline/latest/nuget.exe -OutFile nuget/Nuget.exe

# Install modern C# compiler
&nuget/Nuget.exe install -OutputDirectory nuget Microsoft.CodeDom.Providers.DotNetCompilerPlatform -Version 2.0.1
# Install .NET Standard library
&nuget/Nuget.exe install -OutputDirectory nuget NETStandard.Library -Version 2.0.0

# Install MagickImage
&nuget/Nuget.exe install -OutputDirectory nuget Magick.NET-Q8-AnyCPU -Version 7.19.0
