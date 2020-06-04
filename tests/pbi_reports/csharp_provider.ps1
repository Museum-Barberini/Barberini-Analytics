#requires -Version 5

$DotNetCodeDomLocation = 'nuget/microsoft.codedom.providers.dotnetcompilerplatform.2.0.1'

if (-not (Test-Path $DotNetCodeDomLocation)) {
    &scripts/setup/install_powershell_csharp.ps1
}

Add-Type -Path "$DotNetCodeDomLocation\lib\net45\Microsoft.CodeDom.Providers.DotNetCompilerPlatform.dll"

# using Invoke-Expression moves this class definition to runtime, so it will work after the add-type and the ps5 class interface implementation will succeed
# This uses the public interface ICompilerSettings instead of the private class CompilerSettings
Invoke-Expression -Command @"
class RoslynCompilerSettings : Microsoft.CodeDom.Providers.DotNetCompilerPlatform.ICompilerSettings
{
    [string] get_CompilerFullPath()
    {
        return "$DotNetCodeDomLocation\tools\RoslynLatest\csc.exe"
    }
    [int] get_CompilerServerTimeToLive()
    {
        return 10
    }
}
"@
$DotNetCodeDomProvider = [Microsoft.CodeDom.Providers.DotNetCompilerPlatform.CSharpCodeProvider]::new([RoslynCompilerSettings]::new())

return $DotNetCodeDomProvider
