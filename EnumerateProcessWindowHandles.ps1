$TypeDef = @"

using System;
using System.Diagnostics;
using System.Text;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace Api
{

 public class WinStruct
 {
   public string WinTitle {get; set; }
   public int WinHwnd { get; set; }
 }

 public class ApiDef
 {
	delegate bool EnumThreadDelegate(IntPtr hWnd, IntPtr lParam);


	[DllImport("user32.dll")]
	static extern bool EnumThreadWindows(int dwThreadId, EnumThreadDelegate lpfn,
    	IntPtr lParam);
	
	public static IEnumerable<IntPtr> EnumerateProcessWindowHandles(int processId)
	{
		var handles = new List<IntPtr>();

		foreach (ProcessThread thread in Process.GetProcessById(processId).Threads)
			EnumThreadWindows(thread.Id, 
				(hWnd, lParam) => { handles.Add(hWnd); return true; }, IntPtr.Zero);

		return handles;
	}
 }
}
"@

Add-Type -TypeDefinition $TypeDef -Language CSharpVersion3
