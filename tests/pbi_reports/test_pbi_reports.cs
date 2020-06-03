using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using HWND = System.IntPtr;

namespace MuseumBarberini.Analytics.Tests
{
    public class PbiReportTestCase {
        /// <summary>
        /// The path to the report template file.
        /// </summary>
        public string Report { get; }

        /// <summary>
        /// The path to the PBI Desktop executable file.
        /// </summary>
        public string Desktop { get; }

        public bool HasPassed { get; private set; }

        public bool HasFailed { get; private set; }

        protected Process Process { get; private set; }
        
        protected PbiProcessSnapshot Snapshot { get; private set; }

        public PbiReportTestCase(string report, string desktop) {
            Report = report;
            Desktop = desktop;
        }

        public void Start() {
            Process = new Process {
                StartInfo = {
                    FileName = Desktop,
                    Arguments = $"\"{Report}\""
                }
            };
            Process.Start();
            
            Console.WriteLine("Am I printed?");
            Snapshot = new PbiProcessSnapshot(Process);
        }

        public void Check() {
            if (Process.HasExited) {
                HasFailed = true;
                return;
            }

            Snapshot.Update();

            var windows = Snapshot.Windows.ToList();
            Console.WriteLine("windows={0}", windows.Select(win => win.Title));
            if (windows.Count == 1 && windows[0].Title.EndsWith(" - Power BI Desktop"))
                HasPassed = true;
            else if (!windows.Any(window => string.IsNullOrWhiteSpace(window.Title)))
                HasFailed = true;
        }

        public void Stop() {
            Process.Kill();
        }

        public void SaveResults(string path) {
            Snapshot.SaveArtifacts(Path.Combine(path, Path.GetFileNameWithoutExtension(Report)));
        }
    }

    public class PbiProcessSnapshot {
        public PbiProcessSnapshot(Process process) {
            Process = process;
        }

        public Process Process { get; }

        public IEnumerable<PbiWindowSnapshot> Windows => _windows;
        
        private List<PbiWindowSnapshot> _windows;

        public void Update() {
            _windows = (
                from window in CollectWindows()
                where window.IsVisible
                where !window.Bounds.Equals(default(PbiWindowSnapshot.RECT))
                select window
            ).ToList();
        }

        public void SaveArtifacts(string path) {
            foreach (var (window, index) in Windows.Select((window, index) => (window, index))) {
                var label = new StringBuilder("window");
                label.AppendFormat("_{0}", index);
                if (!string.IsNullOrWhiteSpace(window.Title))
                    label.AppendFormat("_{0}", window.Title);
                label.Append(".png");
                
                var screenshot = window.RecordScreenshot();
                
                screenshot.Save(Path.Combine(path, label.ToString()));
            }
        }

        protected IEnumerable<PbiWindowSnapshot> CollectWindows() {
            foreach (var window in GetRootWindows(Process.Id))
                yield return PbiWindowSnapshot.Create(window);
        }

        private static IEnumerable<HWND> GetRootWindows(int pid)
        {
            var windows = GetChildWindows(HWND.Zero);
            foreach (var child in windows)
            {
                GetWindowThreadProcessId(child, out var lpdwProcessId);
                if (lpdwProcessId == pid)
                    yield return child;
            }
        }

        private static IEnumerable<HWND> GetChildWindows(HWND parent)
        {
            var result = new List<HWND>();
            var listHandle = GCHandle.Alloc(result);
            try
            {
                var childProc = new Win32Callback(EnumWindow);
                EnumChildWindows(parent, childProc, GCHandle.ToIntPtr(listHandle));
            }
            finally
            {
                if (listHandle.IsAllocated)
                    listHandle.Free();
            }
            return result;
        }

        private static bool EnumWindow(IntPtr handle, IntPtr pointer)
        {
            var gch = GCHandle.FromIntPtr(pointer);
            var list = (List<IntPtr>)gch.Target;
            list.Add(handle);
            return true;
        }

#region DllImports
        private delegate bool Win32Callback(HWND hwnd, IntPtr lParam);

        [DllImport(DllImportNames.USER32)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool EnumChildWindows(HWND parentHandle, Win32Callback callback, IntPtr lParam);

        [DllImport(DllImportNames.USER32)]
        private static extern uint GetWindowThreadProcessId(HWND hwnd, out uint lpdwProcessId);
#endregion DllImports
    }

    public class PbiWindowSnapshot {
        [StructLayout(LayoutKind.Sequential)]
        public struct RECT
        {
            public int Left;
            public int Top;
            public int Right;
            public int Bottom;
        }

        public PbiWindowSnapshot(HWND hwnd) {
            Hwnd = hwnd;
        }

        public HWND Hwnd { get; }
        
        public string Title { get; private set; }

        public bool IsVisible { get; private set; }

        public RECT Bounds { get; private set; }

        public static PbiWindowSnapshot Create(HWND hwnd) {
            var window = new PbiWindowSnapshot(hwnd);
            window.Update();
            return window;
        }

        public void Update() {
            Title = GetWindowTitle();
            IsVisible = IsWindowVisible(Hwnd);
            Bounds = GetWindowBounds();
        }

        public Bitmap RecordScreenshot() {
            var bmp = new Bitmap(Bounds.Right - Bounds.Left, Bounds.Bottom - Bounds.Top, PixelFormat.Format32bppArgb);
            using (var gfxBmp = Graphics.FromImage(bmp)) {
                IntPtr hdcBitmap;
                try
                {
                    hdcBitmap = gfxBmp.GetHdc();
                }
                catch
                {
                    return null;
                }
                bool succeeded = PrintWindow(Hwnd, hdcBitmap, 0);
                gfxBmp.ReleaseHdc(hdcBitmap);
                if (!succeeded)
                {
                    return null;
                }
                var hRgn = CreateRectRgn(0, 0, 0, 0);
                GetWindowRgn(Hwnd, hRgn);
                var region = Region.FromHrgn(hRgn);
                if (!region.IsEmpty(gfxBmp))
                {
                    gfxBmp.ExcludeClip(region);
                    gfxBmp.Clear(Color.Transparent);
                }
            }
            return bmp;
        }

        private RECT GetWindowBounds() {
            if (!GetWindowRect(new HandleRef(null, Hwnd), out var rect))
                throw new Win32Exception(Marshal.GetLastWin32Error());
            return rect;
        }

        private string GetWindowTitle()
        {
            var length = GetWindowTextLength(Hwnd);
            var title = new StringBuilder(length);
            GetWindowText(Hwnd, title, length + 1);
            return title.ToString();
        }

#region DllImports

        [DllImport(DllImportNames.GDI32)]
        private static extern IntPtr CreateRectRgn(int nLeftRect, int nTopRect, int nRightRect, int nBottomRect);

        [DllImport(DllImportNames.USER32)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GetWindowRect(HandleRef hwnd, out RECT lpRect);

        [DllImport(DllImportNames.USER32)]
        private static extern int GetWindowRgn(HWND hWnd, IntPtr hRgn);

        [DllImport(DllImportNames.USER32, CharSet = CharSet.Auto, SetLastError = true)]
        private static extern int GetWindowText(HWND hwnd, StringBuilder lpString, int nMaxCount);

        [DllImport(DllImportNames.USER32, SetLastError = true, CharSet = CharSet.Auto)]
        private static extern int GetWindowTextLength(HWND hwnd);

        [DllImport(DllImportNames.USER32)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool IsWindowVisible(IntPtr hwnd);

        [DllImport(DllImportNames.USER32, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool PrintWindow(HWND hwnd, IntPtr hDC, uint nFlags);
#endregion DllImports
    }

    internal static class DllImportNames {
        public const string GDI32 = "gdi32.dll";
        public const string USER32 = "user32.dll";
    }
}
