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

using ImageMagick;


namespace MuseumBarberini.Analytics.Tests
{
    /// <summary>
    /// Represents a test case for a Power BI report file.
    /// Purpose of the test will be to try to open and load the report
    /// and detect if any errors or deadlocks occur while doing so.
    /// </summary>
    public class PbiReportTestCase {
        /// <summary>
        /// The path to the report template file.
        /// </summary>
        public string Report { get; }

        /// <summary>
        /// The path to the PBI Desktop executable file.
        /// </summary>
        public string Desktop { get; }

        /// <summary>
        /// The delay Power BI is granted to hang after all data sources have been adapted
        /// before final error search starts.
        /// </summary>
        public TimeSpan LoadDelay { get; }

        public bool HasPassed { get; private set; }

        public bool HasFailed { get; private set; }

        public string ResultReason { get; private set; }

        protected static Dictionary<string, Bitmap> FailureIcons = Directory.GetFiles(
                Path.Combine(Directory.GetCurrentDirectory(), "tests/pbi_reports"),
                "pbi_failure*.bmp"
            ).ToDictionary(
                file => Path.GetFileNameWithoutExtension(file),
                file => new Bitmap(file)
            );

        protected Process Process { get; private set; }
        
        protected PbiProcessSnapshot Snapshot { get; private set; }
        
        public PbiReportTestCase(string report, string desktop, TimeSpan loadDelay) {
            Report = report;
            Desktop = desktop;
            LoadDelay = loadDelay;
        }

        /// <summary>
        /// Start this test.
        /// </summary>
        public void Start()
            => _logExceptions(() => {
                Process = new Process {
                    StartInfo = {
                        FileName = Desktop,
                        Arguments = $"\"{Report}\""
                    }
                };
                Process.Start();

                Snapshot = new PbiProcessSnapshot(Process);
            });

        /// <summary>
        /// Try to find indications for this test either having passed or failed.
        /// If an indication is found, HasPassed or HasFailed will be set accordingly.
        /// </summary>
        public void Check()
            => _logExceptions(() => {
                void handleFail(string reason) {
                    HasFailed = true;
                    ResultReason = reason;
                }
                _check(
                    handlePass: () => {
                        System.Threading.Thread.Sleep(LoadDelay);
                        _check(
                            handlePass: () => HasPassed = true,
                            handleFail: handleFail
                        );
                    },
                    handleFail: handleFail
                );
            });

        /// <summary>
        /// Abort this test.
        /// </summary>
        public void Stop()
            => _logExceptions(() => {
                Process.Kill();
            });

        /// <summary>
        /// Save the results of this test into <paramref name="path"/>.
        /// </summary>
        public void SaveResults(string path)
            => _logExceptions(() => {
                Snapshot.SaveArtifacts(Path.Combine(path, Path.GetFileNameWithoutExtension(Report)));
            });

        public override string ToString() {
            return $"PBI Report Test: {Path.GetFileName(Report)}";
        }

        private void _check(Action handlePass, Action<string> handleFail) {
            if (Process.HasExited) {
                handleFail("Power BI has unexpectedly terminated");
                return;
            }

            Snapshot.Update();

            var windows = Snapshot.Windows.ToList();
            if (windows.Count == 1 && windows[0].Title.EndsWith(" - Power BI Desktop")) {
                handlePass();
                return;
            }
            if (!windows.Any(window => string.IsNullOrWhiteSpace(window.Title))) {
                handleFail("Power BI did not open any valid window");
                return;
            }
            foreach (var (failure, icon) in FailureIcons.Select(kvp => (kvp.Key, kvp.Value)))
                foreach (var (window, index) in windows.Select((window, index) => (window, index)))
                    if (window.DisplaysIcon(icon, out var similarity))
                        handleFail($"Power BI showed an error in window {index} " +
                                   $"while loading the report: {failure} (similarity={similarity})");
        }

        /// <summary>
        /// Helper function that catches any exception and writes the stack trace to console
        /// before re-throwing the exception. This is helpful when running a C# script from
        /// PowerShell.
        /// </summary>
        /// <param name="action">The actual action to wrap.</param>
        private static void _logExceptions(Action action) {
            try {
                action();
            } catch (Exception ex) {
                Console.WriteLine(ex);
                throw;
            }
        }
    }

    /// <summary>
    /// Represents the state of an observed Power BI process at a certain point in time.
    /// </summary>
    public class PbiProcessSnapshot {
        public PbiProcessSnapshot(Process process) {
            Process = process;
        }

        public Process Process { get; }

        /// <summary>
        /// All windows that are currently opened by the process.
        /// </summary>
        public IEnumerable<PbiWindowSnapshot> Windows => _windows;
        
        private List<PbiWindowSnapshot> _windows;

        /// <summary>
        /// Update this snapshot.
        /// </summary>
        public void Update() {
            _windows = (
                from window in CollectWindows()
                where window.IsVisible
                where !window.Bounds.Equals(default(PbiWindowSnapshot.RECT))
                select window
            ).ToList();
        }

        /// <summary>
        /// Save all artifacts of this snapshot into <paramref name="path"/>.
        /// In detail, these are screenshots of all open windows.
        /// </summary>
        /// <param name="path"></param>
        public void SaveArtifacts(string path) {
            Directory.CreateDirectory(path);

            foreach (var (window, index) in Windows.Select((window, index) => (window, index))) {
                if (!window.IsVisible) continue;

                var label = new StringBuilder("window");
                label.AppendFormat("_{0}", index);
                if (!string.IsNullOrWhiteSpace(window.Title))
                    label.AppendFormat("_{0}", window.Title);
                label.Append(".png");

                var screenshot = window.Screenshot;

                screenshot?.Save(Path.Combine(path, label.ToString()));
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

    /// <summary>
    /// Represents the state of an observed Power BI window at a certain point in time.
    /// </summary>
    public class PbiWindowSnapshot {
        [StructLayout(LayoutKind.Sequential)]
        public struct RECT
        {
            public int Left;
            public int Top;
            public int Right;
            public int Bottom;
        }

        public PbiWindowSnapshot(HWND hwnd) : this() {
            Hwnd = hwnd;
        }

        private PbiWindowSnapshot() {
            _screenshot = new Lazy<Bitmap>(RecordScreenshot);
        }

        /// <summary>
        /// The window handle (hWnd) of this window.
        /// </summary>
        public HWND Hwnd { get; }

        public string Title { get; private set; }

        public bool IsVisible { get; private set; }

        public RECT Bounds { get; private set; }

        /// <summary>
        /// A screenshot of this window.
        /// </summary>
        /// <remarks>
        /// Will be generated lazilly.
        /// </remarks>
        public Bitmap Screenshot => _screenshot.Value;

        protected static double IconSimilarityThreshold = 0.07;

        private readonly Lazy<Bitmap> _screenshot;

        public static PbiWindowSnapshot Create(HWND hwnd) {
            var window = new PbiWindowSnapshot(hwnd);
            window.Update();
            return window;
        }

        /// <summary>
        /// Update this snapshot.
        /// </summary>
        public void Update() {
            Title = GetWindowTitle();
            IsVisible = IsWindowVisible(Hwnd);
            Bounds = GetWindowBounds();
        }

        /// <summary>
        /// Tests whether this window displays the specified icon.
        /// </summary>
        public bool DisplaysIcon(Bitmap icon, out double similarity) {
            var screenshot = Screenshot;
            if (screenshot is null) {
                similarity = default(double);
                return false;
            }

            var magickIcon = CreateMagickImage(icon);
            var magickScreenshot = CreateMagickImage(screenshot);
            var result = magickScreenshot.SubImageSearch(magickIcon);
            return (similarity = result.SimilarityMetric) <= IconSimilarityThreshold;
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

        private Bitmap RecordScreenshot() {
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

        private static Bitmap ConvertToPixelFormat(Bitmap bitmap, PixelFormat pixelFormat) {
            var newBitmap = new Bitmap(bitmap.Width, bitmap.Height, pixelFormat);

            using (var graphic = Graphics.FromImage(newBitmap))
            {
                graphic.DrawImage(bitmap, new Rectangle(0, 0, newBitmap.Width, newBitmap.Height));
            }

            return newBitmap;
        }

        /// <summary>
        /// Collect all positions in <paramref name="sourceBitmap"/> where <paramref name="searchingBitmap"/> is found.
        /// </summary>
        /// <remarks>
        /// Credits: https://stackoverflow.com/a/29333957. Edited.
        /// </remarks>
        private static IEnumerable<Point> FindBitmapsEntry(Bitmap sourceBitmap, Bitmap searchingBitmap)
        {
        #region Arguments check

            if (sourceBitmap == null || searchingBitmap == null)
                throw new ArgumentNullException();

            if (sourceBitmap.PixelFormat != searchingBitmap.PixelFormat)
                throw new ArgumentException("Pixel formats aren't equal");

            if (sourceBitmap.Width < searchingBitmap.Width || sourceBitmap.Height < searchingBitmap.Height)
                yield break; // Size of searchingBitmap bigger then sourceBitmap"

        #endregion

            var pixelFormatSize = Image.GetPixelFormatSize(sourceBitmap.PixelFormat) / 8;


            // Copy sourceBitmap to byte array
            var sourceBitmapData = sourceBitmap.LockBits(
                new Rectangle(0, 0, sourceBitmap.Width, sourceBitmap.Height),
                ImageLockMode.ReadOnly, sourceBitmap.PixelFormat);
            var sourceBitmapBytesLength = sourceBitmapData.Stride * sourceBitmap.Height;
            var sourceBytes = new byte[sourceBitmapBytesLength];
            Marshal.Copy(sourceBitmapData.Scan0, sourceBytes, 0, sourceBitmapBytesLength);
            sourceBitmap.UnlockBits(sourceBitmapData);

            // Copy searchingBitmap to byte array
            var searchingBitmapData = searchingBitmap.LockBits(
                new Rectangle(0, 0, searchingBitmap.Width, searchingBitmap.Height),
                ImageLockMode.ReadOnly, searchingBitmap.PixelFormat);
            var searchingBitmapBytesLength = searchingBitmapData.Stride * searchingBitmap.Height;
            var searchingBytes = new byte[searchingBitmapBytesLength];
            Marshal.Copy(searchingBitmapData.Scan0, searchingBytes, 0, searchingBitmapBytesLength);
            searchingBitmap.UnlockBits(searchingBitmapData);

            var pointsList = new List<Point>();

            // Seqrching entries
            // minimizing searching zone
            // sourceBitmap.Height - searchingBitmap.Height + 1
            for (var mainY = 0; mainY < sourceBitmap.Height - searchingBitmap.Height + 1; mainY++)
            {
                var sourceY = mainY * sourceBitmapData.Stride;

                for (var mainX = 0; mainX < sourceBitmap.Width - searchingBitmap.Width + 1; mainX++)
                {
                    // mainY & mainX - pixel coordinates of sourceBitmap
                    // sourceY + sourceX = pointer in array sourceBitmap bytes
                    var sourceX = mainX * pixelFormatSize;

                    var isEqual = true;
                    for (var c = 0; c < pixelFormatSize; c++)
                    {
                        // through the bytes in pixel
                        if (sourceBytes[sourceX + sourceY + c] == searchingBytes[c])
                            continue;
                        isEqual = false;
                        break;
                    }

                    if (!isEqual)
                        continue;

                    var isStop = false;

                    // find fist equalation and now we go deeper) 
                    for (var secY = 0; secY < searchingBitmap.Height; secY++)
                    {
                        var searchY = secY * searchingBitmapData.Stride;

                        var sourceSecY = (mainY + secY) * sourceBitmapData.Stride;

                        for (var secX = 0; secX < searchingBitmap.Width; secX++)
                        {
                            // secX & secY - coordinates of searchingBitmap
                            // searchX + searchY = pointer in array searchingBitmap bytes

                            var searchX = secX * pixelFormatSize;

                            var sourceSecX = (mainX + secX) * pixelFormatSize;

                            for (var c = 0; c < pixelFormatSize; c++)
                            {
                                // through the bytes in pixel
                                if (sourceBytes[sourceSecX + sourceSecY + c] == searchingBytes[searchX + searchY + c])
                                    continue;

                                // not equal - abort iteration
                                isStop = true;
                                break;
                            }

                            if (isStop)
                                break;
                        }

                        if (isStop)
                            break;
                    }

                    if (!isStop) // hit
                        yield return new Point(mainX, mainY);
                }
            }
        }

        private static MagickImage CreateMagickImage(Bitmap bitmap) {
            var magickImage = new MagickImage();
            using (var stream = new MemoryStream())
            {
                bitmap.Save(stream, ImageFormat.Bmp);

                stream.Position = 0;
                magickImage.Read(stream);
            }

            const double scaleFactor = 1 / 3.0; // For performance
            magickImage.Resize(
                (int)Math.Round(magickImage.Width * scaleFactor),
                (int)Math.Round(magickImage.Height * scaleFactor)
            );
            return magickImage;
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
