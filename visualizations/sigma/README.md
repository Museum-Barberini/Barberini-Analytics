# Sigma Text Graph
Custom Visual for Power BI

## Development

For general information on how to work with custom visuals, read [the MS Docs article about it](https://docs.microsoft.com/de-de/power-bi/developer/visuals/custom-visual-develop-tutorial).

### Installation

1. First of all, to develop custom visuals, you will need to install the Power BI visual tools on Windows:

   ```powershell
   npm install -g powerbi-visuals-tools
   ```

2. To install all dependencies for this visual, run:

   ```powershell
   npm install
   ```

3. To be able to use the visual, you need to install the certificates. [This doc](https://docs.microsoft.com/de-de/power-bi/developer/visuals/custom-visual-develop-tutorial#creating-and-installing-a-certificate) describes in detail how you can do it.

### Building the visual

```powershell
pbiviz package
```

### Debugging the visual

1. Start the webpack server:

   ```powershell
   pbiviz start
   ```

2. Open Power BI Online on the same machine.
3. Turn on developer mode in the web app's settings.
4. Edit any report and add the "Custom Visual" from the visualization report.

## Known issues

### _Too many text values. Not all values are shown_

See [here](https://community.powerbi.com/t5/Custom-Visuals-Development/Text-Filter-is-limited-to-1000-values-exactly/td-p/383980). Looks as if `dataReductionAlgorithm` in the `capabilities.json` is not yet respected everywhere.

## Credits

### Icon

> <a target="_blank" href="https://icons8.com/icons/set/sigma">Sigma icon</a> icon by <a target="_blank" href="https://icons8.com">Icons8</a>
