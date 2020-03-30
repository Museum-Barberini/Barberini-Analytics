# Sigma Text Graph
Custom Visual for Power BI

## Development

For general information on how to work with custom visuals, read [here](https://medium.com/@jatin7gupta/getting-started-with-power-bi-custom-visuals-59ce8d850feb). First of all, to develop custom visuals, you will need to install the Power BI visual tools:

```powershell
npm install -g powerbi-visuals-tools
```

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