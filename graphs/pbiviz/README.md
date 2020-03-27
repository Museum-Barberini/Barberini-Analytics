# Sigma Text Graph
Custom Visual for Power BI

## Development

For general information on how to install `pbiviz` and how to build this, read [here](https://medium.com/@jatin7gupta/getting-started-with-power-bi-custom-visuals-59ce8d850feb). To debug this visual, execute `pbiviz start` and open Power BI Online. You will also need to turn on developer mode in Power BI Online.

## Known issues

### _Too many text values. Not all values are shown_

See [here](https://community.powerbi.com/t5/Custom-Visuals-Development/Text-Filter-is-limited-to-1000-values-exactly/td-p/383980). Looks as if `dataReductionAlgorithm` in the `capabilities.json` is not yet respected everywhere.

## Credits

### Icon

> <a target="_blank" href="https://icons8.com/icons/set/sigma">Sigma icon</a> icon by <a target="_blank" href="https://icons8.com">Icons8</a>
