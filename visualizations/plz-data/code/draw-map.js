async function drawMap() {
  
  	// 1. Access data
  	const bundeslandShapes   = await d3.json("./../bundeslaender_simplify20.geojson")
  	const dataset            = await d3.csv("./../agg_by_land.csv")
  	let plzShapes            = await d3.json("./../plz-geojson.json")
  	let plzData              = await d3.csv("./../agg_by_plz.csv")
  	const plzLandMapping     = await d3.csv("./../zuordnung_plz_ort.csv")

  	const mappingDict = {}
  	plzLandMapping.forEach(d => mappingDict[d.plz] = d.bundesland)

  	const shapesNameAccessor  = d => d.properties.GEN
  	const datasetNameAccessor = d => d.Bundesland

  	// Easy access to metrics later on
  	const gesamtCash = d3.sum(dataset, d => d.cash)

  	let metricNettoAbsolute = {}
  	let metricNettoRelative = {}
  	let metricBelege        = {}
  	dataset.forEach(d => {
  		name = datasetNameAccessor(d)
  		metricNettoAbsolute[name] = +d.cash
  		metricNettoRelative[name] = +d.cash / gesamtCash * 100
  		metricBelege[name]        = +d.Belege
  	})

  	// 2. Create chart dimensions
  	let dimensions = {
		width: 2000,
		height: 1000,
		margin: {
			top: 10,
			right: 10,
			bottom: 10,
			left: 10,
		},
	}
	dimensions.boundedWidth = dimensions.width
		- dimensions.margin.left
		- dimensions.margin.right

	const projection = d3.geoMercator()
		.fitWidth(1000, bundeslandShapes)
	const pathGenerator = d3.geoPath(projection)

	const [[x0, x1], [y0, y1]] = pathGenerator.bounds(bundeslandShapes)
	dimensions.boundedHeight = y1
	dimensions.height = dimensions.boundedHeight
		+ dimensions.margin.top
		+ dimensions.margin.bottom

	// 3. Draw canvas
	const wrapper = d3.select("#wrapper-states")
		.append("svg")
		.attr("width", dimensions.width)
		.attr("height", dimensions.height)

	const bounds = wrapper.append("g")

	const boundsPlz = wrapper.append("g")
		.style("transform", `translate(${
			dimensions.margin.left + 1000
		}px, ${
			265
		}px)`)

	const background = bounds.append("rect")
		.attr("width", "100%")
		.attr("height", "100%")
		.attr("fill", "lightgrey")

	// 4. Create scales
	const nettoAbsoluteValues = Object.values(metricNettoAbsolute)
	const nettoAbsoluteExtent = d3.extent(nettoAbsoluteValues)
	const colorScale = d3.scaleLinear()
		.domain([1, nettoAbsoluteExtent[1]])
		.range(["#E1F5FE", "#01579B"])

	// 5. Draw data
	const graticule = bounds.append("path")
		.attr("class", "graticule")
		.attr("d", pathGenerator(d3.geoGraticule10()))

	const states = bounds.selectAll(".states")
		.data(bundeslandShapes.features)
		.enter().append("path")
			.attr("class", "states")
			.attr("d", pathGenerator)
			.attr("fill", d => {
				const metricValue = metricNettoAbsolute[shapesNameAccessor(d)]
				if (typeof metricValue == "undefined") return "white"
				if (metricValue == 0) return "white"
				return colorScale(metricValue)
			})

	// 5. draw peripherals

	const boundsLegend = wrapper.append("g")
		.style("transform", `translateX(1000px)`)

	const legendBackground = boundsLegend.append("rect")
		.attr("y", 50)
		.attr("width", 655)
		.attr("height", 130)
		.attr("fill", "white")
	const title = boundsLegend.append("text")
		.attr("class", "title")
		.attr("x", 170)
		.attr("y", 95)
		.text("Netto Umsatz nach Gebieten")
	const legendTimeSpan = boundsLegend.append("text")
		.attr("class", "legend")
		.attr("x", 20)
		.attr("y", 130)
		.text(`Zeitraum: 20.06.19 bis 27.10.19 | Umsatz: ${
			d3.format(",.2f")(gesamtCash)
		} € | Belege: ${
			d3.format(",.0f")(d3.sum(Object.values(metricBelege)))
		}`)

	// 7. Set up interactions
	states
		.on("mouseenter", onMouseEnter)
		.on("mouseleave", onMouseLeave)
		.on("click", onClick)

	const tooltip = d3.select("#tooltip")
	
	function onMouseEnter(datum) {

		tooltip.style("opacity", 1)

		const name = shapesNameAccessor(datum)
		tooltip.select("#name")
			.text(name)
		tooltip.select("#netto-absolute")
			.text(`${d3.format(",.1f")(metricNettoAbsolute[name] || 0)} €`)
		tooltip.select("#netto-relative")
			.text(`${d3.format(",.2f")(metricNettoRelative[name] || 0)} %`)
		tooltip.select("#belege")
			.text(`${d3.format(",.0f")(metricBelege[name] || 0)}`)

		const [centerX, centerY] = pathGenerator.centroid(datum)
		tooltip.style("transform", `translate(`
			+ `calc( -50% + ${centerX + dimensions.margin.left}px),`
			+ `calc(-100% + ${centerY + dimensions.margin.top}px)`
			+ `)`)
	}

	function onMouseLeave() {
		tooltip.style("opacity", 0)
	}

	function onClick(datum) {
		console.log("clicked")
		const name = shapesNameAccessor(datum)
		drawPlzData(name, boundsPlz, plzShapes, plzData, mappingDict, gesamtCash)
	}
}

function drawPlzData(landName, boundsPlz, plzShapesInput, plzData, mappingDict, gesamtCash){
	
	// 0. start with a clean slate
	boundsPlz.selectAll("*").remove()

	// 1. Access data
  	const datasetPlzAccessor = d => d.plz
  	const shapesPlzAccessor  = d => d.properties.plz

  	let dataset   = plzData.filter(d => d.bundesland == landName)
  	const plzList = []
  	dataset.forEach(d => plzList.push(datasetPlzAccessor(d)))
  	let plzShapes = Object.assign({}, plzShapesInput)  // copy input
  	plzShapes.features = plzShapes.features.filter(d => mappingDict[shapesPlzAccessor(d)] == landName)

  	// Easy access to metrics later on
  	let metricNettoAbsolute = {}
  	let metricNettoRelative = {}
  	let metricBelege        = {}
  	dataset.forEach(d => {
  		plz = datasetPlzAccessor(d)
  		metricNettoAbsolute[plz] = +d.cash
  		metricNettoRelative[plz] = +d.cash / gesamtCash * 100
  		metricBelege[plz]        = +d.Belege
  	})

  	// 2. Create chart dimensions
  	let dimensions = {
		width: 1000,
		height: 1000,
		margin: {
			top: 10,
			right: 10,
			bottom: 10,
			left: 10,
		},
	}

	const projection = d3.geoMercator()
		.fitSize([950, 1000], plzShapes)
	const pathGenerator = d3.geoPath(projection)

	// 3. Draw canvas
	const bounds = boundsPlz

	const background = bounds.append("rect")
		.attr("width", "100%")
		.attr("height", "100%")
		.attr("fill", "lightgrey")

	// 4. Create scales
	const nettoAbsoluteValues = Object.values(metricNettoAbsolute)
	const nettoAbsoluteExtent = d3.extent(nettoAbsoluteValues)
	const colorScale = d3.scaleLinear()
		.domain([1, nettoAbsoluteExtent[1]])
		.range(["#E1F5FE", "#01579B"])

	// 5. Draw data

	const plzs = bounds.selectAll(".plz")
		.data(plzShapes.features)
		.enter().append("path")
			.attr("class", "plz")
			.attr("d", pathGenerator)
			.attr("fill", d => {
				const metricValue = metricNettoAbsolute[shapesPlzAccessor(d)]
				if (typeof metricValue == "undefined") return "white"
				if (metricValue == 0) return "white"
				return colorScale(metricValue)
			})


	// 5. draw peripherals


	// 7. Set up interactions
	plzs
		.on("mouseenter", onMouseEnter)
		.on("mouseleave", onMouseLeave)
	const tooltip = d3.select("#tooltip")
	
	function onMouseEnter(datum) {

		tooltip.style("opacity", 1)

		const plz = shapesPlzAccessor(datum)
		tooltip.select("#name")
			.text(datum.properties.note)
		tooltip.select("#netto-absolute")
			.text(`${d3.format(",.1f")(metricNettoAbsolute[plz] || 0)} €`)
		tooltip.select("#netto-relative")
			.text(`${d3.format(",.2f")(metricNettoRelative[plz] || 0)} %`)
		tooltip.select("#belege")
			.text(`${d3.format(",.0f")(metricBelege[plz] || 0)}`)

		const [centerX, centerY] = pathGenerator.centroid(datum)
		tooltip.style("transform", `translate(`
			+ `calc( -50% + ${centerX + dimensions.margin.left + 1000}px),`
			+ `calc(-100% + ${centerY + 260}px)`
			+ `)`)
	}

	function onMouseLeave() {
		tooltip.style("opacity", 0)
	}
}

drawMap()