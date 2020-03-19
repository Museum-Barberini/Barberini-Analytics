var isVisible = true;
var isPinned = false;

var oldPin;

var com_active = new Array();
var active_communities = 0;
var visible_nodes = new Array();

const
	edgeFilterProportion = 0.25,
	minEdgesCount = 12;


function applyFilters(filters) {
	if (filters.size == 0) {
		sigInst
			.iterEdges(edge => edge.hidden = false)
			.iterNodes(node => node.hidden = false);
	} else {
		filters = Array.from(filters);
		var neighbors = new Map();
		sigInst
			.iterEdges(edge => {
				edge.hidden = false;
				if (!neighbors.has(edge.source))
					neighbors.set(edge.source, new Set());
				if (!neighbors.has(edge.target))
					neighbors.set(edge.target, new Set());
				neighbors.get(edge.source).add(edge.target);
				neighbors.get(edge.target).add(edge.source);
			})
			.iterNodes(node => {
				node.hidden = !filters.every(filter =>
					node.id == filter || (neighbors.has(node.id) && neighbors.get(node.id).has(filter)))
			});
	}
	
	// Limit number of visible edges
	weights = [];
	sigInst.iterEdges(edge => weights.push(edge.weight));
	weights.sort();
	minWeight = Math.max(weights[~~(weights.length * edgeFilterProportion)], minEdgesCount);
	edges = new Map();
	sigInst
		.iterEdges(edge => {
			[edge.source, edge.target].forEach(node => {
				if (!edges.has(node))
					edges.set(node, []);
				edges.get(node).push(edge);
			});
			if (!edge.hidden) edge.hidden = edge.weight < minWeight;
		})
		.iterNodes(node => {
			if (!node.hidden) node.hidden = !edges.has(node.id);
		});
	
	sigInst.draw(2,2,2);
};

hoverFilters = new Set();
pinFilters = new Set();

function applyAllFilters() {
	applyFilters(new Set([...pinFilters, ...hoverFilters]));
}


sigInst
	.bind('overnodes', event => {
		event.content.forEach(hoverFilters.add, hoverFilters)
		applyAllFilters()
	})
	.bind('outnodes', event => {
		event.content.forEach(hoverFilters.delete, hoverFilters);
		applyAllFilters();
	})
	.bind('downnodes', event => {
		event.content.forEach(node => {
			if (!pinFilters.has(node)) {
				pinFilters.add(node);
			} else {
				pinFilters.delete(node);
			}
		});
		applyAllFilters();
	});

applyAllFilters();
