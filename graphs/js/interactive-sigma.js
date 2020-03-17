var isVisible = true;
var isPinned = false;

var oldPin;

var com_active = new Array();
var active_communities = 0;
var visible_nodes = new Array();


function applyFilters(filters) {
	if (filters.size == 0) {
		sigInst.iterNodes(node => node.hidden = false);
	} else {
		filters = Array.from(filters);
		var neighbors = new Map();
		sigInst
			.iterEdges(edge => {
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
