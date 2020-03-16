var isVisible = true;
var isPinned = false;

var oldPin;

var com_active = new Array();
var active_communities = 0;
var visible_nodes = new Array();



// Bind events :
sigInst.bind('overnodes', function (event) {
	var nodes = event.content;
	var neighbors = {};

	sigInst.iterEdges(function (e) {

		if (nodes.indexOf(e.source) >= 0 || nodes.indexOf(e.target) >= 0) {
			neighbors[e.source] = 1;
			neighbors[e.target] = 1;

		}
	}).iterNodes(function (n) {
		if (!neighbors[n.id]) {
			n.hidden = 1;
		} else {
			n.hidden = 0;
		}
	}).draw(2, 2, 2);
}).bind('outnodes', function (event) {

	var nodes = event.content;

	if (isVisible && !isPinned) {

		if (active_communities > 0) {
			sigInst.iterNodes(function (n) {

				if ($.inArray(parseInt(n.id), visible_nodes) > -1) {

					n.hidden = 0;
				} else {

					n.hidden = 1;
				}
			}).draw(2, 2, 2);
		}
		else {
			sigInst.iterEdges(function (e) {
				e.hidden = 0;
			}).iterNodes(function (n) {
				n.hidden = 0;
			}).draw(2, 2, 2);
		}

	}


	else if (!isVisible && !isPinned) {
		sigInst.iterNodes(function (n) {
				n.hidden = 1;
		}).draw(2, 2, 2);


	} else if (nodes != oldPin) {


		var neighbors = {};

		sigInst.iterEdges(function (e) {

			if (oldPin.indexOf(e.source) >= 0 || oldPin.indexOf(e.target) >= 0) {
				neighbors[e.source] = 1;
				neighbors[e.target] = 1;

			}
		}).iterNodes(function (n) {
			if (!neighbors[n.id]) {
				n.hidden = 1;
			} else {
				n.hidden = 0;
			}
		}).draw(2, 2, 2);

	}


});