export module SigmaV01 {
	interface Sigma {
		drawingProperties: Object;
		mouseProperties: Object;
		_core: any;
		addEdge(id: (string | number), sourceId: string, targetId: string, node: Edge);
		addNode(id: (string | number), node: Node);
		bind(event: string, callback: (e: any) => void): Sigma;
		draw(): void;
		getNodes(id: string): Node,
		iterEdges(callback: (edge: Edge) => void): Sigma;
		iterNodes(callback: (node: Node) => void): Sigma;
		
		activateFishEye(): void;
		
		startForceAtlas2(): void;
		stopForceAtlas2(): void;
	}
	
	interface GraphComponent {
		attributes: [];
		hidden: boolean;
		id: (number | string);
	}
	
	interface Edge extends GraphComponent {
		source: string, target: string;
		weight: number;
	}
	
	interface Node extends GraphComponent {
		color: string;
		forceLabel: boolean;
		hidden: boolean;
		label: string;
		labelSize: number;
		size: number;
		x: number, y: number;
	}
}
