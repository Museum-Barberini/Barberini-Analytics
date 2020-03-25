"use strict";

import "core-js/stable";
import "./../style/visual.less";
import powerbi from "powerbi-visuals-api";
import VisualConstructorOptions = powerbi.extensibility.visual.VisualConstructorOptions;
import VisualUpdateOptions = powerbi.extensibility.visual.VisualUpdateOptions;
import IVisual = powerbi.extensibility.visual.IVisual;
import DataView = powerbi.DataView;
import DataViewTable = powerbi.DataViewTable;

import {logExceptions} from "./utils/logExceptions";
import * as math from "mathjs";
import * as d3 from "d3";

// WHY WE CANNOT IMPORT SIGMA.JS AS DEFINED IN NPM
// The problem is that sigma is always imported as a function rather than as a module.
// This makes it impossible to access sigma.parsers.gexf reliably, for example.
// Maybe this is related to Power BI's hacks to alias this, self and window?
// See also here: https://github.com/jacomyal/sigma.js/issues/871#issuecomment-600577941
// And see also here: https://github.com/DefinitelyTyped/DefinitelyTyped/issues/34776


class Edge {
    
    constructor(node1: string, node2: string) {
        this._nodes = [node1, node2];
        this._nodes.sort();
    }
    
    private _nodes: [string, string];
    
    get nodes() {
        return new Set(this._nodes);
    }
    
    public key(): Symbol {
        return Symbol.for(JSON.stringify(this._nodes));
    }
}

export class Visual implements IVisual {
    
    private target: HTMLElement;
    private container: HTMLDivElement;
    
    private sigInst: any;
    
    private stopwords: string[];
    
    private readonly nodeSize = 800;
    
    constructor(options: VisualConstructorOptions) {
        console.log("Visual constructor", options, new Date().toLocaleString());
        this.target = options.element;
        
        this.container = document.createElement('div');
        this.container.className = 'sigma-expand';
        this.container.style.width = '100%';
        this.container.style.height = '100%';
        this.target.appendChild(this.container);
        
        this.sigInst = (sigma as any).init(this.container);
        this.sigInst.drawingProperties = {
            defaultLabelColor: '#f00',
            defaultLabelSize: 24,
            defaultLabelBGColor: '#eee',
            defaultLabelHoverColor: '#f00',
            labelThreshold: 0,
            drawLabels: true,
            defaultEdgeType: 'curve',
            edgeWeightInfluence: 0,
            minNodeSize: 0.5,
            maxNodeSize: 15,
            minEdgeSize: 0.3,
            maxEdgeSize: 1
        }
        this.sigInst.mouseProperties = {
            maxRatio: 4 // max zoom factor
        };
        
        this.stopwords = require('csv-loader!../static/stopwords.csv').map((row: string[]) => row[0]);
        this.stopwords.push("museumbarberini"); // TODO: Hardcoded thus ugly.
        // TODO: It would be nicer to specify this information as a table, but unfortunately,
        // Power BI does not yet support multiple distinct data view mappings.
        console.log("stopwords", this.stopwords);
        
        console.log("constructor done");
    }

    @logExceptions()
    public update(options: VisualUpdateOptions) {
        console.log('Visual update', options, new Date().toLocaleString());
        
        const dataView: DataView = options.dataViews[0];
        const tableDataView: DataViewTable = dataView.table;
        
        if (!tableDataView) {
            return;
        }
        
        console.log("tableDataView.rows", tableDataView.rows);
        
        let textsByCategory = this.groupBy(tableDataView.rows, row => String(row[1]), row => String(row[0]).toLowerCase());
        let categories = Array.from(textsByCategory.keys());
        console.log("allTexts", textsByCategory);
        let wordsByTextByCategory = this.mapMap(textsByCategory, texts =>
            new Map(texts.map(text =>
                [text,
                text
                    .split(/\s+/)
                    .map(word => this.trimString(word, /\p{P}/gu))
                    .filter(word => word)
                    .filter(word => !this.stopwords.includes(word))
                ]))
            );
        /** further cleansing ideas:
         * define custom ignore words/replacements (e. g., museumbarberini -> barberini)
         */
        console.log("wordsByTextByCategory", wordsByTextByCategory);
        
        let allWords = this.gather([...wordsByTextByCategory.values()].map(wordsByText => this.gather([...wordsByText.values()])));
        let histogram = this.histogram(allWords);
        console.log("histogram", histogram);
        let allWordsByCategory = new Map(
            [...wordsByTextByCategory.entries()].map(([category, wordsByText]) =>
                [category, this.gather([...wordsByText.values()])]
            )
        );
        let histogramsByCategory = new Map(
            [...allWordsByCategory.entries()].map(([category, allWords]) =>
                [category, this.histogram(allWords)]
            )
        );
        
        let hues = categories.map((category, index) => index / categories.length * 2 * Math.PI);
        
        // TODO: Ideally, we could update the graph differentially. This would require extra effort for dealing with the IDs.
        this.sigInst._core.graph.empty();
        
        let sum = Array.from(histogram.values()).reduce((s, v) => s + v, 0);
        let nodeIds = new Map();
        var nodeId = 0;
        histogram.forEach((count, word, _) => {
            nodeIds.set(word, ++nodeId);
            let saturations = categories.map(category => histogramsByCategory.get(category).get(word) ?? 0);
            let [hue, sat] = ((complex: math.Complex) => {let polar = complex.toPolar(); return [polar.phi, polar.r]})(
                this.zip(hues, saturations, (hue, sat) => math.complex({r: sat, phi: hue}))
                    .reduce((sum, next) => <math.Complex>math.add(sum, next), math.complex(0, 0)));
            let light = Math.pow(1 - count, 42);
            sat = Math.pow(sat, 0.1);
            sat /= Math.SQRT2 * categories.length;
            sat *= 3;
            console.log(word, sat);
            this.sigInst.addNode(nodeId, {
                x: 100 - 200*Math.random(),
                y: 100 - 200*Math.random(),
                id: nodeId,
                label: word,
                color: d3.hsl(hue / (2 * Math.PI) * 360, sat, light).hex(),
                size: count / sum * this.nodeSize,
                labelSize: count,
                attributes: []})});
        var edgeId = nodeId;
        let edgeWeights = new Map<Symbol, number>();
        wordsByTextByCategory.forEach(wordsByText =>
            wordsByText.forEach(words => {
                words.forEach((word1, index1) => words.forEach((word2, index2) => {
                    if (index1 >= index2) return;
                    if (word1 == word2) return;
                    let edge = new Edge(word1, word2).key();
                    edgeWeights.set(
                        edge,
                        this.getOrSetDefault(
                            edgeWeights,
                            edge,
                            () => 0) + (1 / (index2 - index1)));
            }))}));
        edgeWeights.forEach((weight, edge) => {
            let [word1, word2] = JSON.parse((<any>edge).description);
            this.sigInst.addEdge(++edgeId, nodeIds.get(word1), nodeIds.get(word2), {
                id: edgeId,
                source: nodeIds.get(word1),
                target: nodeIds.get(word2),
                attributes: [],
                weight: weight
            })
        });
        
        this.sigInst.startForceAtlas2();
        this.sigInst.stopForceAtlas2();
        
        
        // BIG MUD STARTING HERE AND NEVER ENDING
        {
const
    edgeFilterProportion = 0.025,
    maxEdgesCount = 20,
    minEdgesCount = 12,
    maxNodesCount = 40;


function applyFilters(sigInst, filters) {
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
	/*let weights = [];
	sigInst.iterEdges(edge => weights.push(edge.weight));
    weights.sort();
    
    let edgeLimit = weights.length - maxEdgesCount; //Math.min(weights.length - maxEdgesCount, Math.max(minEdgesCount, weights[~~(weights.length * edgeFilterProportion)]));
	let minWeight = weights[edgeLimit];
	let edges = new Map();
	sigInst
		.iterEdges(edge => {
			[edge.source, edge.target].forEach(node => {
				if (!edges.has(node))
					edges.set(node, []);
				edges.get(node).push(edge);
			});
			if (!edge.hidden) edge.hidden = edge.weight <= minWeight;
		})
		.iterNodes(node => {
			if (!node.hidden) node.hidden = !edges.has(node.id) || !edges.get(node.id) || edges.get(node.id).hidden;
        });*/
    
    let nodeSizes = [];
    sigInst.iterNodes(node => nodeSizes.push(node.size));
    nodeSizes.sort();
    let nodeLimit = nodeSizes.length - maxNodesCount;
    let minSize = nodeSizes[nodeLimit];
    
    sigInst
        .iterNodes(node => {
            if (!node.hidden) node.hidden = node.size <= minSize;
        })
        .iterEdges(edge => {
            if (!edge.hidden) edge.hidden = sigInst.getNodes(edge.source).hidden && sigInst.getNodes(edge.target).hidden;
        })
	
    sigInst.draw(2,2,2);
};

let hoverFilters = new Set();
let pinFilters = new Set();

function applyAllFilters(sigInst) {
	applyFilters(sigInst, new Set([...pinFilters, ...hoverFilters]));
}


this.sigInst
	.bind('overnodes', event => {
		event.content.forEach(hoverFilters.add, hoverFilters)
		applyAllFilters(this.sigInst)
	})
	.bind('outnodes', event => {
		event.content.forEach(hoverFilters.delete, hoverFilters);
		applyAllFilters(this.sigInst);
	})
	.bind('downnodes', event => {
		event.content.forEach(node => {
			if (!pinFilters.has(node)) {
                pinFilters.add(node);
                node.forceLabel = true;
			} else {
                pinFilters.delete(node);
                node.forceLabel = false;
			}
		});
		applyAllFilters(this.sigInst);
	});

applyAllFilters(this.sigInst);

        }
        
        this.sigInst.activateFishEye().draw();
        
        console.log("Update done");
    }
    
    private groupBy<T, K, V>(array: T[], funKey: (item: T) => K, funValue: (item: T) => V) {
        const map = new Map<K, V[]>();
        array.forEach(obj => {
            let group = this.getOrSetDefault(map, funKey(obj), () => []);
            group.push(funValue(obj));
        });
        return map;
    }
    
    private mapMap<K, V, U>(map: Map<K, V>, fun: (value: V) => U) {
        return new Map(Array.from(map, ([key, value]) => [key, fun(value)]));
    }
    
    private trimString(string: String, pattern: RegExp) {
        return string.replace(RegExp(`^${pattern.source}|${pattern.source}$`, pattern.flags), '');
    }
    
    private histogram<T>(values: T[]): Map<T, number> {
        let histogram = new Map<T, number>();
        values.forEach(value => {
            histogram.set(value, 
                (histogram.has(value)
                    ? histogram.get(value)
                    : 0) + 1);
        });
        histogram.forEach((count, key) => histogram.set(key, count / values.length));
        return histogram
    }
    
    private getOrSetDefault<K, V>(map: Map<K, V>, key: K, factory: () => V): V {
        if (map.has(key))
            return map.get(key);
        let value = factory();
        map.set(key, value);
        return value;
    }
    
    private gather<T>(array: T[][]): T[] {
        return [].concat.apply([], array);
    }
    
    private zip<T, U, V>(first: T[], second: U[], fun: (x: T, y: U) => V): V[] {
        return first.map((x, i) => fun(x, second[i]));
    }
    
}
