'use strict';

import './../style/visual.less';
import powerbi from 'powerbi-visuals-api';
import IVisual = powerbi.extensibility.visual.IVisual;
import VisualConstructorOptions = powerbi.extensibility.visual.VisualConstructorOptions;
import VisualUpdateOptions = powerbi.extensibility.visual.VisualUpdateOptions;
import DataView = powerbi.DataView;
import DataViewTable = powerbi.DataViewTable;
import DataViewTableRow = powerbi.DataViewTableRow;

import 'core-js/stable';
import * as math from 'mathjs';
import * as d3 from 'd3';

import './utils/arrayUtils';
import {histogram} from './histogram';
import {logExceptions} from './utils/logExceptions';
import * as mathUtils from './utils/mathUtils';
import './utils/mapUtils';
import * as sigmaUtils from './utils/sigmaUtils';
import './utils/stringUtils';

// WHY WE CANNOT IMPORT SIGMA.JS AS DEFINED IN NPM
// The problem is that sigma is always imported as a function rather than as a module.
// This makes it impossible to access sigma.parsers.gexf reliably, for example.
// Maybe this is related to Power BI's hacks to alias this, self and window?
// See also here: https://github.com/jacomyal/sigma.js/issues/871#issuecomment-600577941
// And see also here: https://github.com/DefinitelyTyped/DefinitelyTyped/issues/34776
import {SigmaV01} from './js/sigma';
const Sigma = sigma as any;


export class SigmaVisual implements IVisual {
    private stopwords: string[];
    
    //#region CONFIGURATION
    private readonly maxNodeCount = 40;
    //#endregion CONFIGURATION
    
    private placeholder: HTMLDivElement;
    private placeholderContent: HTMLElement;
    private container: HTMLDivElement;
    private sigma: SigmaV01.Sigma; // TODO: Write type definitions for our old version of sigma.js
    
    private readonly hoverFilters = new Set<string>();
    private readonly pinFilters = new Set<string>();
    
    constructor(options: VisualConstructorOptions) {
        console.log("🚀 Loading Sigma Text Graph ...", options, new Date().toLocaleString());
        
        this.stopwords = require('csv-loader!../static/stopwords.csv').map((row: string[]) => row[0]);
        this.stopwords.push("museumbarberini"); // TODO: Hardcoded thus ugly.
        // TODO: It would be nicer to specify this information as an input table, but unfortunately,
        // Power BI does not yet support multiple distinct data view mappings.
        
        this.initializeComponent(options.element);
        
        console.log("✅ Sigma Text Graph was sucessfully loaded!");
        // Power BI does not take any notice when an error occurs during execution.
        // If you don't see this message, you know that something has gone wrong ...
    }
    
    @logExceptions()
    public update(options: VisualUpdateOptions) {
        console.log('👂 Updating Sigma Text Graph ...', options, new Date().toLocaleString());
        
        let dataView: DataView;
        let table: DataViewTable;
        let rows: DataViewTableRow[];
        let hasData = true;
        
        if (!((dataView = options.dataViews[0]) && (table = dataView.table))) {
            hasData = false;
            this.placeholderContent.innerHTML = "Start by adding some data";
        } else if (!table.columns.some(column => column.roles['text'])) {
            hasData = false;
            this.placeholderContent.innerHTML = "Start by adding a measure";
        } else if (((rows = table.rows)?.length ?? 0) == 0) {
            hasData = false;
            this.placeholderContent.innerHTML = "The measure is empty";
        }
        
        this.placeholder.style.display = hasData ? 'none' : 'flex';
        this.container.style.display = hasData ? '' : 'none';
        if (!hasData) {
            console.log("❌ Aborting update of Sigma Text Graph, not enough data.");
            return;
        }
        
        sigmaUtils.clean(this.sigma);
        
        // TODO: Ideally, we could update the graph differentially. This would require extra effort for dealing with the IDs.
        const textsByCategory = rows.groupBy(row => String(row[1]), row => String(row[0]).toLowerCase());
        new GraphBuilder()
            .analyze(textsByCategory, word => !this.stopwords.includes(word))
            .generateColors()
            .buildGraph(this.sigma);
        
        this.pinFilters.clear(); // TODO: Keep old filters as possible
        this.hoverFilters.clear();
        this.applyAllFilters(false);
        this.sigma.startForceAtlas2();
        this.sigma.stopForceAtlas2(); // Disable this line to keep nodes dancing until hovered
        
        console.log("✅ Sigma Visualization was successfully updated");
    }
    
    private initializeComponent(parent: HTMLElement) {
        // Create placeholder (visible until valid data are passed)
        this.placeholder = document.createElement('div');
        this.placeholder.style.width = '100%';
        this.placeholder.style.height = '100%';
        this.placeholder.style.display = 'flex';
        this.placeholder.style.justifyContent = 'center';
        this.placeholder.style.alignItems = 'center';
        const placeholderChild = document.createElement('center')
        this.placeholder.appendChild(placeholderChild);
        placeholderChild.innerHTML = "<h1>¯\\_(ツ)_/¯</h1>";
        placeholderChild.appendChild(this.placeholderContent = document.createElement('p'));
        parent.appendChild(this.placeholder);
        
        // Create sigma container
        this.container = document.createElement('div');
        this.container.className = 'sigma-expand';
        this.container.style.width = '100%';
        this.container.style.height = '100%';
        this.container.style.display = ''; // hide
        parent.appendChild(this.container);
        
        // Create sigma instance
        this.sigma = Sigma.init(this.container);
        this.sigma.drawingProperties = {
            defaultLabelColor: '#f00',
            defaultLabelSize: 24,
            defaultLabelBGColor: '#eee',
            defaultLabelHoverColor: '#f00',
            labelThreshold: 0,
            drawLabels: true,
            defaultEdgeType: 'curve', // TODO: Does not work
            edgeWeightInfluence: 0,
            minNodeSize: 0.5,
            maxNodeSize: 15,
            minEdgeSize: 0.3,
            maxEdgeSize: 1
        }
        this.sigma.mouseProperties = {
            maxRatio: 4 // max zoom factor
        };
        
        this.sigma
            .bind('overnodes', event => this.enterNodes(event.content))
            .bind('outnodes', event => this.leaveNodes(event.content))
            .bind('downnodes', event => this.clickNodes(event.content));
        this.sigma.activateFishEye();
    }
    
    public applyAllFilters(display = true) {
        return this.applyFilters(
            new Set([
                ...this.pinFilters,
                ...this.hoverFilters
            ]), display);
    }
    
    public applyFilters(filters: Set<string>, display = true) {
        console.log("Current filters:", filters);
        
        if (filters.size) {
            // Show the full graph
            this.sigma
                .iterEdges(edge => edge.hidden = false)
                .iterNodes(node => node.hidden = false);
        } else {
            // Only show the subgraph of nodes that are adjacent to any node from the filter set
            const _filters = Array.from(filters);
            const neighbors = new Map();
            this.sigma
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
                    node.hidden = !_filters.every(filter =>
                        node.id == filter || (neighbors.has(node.id) && neighbors.get(node.id).has(filter)))
                });
        }
        
        // Limit number of visible nodes: Only show the largest ones (for sake of clarity)
        // TODO: Restore filtering by edge weight instead (see git log)! It is much more interesting!
        const nodeSizes = [];
        this.sigma.iterNodes(node => {if (!node.hidden) nodeSizes.push(node.size)});
        nodeSizes.sort((a, b) => a - b);
        const nodeLimit = nodeSizes.length - this.maxNodeCount;
        const minSize = nodeSizes[nodeLimit];
        
        this.sigma
            .iterNodes(node => {if (!node.hidden)
                node.hidden = node.size < minSize;
            })
            .iterEdges(edge => {if (!edge.hidden)
                edge.hidden = [edge.source, edge.target].every(node =>
                    this.sigma.getNodes(node).hidden);
            })
        
        console.log("Filtered nodes to:", (() => {const nodes = new Set(); this.sigma.iterNodes(n => {if (!n.hidden) nodes.add(n.label)}); return nodes;})());
        
        if (display) this.sigma.draw();
    };
    
    //#region EVENTHANDLING
    private enterNodes(nodeIds: string[]) {
        nodeIds.forEach(this.hoverFilters.add, this.hoverFilters)
        this.applyAllFilters()
    }
    
    private leaveNodes(nodeIds: string[]) {
        nodeIds.forEach(this.hoverFilters.delete, this.hoverFilters);
        this.applyAllFilters();
    }
    
    private clickNodes(nodes: string[]) {
        console.log('clickNodes', nodes);
        nodes.forEach(nodeId => {
            if (!this.pinFilters.has(nodeId)) {
                this.pinFilters.add(nodeId);
            } else {
                this.pinFilters.delete(nodeId);
                this.hoverFilters.delete(nodeId);
            }
            this.sigma.iterNodes(node => { if (node.id == nodeId) {
                node.forceLabel = this.pinFilters.has(nodeId);
                node.label = node.forceLabel ? node.label.toUpperCase() : node.label.toLowerCase();
            }});
        });
        this.applyAllFilters();
    }
    //#endregion EVENTHANDLING
}


class GraphBuilder {
    
    //#region CONFIGURATION
    private readonly nodeSize = 800;
    
    private readonly colorLightCoefficient = 42; // the higher the more nodes can be seen
    private readonly colorSaturationCoefficients = [
        0.1, // the higher the faster nodes get grey
        3 // the higher the more color is in the graph
    ];
    //#endregion CONFIGURATION
    
    categories: string[];
    wordsByTextByCategory: Map<string, Map<string, string[]>>;
    // The key of the following map is a Symbol which contains the JSON representation of an Edge instance.
    // This is the best possibility I found for using custom equality for the map's key comparison.
    edgeWeights: Map<Symbol, number>;
    histogram: Map<string, number>;
    histogramsByCategory: Map<string, Map<string, number>>;
    
    hues: number[] | null;
    
    public analyze(textsByCategory: Map<string, string[]>, wordFilter: (word: string) => boolean) {
        this.categories = Array.from(textsByCategory.keys());
        this.analyzeTexts(textsByCategory, wordFilter);
        this.analyzeWords();
        return this;
    }
    
    public generateColors() {
        if (this.categories.length < 2) {
            this.hues = null;
            return this;
        }
        this.hues = this.categories.map((_category, index) =>
            index / this.categories.length * (2 * Math.PI));
        return this;
    }
    
    public buildGraph(sigInst: SigmaV01.Sigma) {
        const nodeIds = new Map();
        let componentId = 0;
        
        this.histogram.forEach((count, word, _) => {
            nodeIds.set(word, ++componentId);
            sigInst.addNode(componentId, {
                id: componentId,
                label: word,
                
                size: count * this.nodeSize,
                labelSize: count,
                
                color: this.computeColor(word, count),
                
                // TODO: Unused potential here! Find a way to position elements on a useful way.
                x: 100 - 200 * Math.random(),
                y: 100 - 200 * Math.random(),
                
                forceLabel: false,
                hidden: false,
                attributes: []
            });
        });
        
        this.edgeWeights.forEach((weight, edge) => {
            const [word1, word2] = JSON.parse((<any>edge).description);
            sigInst.addEdge(++componentId, nodeIds.get(word1), nodeIds.get(word2), {
                id: componentId,
                source: nodeIds.get(word1),
                target: nodeIds.get(word2),
                
                weight: weight,
                
                hidden: false,
                attributes: []
            });
        });
        
        return this;
    }

    private analyzeTexts(textsByCategory: Map<string, string[]>, wordFilter: (word: string) => boolean) {
        this.wordsByTextByCategory = textsByCategory.mapEx(texts =>
            texts.mapEx(text =>
                text
                    .split(/\s+/)
                    .map(word => word.trimEx(/\p{P}/gu))
                    .filter(word => word)
                    .filter(wordFilter)
                    /** further cleansing ideas:
                     * define custom ignore words/replacements (e. g., museumbarberini -> barberini)
                     */
                )
            );
        console.log("wordsByTextByCategory", this.wordsByTextByCategory);
        return this;
    }
    
    private analyzeWords() {
        const allWords = [...this.wordsByTextByCategory.values()].fold(wordsByText => [...wordsByText.values()].flatten());
        this.histogram = histogram(allWords);
        console.log("histogram", this.histogram);
        const allWordsByCategory = this.wordsByTextByCategory.mapEx(wordsByText => [...wordsByText.values()].flatten());
        this.histogramsByCategory = allWordsByCategory.mapEx(histogram);
        
        const edgeWeights = new Map<Symbol, number>();
        this.wordsByTextByCategory.forEach(wordsByText =>
            wordsByText.forEach(words => {
                words.forEach((word1, index1) => words.forEach((word2, index2) => {
                    if (index1 >= index2) return;
                    if (word1 == word2) return;
                    const edge = new UndirectedEdge(word1, word2).key();
                    edgeWeights.update(edge, 0, weight =>
                        weight + (1 / (index2 - index1)));
                }))
            })
        );
        this.edgeWeights = edgeWeights;
        return this;
    }
    
    private computeColor(word: string, count: number) {
        const saturations = this.categories.map(category =>
            this.histogramsByCategory.get(category).get(word) ?? 0);
        let [hue, sat] = this.hues
            ? mathUtils.deconstructPolar(this.hues
                .zip(saturations, (hue, sat) => math.complex({r: sat, phi: hue}))
                .reduce((sum, next) => <math.Complex>math.add(sum, next), math.complex(0, 0)))
            : [0, 0];
        const light = Math.pow(1 - count, this.colorLightCoefficient);
        sat = Math.pow(sat, this.colorSaturationCoefficients[0]);
        sat /= Math.SQRT2 * this.categories.length;
        sat *= this.colorSaturationCoefficients[1];
        return d3.hsl(hue / (2 * Math.PI) * 360, sat, light).hex();
    }
}


class UndirectedEdge {
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
