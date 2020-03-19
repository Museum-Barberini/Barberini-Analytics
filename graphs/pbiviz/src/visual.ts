"use strict";

import "core-js/stable";
import "./../style/visual.less";
import powerbi from "powerbi-visuals-api";
import VisualConstructorOptions = powerbi.extensibility.visual.VisualConstructorOptions;
import VisualUpdateOptions = powerbi.extensibility.visual.VisualUpdateOptions;
import IVisual = powerbi.extensibility.visual.IVisual;
import EnumerateVisualObjectInstancesOptions = powerbi.EnumerateVisualObjectInstancesOptions;
import VisualObjectInstance = powerbi.VisualObjectInstance;
import DataView = powerbi.DataView;
import VisualObjectInstanceEnumerationObject = powerbi.VisualObjectInstanceEnumerationObject;
import IVisualHost = powerbi.extensibility.visual.IVisualHost;

type Selection<T extends d3.BaseType> = d3.Selection<T, any,any, any>;

//import 'sigma/build/plugins/sigma.parsers.json.require.js'; // file does not exist
//import '../node_modules/sigma/build/plugins/sigma.parsers.json.min.js'; // Browser: sigma is not declared, even if added to pbiviz.json/externalJS
import Sigma from 'sigma';
//const sigmaatlas = import('sigma/build/plugins/sigma.layout.forceAtlas2.min'); // Browser: ChunkLoadError (promise rejeted), even if added to pbiviz.json/externalJS
//const sigmaatlas = require('sigma/build/plugins/sigma.layout.forceAtlas2.min'); // Browser: uncaught sigma is not declared, even if added to pbiviz.json/externalJS
const sigmaatlas = null; // DEBUG
// LATEST TODO: The stuff above documents all fruiteless approaches to import the whole sigma module.
// PROBLEM: I failed on several ways to import sigma.parsers.gexfparser here. sigma should be a module, but is a public function from the module.
// See also here: https://github.com/jacomyal/sigma.js/issues/871#issuecomment-600577941
// And see also here: https://github.com/DefinitelyTyped/DefinitelyTyped/issues/34776

export class Visual implements IVisual {
    
    private target: HTMLElement;
    private host: IVisualHost;
    private outerContainer: HTMLDivElement;
    private container: HTMLDivElement;
    private circle: Selection<SVGElement>;
    private textValue: Selection<SVGElement>;
    private textLabel: Selection<SVGElement>;
    
    private updateCount: number;
    
    constructor(options: VisualConstructorOptions) {
        console.log("Visual constructor", options, new Date().toLocaleString());
        this.target = options.element;
        
        this.outerContainer = document.createElement('div');
        this.outerContainer.id = 'sigma-example-parent';
        this.outerContainer.style.width = '100%';
        this.outerContainer.style.height = '100%';
        options.element.appendChild(this.outerContainer);
        
        this.container = document.createElement('div');
        this.container.className = 'sigma-expand';
        this.container.style.width = '100%';
        this.container.style.height = '100%';
        this.outerContainer.appendChild(this.container);
        
        console.log("Sigma?");
        console.log(Sigma);
        console.log("sigmaatlas?");
        console.log(sigmaatlas);
        var sigInst: SigmaJs.Sigma = new Sigma({
            container: this.container,
            settings: {
                defaultLabelColor: '#f00',
                defaultLabelSize: 20,
                defaultLabelBGColor: '#eee',
                defaultLabelHoverColor: '#f00',
                labelThreshold: 4,
                defaultEdgeType: 'curve',
                edgeWeightInfluence: 0,
                minNodeSize: 0.5,
                maxNodeSize: 15,
                minEdgeSize: 0.3,
                maxEdgeSize: 1
            }
        });
        /*}).mouseProperties({
            maxRatio: 4
        });*/
        console.log("siginst?");
        console.log(sigInst);
        
        // TODO: Apply rest of sigma stuff here.
        
        //sigma.parsers.gefx('foo.gefx', sigInst);
        /*var gexfString = '<?gefx template="true"?>';
        var gexf = ( new window.DOMParser() ).parseFromString(gexfString, "text/xml");
        sigma.parseGexf(sigInst, gexf);
        sigInst.startForceAtlas2();
        sigInst.stopForceAtlas2();
        sigInst.activateFishEye().draw();*/
        //this.root = $("<div>");//.appendTo(this.target);
            /*.attr("drag-resize-disabled", "true")
            .css({
                "background_color": "red",
                "position": "absolute"
            });*/
        /*this.container = $('<div class="sigma-expand"></div>').appendTo(this.outerContainer)
            .attr("id", "foo")
            .css({
                "width": "100%",
                "height": "100%"
            });*/
        
        this.updateCount = 0;
        console.log("constructor done");
    }

    public update(options: VisualUpdateOptions) {
        console.log('Visual update', options, new Date().toLocaleString());
        this.container.innerHTML = '<p>Update count: <a href="http://google.de"><em>' + this.updateCount++ + '</em></a> at ' + new Date().toLocaleString() + '</p>';
        this.container.style.background = 'green';
    }
}
