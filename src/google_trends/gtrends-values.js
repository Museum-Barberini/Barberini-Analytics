#!/usr/bin/env node
"use strict";
const fs = require('fs'), path = require('path');
const jsonc = require('jsonc');
const node = require('deasync');
const googleTrends = require('google-trends-api');

const facts = jsonc.parse(fs.readFileSync('../../data/barberini_facts.jsonc', 'utf8')); 

let args = process.argv.slice(1).map(arg => arg.replace(/^"(.*)"$/, '$1'));
let topics = require(args[1]);

let interestMap = Object.entries(topics)
	.map(([topicId, topicName]) => {
		return wait(getInterestsOverTime(topicName))
			.map(dataPoint => {
				return {topicId, date: dataPoint.date, interestValue: dataPoint.interestValue}});
	}).reduce(function(rows, row) { return rows.concat(row) }, []);

let output = JSON.stringify(interestMap);

let dir = path.dirname(args[2]);
if (!fs.existsSync(dir))
    fs.mkdirSync(dir, { recursive: true });
fs.writeFileSync(args[2], output + '\n');

    
async function getInterestsOverTime(query) {
	return googleTrends.interestOverTime({
		keyword: query,
		geo: facts.geo,
		startTime: new Date(facts.foundingDate)
	}).catch(error => {
        throw error;
    }).then(results => {
		let resultsObj = JSON.parse(results);
		let timeline = resultsObj.default['timelineData'];
		
		return timeline.map(dataPoint => {
			console.assert(dataPoint.value.length = 1);
			return {
				date: new Date(dataPoint.formattedAxisTime).toISOString().split('T')[0],
				interestValue: dataPoint.value[0]
		}});
    });
}

// Wait for a promise without using the await
// CREDITS: https://stackoverflow.com/a/50323216
function wait(promise) {
    var done = 0;
    var result = null;
    promise.then(
        // on value
        function (value) {
            done = 1;
            result = value;
            return (value);
        },
        // on exception
        function (reason) {
            done = 1;
            throw reason;
        }
    );

    while (!done)
        node.runLoopOnce();

    return (result);
}
