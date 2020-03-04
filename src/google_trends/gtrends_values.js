#!/usr/bin/env node
"use strict";
const fs = require('fs'), path = require('path');
const node = require('deasync');
const googleTrends = require('google-trends-api');

let args = process.argv.slice(1).map(arg => arg.replace(/^"(.*)"$/, '$1')).slice(1);
if (args.length != 4) {
	throw "Usage: node gtrends_values.js countryCode startTime /path/to/topics.json /path/to/interestValues.json";
}
let countryCode = args[0];
let startTime = new Date(args[1]);
let topics = JSON.parse(fs.readFileSync(args[2]).toString()); //require(args[2]);
let outputPath = args[3];

let interestMap = topics
	.map(topic => {
		return wait(getInterestsOverTime(topic, countryCode, startTime))
			.map(dataPoint => {
				return {topic, date: dataPoint.date, interestValue: dataPoint.interestValue}});
	}).reduce(function(rows, row) { return rows.concat(row) }, []);

let output = JSON.stringify(interestMap);

let dir = path.dirname(outputPath);
if (!fs.existsSync(dir))
	fs.mkdirSync(dir, { recursive: true });
fs.writeFileSync(outputPath, output + '\n');


async function getInterestsOverTime(query, countryCode, startTime) {
	return googleTrends.interestOverTime({
		keyword: query,
		geo: countryCode,
		startTime: startTime
	}).catch(error => {
		console.log("Invalid answer from google trends API. Maybe your query was invalid. For example, countryCodes must be UPPERCASE.");
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
