const { readFileSync } = require('fs');
const { extractText } = require('@firecrawl/pdf-inspector');

const pdf = readFileSync(process.argv[2]);
const result = extractText(pdf);
console.log(typeof result === 'string' ? result : JSON.stringify(result, null, 2));