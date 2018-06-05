'use strict'

const fs = require('fs')
const date = require('date-and-time')
const parse = require('csv-parse')
const stringify = require('csv-stringify')
const transform = require('stream-transform')
const { Transform } = require('stream')

const parser = parse({delimiter: ','})
const input = fs.createReadStream(process.argv[2])

var transformer = transform((record, callback) => {
  let line = [[
    date.format(date.parse(record[0], 'DD MMM YYYY'), 'DD/MM/YYYY'),
    -(parseFloat(record[2])),
    record[1].split('- Card ending')[0].trim()
  ]]
  stringify(line, callback)
}, {parallel: 10})

const skipFirstLine = new Transform({
  writableObjectMode: true,

  transform(chunk, encoding, callback) {
    let lines = chunk.toString().split('\n')
    for (let n in lines) {
      if (n == 0) {
        continue
      }
      let line = lines[n]
      this.push(line)
    }
    callback()
  }
})

input.pipe(skipFirstLine).pipe(parser).pipe(transformer).pipe(process.stdout)
