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
  stringify([record], (err, output) => {
    callback(null, output)
  })
}, {parallel: 10})

const skipFirstLine = new Transform({
  writableObjectMode: true,

  transform(chunk, encoding, callback) {
    let lines = chunk.toString().split('\n')
    for (let n in lines) {
      let line = lines[n]
      if (this.seenFirstLine) {
        this.push(line)
      }
      this.seenFirstLine = true
    }
    callback()
  }
})
skipFirstLine.seenFirstLine = false

input.pipe(skipFirstLine).pipe(parser).pipe(transformer).pipe(process.stdout)
