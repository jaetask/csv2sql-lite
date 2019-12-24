/* requires */

var util = require('util');
var Transform = require('stream').Transform;

/* constructor */

function CSV2SQL(options) {
  // allow use without new
  if (!(this instanceof CSV2SQL)) {
    return new CSV2SQL(options);
  }

  this.internalBuffer = '';

  this.isFirstDataRow = true;
  this.isFirstRowColumnNames = true;
  this.isFirstChunk = true;

  this.tableName = options.tableName || 'undefined';
  this.dbName = options.dbName || false;
  this.dropTable = options.dropTable || false;
  this.truncateTable = options.truncateTable || false;
  this.seperator = options.seperator || ',';
  this.lineSeperator = options.lineSeperator || '\n';
  this.columnNameSeparator = options.columnNameSeparator || '`';

  //helper functions
  this.insertColumnNames = insertColumnNames;
  this.lineToInsert = lineToInsert;

  //init Transform, call super constructor
  Transform.call(this, options);
}
util.inherits(CSV2SQL, Transform);

/* implement transform stream */

//TODO: encoding not 'sticking'
CSV2SQL.prototype._transform = function(chunk, enc, cb) {
  this.internalBuffer += chunk.toString();
  var newLinePos;
  var line;
  var linePush;

  if (this.isFirstChunk && this.dbName !== false) {
    this.push(`USE ${this.dbName};\n`);
  }

  if (this.isFirstChunk && this.dropTable !== false) {
    this.push(`DROP TABLE IF EXISTS ${this.tableName};\n`);
  }

  if (this.isFirstChunk && this.truncateTable !== false) {
    this.push(`TRUNCATE TABLE ${this.tableName};\n`);
  }

  if (this.isFirstChunk) {
    this.isFirstChunk = false;
  }

  newLinePos = this.internalBuffer.indexOf(this.lineSeperator);
  let currentLine = 0;
  while (newLinePos !== -1) {
    if (currentLine > 100000000) {
      return;
    }
    line = this.internalBuffer.substring(0, newLinePos);
    this.internalBuffer = this.internalBuffer.substring(newLinePos + 1);

    if (this.isFirstRowColumnNames) {
      linePush = this.insertColumnNames(line);
      currentLine++;
    } else {
      linePush = this.lineToInsert(line);
      currentLine++;
    }

    newLinePos = this.internalBuffer.indexOf(this.lineSeperator);

    this.push(linePush + '\n');
  }

  cb();
};

/* implement transform flush 'event' */

//after all the chunks have been processed, put a ';' to finish off the INSERT
CSV2SQL.prototype._flush = function(cb) {
  this.push(';');
  cb();
};

/* export */

module.exports = CSV2SQL;

/* helper */

function insertColumnNames(line) {
  const colNames = line.split(this.seperator);
  const colNamesString = [
    this.columnNameSeparator,
    colNames.join(`${this.columnNameSeparator},${this.columnNameSeparator}`),
    this.columnNameSeparator,
  ].join('');

  this.isFirstRowColumnNames = false;

  return `INSERT INTO ${this.tableName} (${colNamesString}) VALUES`;
}

/* helper */

function lineToInsert(line) {
  //TODO: use a csv parser here, or write own
  var dataArr = line.split(this.seperator);
  var row;

  // console.log('dataArr', dataArr);

  //insert comma's between VALUES (..), (..), ... , (..)
  if (this.isFirstDataRow) {
    row = '(';
    this.isFirstDataRow = false;
  } else {
    row = ',(';
  }

  //build up the row (a, b, ... , c)
  for (var i = 0; i < dataArr.length; i++) {
    if (dataArr[i] === '' || dataArr[i] === 'NULL') {
      row += 'NULL';
    } else {
      //enclose datums in single quotes
      row += "'" + dataArr[i] + "'";
    }

    // insert comma's between datums (what about numbers)?
    if (i !== dataArr.length - 1) {
      row += ',';
    }
  }
  row += ')';

  return row;
}
