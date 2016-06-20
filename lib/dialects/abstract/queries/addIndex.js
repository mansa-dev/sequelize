'use strict';

const Model = require('../../../model')
    , util = require('util')
    , _ = require('lodash');

/**
 * Returns an AddIndex Query
 *
 * @param tableName    Name of an existing table, possibly with schema.
 * @param options
 *                        type: UNIQUE|FULLTEXT|SPATIAL
 *                        name: The name of the index. Default is <table>_<attr1>_<attr2>
 *                        fields: An array of attributes as string or as hash.
 *                          If the attribute is a hash, it must have the following content:
 *                          - name: The name of the attribute/column
 *                          - length: An integer. Optional
 *                          - order: 'ASC' or 'DESC'. Optional
 *                        parser
 * @param rawTablename, the name of the table, without schema. Used to create the name of the index
 *
 * @return String
 */

module.exports = function(tableName, attributes, options, rawTablename) {
  options = options || {};

  if (!Array.isArray(attributes)) {
    options = attributes;
    attributes = undefined;
  } else {
    options.fields = attributes;
  }

  // Backwards compatability
  if (options.indexName) {
    options.name = options.indexName;
  }
  if (options.indicesType) {
    options.type = options.indicesType;
  }
  if (options.indexType || options.method) {
    options.using = options.indexType || options.method;
  }

  options.prefix = options.prefix || rawTablename || tableName;
  if (options.prefix && _.isString(options.prefix)) {
    options.prefix = options.prefix.replace(/\./g, '_');
    options.prefix = options.prefix.replace(/(\"|\')/g, '');
  }

  const fieldsSql = options.fields.map(field => {
    if (typeof field === 'string') {
      return this.quoteIdentifier(field);
    } else if (field._isSequelizeMethod) {
      return this.handleSequelizeMethod(field);
    } else {
      let result = '';

      if (field.attribute) {
        field.name = field.attribute;
      }

      if (!field.name) {
        throw new Error('The following index field has no name: ' + util.inspect(field));
      }

      result += this.quoteIdentifier(field.name);

      if (this._dialect.supports.index.collate && field.collate) {
        result += ' COLLATE ' + this.quoteIdentifier(field.collate);
      }

      if (this._dialect.supports.index.length && field.length) {
        result += '(' + field.length + ')';
      }

      if (field.order) {
        result += ' ' + field.order;
      }

      return result;
    }
  });

  if (!options.name) {
    // Mostly for cases where addIndex is called directly by the user without an options object (for example in migrations)
    // All calls that go through sequelize should already have a name
    options = this.nameIndexes([options], options.prefix)[0];
  }

  options = Model.$conformIndex(options);

  if (!this._dialect.supports.index.type) {
    delete options.type;
  }

  if (options.where) {
    options.where = this.whereQuery(options.where);
  }

  if (_.isString(tableName)) {
    tableName = this.quoteIdentifiers(tableName);
  } else {
    tableName = this.quoteTable(tableName);
  }

  const concurrently = this._dialect.supports.index.concurrently && options.concurrently ? 'CONCURRENTLY' : undefined;
  let ind;
  if (this._dialect.supports.indexViaAlter) {
    ind = [
      'ALTER TABLE',
      tableName,
      concurrently,
      'ADD'
    ];
  } else {
    ind = ['CREATE'];
  }

  ind = ind.concat(
    options.unique ? 'UNIQUE' : '',
    options.type, 'INDEX',
    !this._dialect.supports.indexViaAlter ? concurrently : undefined,
    this.quoteIdentifiers(options.name),
    this._dialect.supports.index.using === 1 && options.using ? 'USING ' + options.using : '',
    !this._dialect.supports.indexViaAlter ? 'ON ' + tableName : undefined,
    this._dialect.supports.index.using === 2 && options.using ? 'USING ' + options.using : '',
    '(' + fieldsSql.join(', ') + (options.operator ? ' '+options.operator : '') + ')',
    (this._dialect.supports.index.parser && options.parser ? 'WITH PARSER ' + options.parser : undefined),
    (this._dialect.supports.index.where && options.where ? options.where : undefined)
  );

  return _.compact(ind).join(' ');
};
