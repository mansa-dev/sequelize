'use strict';

const _ = require('lodash');

/**
 * bulkInsert Query
 */

module.exports = function(tableName, attrValueHashes, options, rawAttributes) {
  options = options || {};
  rawAttributes = rawAttributes || {};

  const tuples = [];
  const serials = [];
  const allAttributes = [];
  let onDuplicateKeyUpdate = '';

  for (const attrValueHash of attrValueHashes) {
    _.forOwn(attrValueHash, (value, key) => {
      if (allAttributes.indexOf(key) === -1) {
        allAttributes.push(key);
      }

      if (rawAttributes[key] && rawAttributes[key].autoIncrement === true) {
        serials.push(key);
      }
    });
  }

  for (const attrValueHash of attrValueHashes) {
    tuples.push('(' + allAttributes.map(key => {
      if (this._dialect.supports.bulkDefault && serials.indexOf(key) !== -1) {
        return attrValueHash[key] || 'DEFAULT';
      }
      return this.escape(attrValueHash[key], rawAttributes[key], { context: 'INSERT' });
    }).join(',') + ')');
  }

  if (this._dialect.supports.updateOnDuplicate && options.updateOnDuplicate) {
    onDuplicateKeyUpdate += ' ON DUPLICATE KEY UPDATE ' + options.updateOnDuplicate.map(attr => {
      const field = rawAttributes && rawAttributes[attr] && rawAttributes[attr].field || attr;
      const key = this.quoteIdentifier(field);
      return key + '=VALUES(' + key + ')';
    }).join(',');
  }

  const replacements = {
    ignoreDuplicates: options.ignoreDuplicates ? this._dialect.supports.ignoreDuplicates : '',
    table: this.quoteTable(tableName),
    attributes: allAttributes.map(attr => this.quoteIdentifier(attr)).join(','),
    tuples: tuples.join(','),
    onDuplicateKeyUpdate,
    returning: this._dialect.supports.returnValues && options.returning ? ' RETURNING *' : ''
  };

  return `INSERT${replacements.ignoreDuplicates} INTO ${replacements.table} (${replacements.attributes}) VALUES ${replacements.tuples}${replacements.onDuplicateKeyUpdate}${replacements.returning};`;
};
