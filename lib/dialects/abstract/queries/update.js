'use strict';

const Utils = require('../../../utils')
    , DataTypes = require('../../../data-types')
    , _ = require('lodash');
/**
 * Update Query
 *
 * @param tableName Name of the table
 * @param values    A hash with attribute-value-pairs
 * @param where     A hash with conditions (e.g. {name: 'foo'})
                      OR an ID as integer
                      OR a string with conditions (e.g. 'name="foo"').
                      If you use a string, you have to escape it on your own.
 *
 * @return String
 */

module.exports = function(tableName, attrValueHash, where, options, attributes) {
  options = options || {};
  _.defaults(options, this.options);

  attrValueHash = Utils.removeNullValuesFromHash(attrValueHash, options.omitNull, options);

  const values = [];
  const modelAttributeMap = {};
  let query = '<%= tmpTable %>UPDATE <%= table %> SET <%= values %><%= output %> <%= where %>';
  let outputFragment;
  let tmpTable = '';        // tmpTable declaration for trigger
  let selectFromTmp = '';   // Select statement for trigger

  if (this._dialect.supports['LIMIT ON UPDATE'] && options.limit) {
    query += ' LIMIT ' + this.escape(options.limit) + ' ';
  }

  if (this._dialect.supports.returnValues) {
    if (!!this._dialect.supports.returnValues.output) {
      // we always need this for mssql
      outputFragment = ' OUTPUT INSERTED.*';

      //To capture output rows when there is a trigger on MSSQL DB
      if (attributes && options.hasTrigger && this._dialect.supports.tmpTableTrigger) {
        tmpTable = 'declare @tmp table (<%= columns %>); ';
        let tmpColumns = '';
        let outputColumns = '';

        for (const modelKey in attributes){
          const attribute = attributes[modelKey];
          if(!(attribute.type instanceof DataTypes.VIRTUAL)){
            if (tmpColumns.length > 0){
              tmpColumns += ',';
              outputColumns += ',';
            }

            tmpColumns += this.quoteIdentifier(attribute.field) + ' ' + attribute.type.toSql();
            outputColumns += 'INSERTED.' + this.quoteIdentifier(attribute.field);
          }
        }

        const replacement ={
          columns : tmpColumns
        };

        tmpTable = Utils._.template(tmpTable)(replacement).trim();
        outputFragment = ' OUTPUT ' + outputColumns + ' into @tmp';
        selectFromTmp = ';select * from @tmp';

        query += selectFromTmp;
      }
    } else if (this._dialect.supports.returnValues && options.returning) {
      // ensure that the return output is properly mapped to model fields.
      options.mapToModel = true;
      query += ' RETURNING *';
    }
  }

  if (attributes) {
    Utils._.each(attributes, (attribute, key) => {
      modelAttributeMap[key] = attribute;
      if (attribute.field) {
        modelAttributeMap[attribute.field] = attribute;
      }
    });
  }

  for (const key in attrValueHash) {
    if (modelAttributeMap && modelAttributeMap[key] &&
        modelAttributeMap[key].autoIncrement === true &&
        !this._dialect.supports.autoIncrement.update) {
      // not allowed to update identity column
      continue;
    }

    const value = attrValueHash[key];
    values.push(this.quoteIdentifier(key) + '=' + this.escape(value, (modelAttributeMap && modelAttributeMap[key] || undefined), { context: 'UPDATE' }));
  }

  const replacements = {
    table: this.quoteTable(tableName),
    values: values.join(','),
    output: outputFragment,
    where: this.whereQuery(where),
    tmpTable
  };

  if (values.length === 0) {
    return '';
  }

  return Utils._.template(query)(replacements).trim();
};
