'use strict';

const _ = require('lodash')
    , Utils = require('../../../utils')
    , Model = require('../../../model');

/**
 * Returns a query for selecting elements in the table <tableName>
 *
 * @param {String} tableName
 * @param {Object} options               , Various options to build the query
 * @param {Array}  [options.attributes]  , An array of attributes (e.g. ['name', 'birthday']). Default: *
 * @param {Object} [options.where]       , A hash with conditions (e.g. {name: 'foo'})
 *                                         OR an ID as integer
 *                                         OR a string with conditions (e.g. 'name="foo"').
 *                                         If you use a string, you have to escape it on your own.
 * @param {String|Array} [options.order] ,'id DESC'
 * @param {String|Array} [options.group]
 * @param {Integer} [options.limit]      , The maximum count you want to get.
 * @param {Integer} [options.offset]     , An offset value to start from. Only useable with limit!
 * @param {Instance} model               , Model object
 *
 * @return {String}
 */

module.exports = function (tableName, options, model) {
  options = options || {};

  const limit = options.limit;
  const mainModel = model;
  const mainQueryItems = [];
  const subQuery = options.subQuery === undefined ? limit && options.hasMultiAssociation : options.subQuery;
  const subQueryItems = [];
  let table = null;
  let query;
  let mainAttributes = options.attributes && options.attributes.slice();
  let mainJoinQueries = [];
  // We'll use a subquery if we have a hasMany association and a limit
  let subQueryAttributes = null;
  let subJoinQueries = [];
  let mainTableAs = null;

  if (options.tableAs) {
    mainTableAs = this.quoteTable(options.tableAs);
  } else if (!Array.isArray(tableName) && model) {
    mainTableAs = this.quoteTable(tableName);
  }

  table = !Array.isArray(tableName) ? this.quoteTable(tableName) : tableName.map(t => {
    if (Array.isArray(t)) {
      return this.quoteTable(t[0], t[1]);
    }
    return this.quoteTable(t, true);
  }).join(', ');

  if (subQuery && mainAttributes) {
    for (const keyAtt of model.primaryKeyAttributes) {
      // Check if mainAttributes contain the primary key of the model either as a field or an aliased field
      if (!_.find(mainAttributes, attr => keyAtt === attr || keyAtt === attr[0] || keyAtt === attr[1])) {
        mainAttributes.push(model.rawAttributes[keyAtt].field ? [keyAtt, model.rawAttributes[keyAtt].field] : keyAtt);
      }
    }
  }

  // Escape attributes
  mainAttributes = mainAttributes && mainAttributes.map(attr => {
    let addTable = true;

    if (attr._isSequelizeMethod) {
      return this.handleSequelizeMethod(attr);
    }

    if (Array.isArray(attr) && attr.length === 2) {
      attr = attr.slice();

      if (attr[0]._isSequelizeMethod) {
        attr[0] = this.handleSequelizeMethod(attr[0]);
        addTable = false;
      } else if (attr[0].indexOf('(') === -1 && attr[0].indexOf(')') === -1) {
        attr[0] = this.quoteIdentifier(attr[0]);
      }
      attr = [attr[0], this.quoteIdentifier(attr[1])].join(' AS ');
    } else {
      attr = attr.indexOf(Utils.TICK_CHAR) < 0 && attr.indexOf('"') < 0 ? this.quoteIdentifiers(attr) : attr;
    }

    if (options.include && attr.indexOf('.') === -1 && addTable) {
      attr = mainTableAs + '.' + attr;
    }
    return attr;
  });

  // If no attributes specified, use *
  mainAttributes = mainAttributes || (options.include ? [mainTableAs + '.*'] : ['*']);

  // If subquery, we add the mainAttributes to the subQuery and set the mainAttributes to select * from subquery
  if (subQuery || options.groupedLimit) {
    // We need primary keys
    subQueryAttributes = mainAttributes;
    mainAttributes = [(mainTableAs || table) + '.*'];
  }

  if (options.include) {
    const generateJoinQueries = (include, parentTable) => {
      const association = include.association;
      const through = include.through;
      const joinType = include.required ? ' INNER JOIN ' : ' LEFT OUTER JOIN ';
      const parentIsTop = !include.parent.association && include.parent.model.name === options.model.name;
      const whereOptions = Utils._.clone(options);
      const table = include.model.getTableName();
      const joinQueries = {
        mainQuery: [],
        subQuery: []
      };
      let as = include.as;
      let joinQueryItem = '';
      let attributes;
      let targetWhere;

      whereOptions.keysEscaped = true;

      if (tableName !== parentTable && mainTableAs !== parentTable) {
        as = parentTable + '.' + include.as;
      }

      // includeIgnoreAttributes is used by aggregate functions
      if (options.includeIgnoreAttributes !== false) {
        attributes = include.attributes.map(attr => {
          let attrAs = attr;
          let verbatim = false;

          if (Array.isArray(attr) && attr.length === 2) {
            if (attr[0]._isSequelizeMethod) {
              if (attr[0] instanceof Utils.Literal ||
                attr[0] instanceof Utils.Cast ||
                attr[0] instanceof Utils.Fn
              ) {
                verbatim = true;
              }
            }

            attr = attr.map($attr => $attr._isSequelizeMethod ? this.handleSequelizeMethod($attr) : $attr);

            attrAs = attr[1];
            attr = attr[0];
          } else if (attr instanceof Utils.Literal) {
            return attr.val; // We trust the user to rename the field correctly
          } else if (attr instanceof Utils.Cast || attr instanceof Utils.Fn) {
            throw new Error(
              'Tried to select attributes using Sequelize.cast or Sequelize.fn without specifying an alias for the result, during eager loading. ' +
              'This means the attribute will not be added to the returned instance'
            );
          }

          let prefix;
          if (verbatim === true) {
            prefix = attr;
          } else {
            prefix = this.quoteIdentifier(as) + '.' + this.quoteIdentifier(attr);
          }
          return prefix + ' AS ' + this.quoteIdentifier(as + '.' + attrAs, true);
        });
        if (include.subQuery && subQuery) {
          subQueryAttributes = subQueryAttributes.concat(attributes);
        } else {
          mainAttributes = mainAttributes.concat(attributes);
        }
      }

      if (through) {
        const throughTable = through.model.getTableName();
        const throughAs = as + '.' + through.as;
        const throughAttributes = through.attributes.map(attr =>
          this.quoteIdentifier(throughAs) + '.' + this.quoteIdentifier(Array.isArray(attr) ? attr[0] : attr)
           + ' AS '
           + this.quoteIdentifier(throughAs + '.' + (Array.isArray(attr) ? attr[1] : attr))
        );
        const primaryKeysSource = association.source.primaryKeyAttributes;
        const tableSource = parentTable;
        const identSource = association.identifierField;
        const primaryKeysTarget = association.target.primaryKeyAttributes;
        const tableTarget = as;
        const identTarget = association.foreignIdentifierField;
        const attrTarget = association.target.rawAttributes[primaryKeysTarget[0]].field || primaryKeysTarget[0];

        let attrSource = primaryKeysSource[0];
        let sourceJoinOn;
        let targetJoinOn;
        let throughWhere;

        if (options.includeIgnoreAttributes !== false) {
          // Through includes are always hasMany, so we need to add the attributes to the mainAttributes no matter what (Real join will never be executed in subquery)
          mainAttributes = mainAttributes.concat(throughAttributes);
        }

        // Figure out if we need to use field or attribute
        if (!subQuery) {
          attrSource = association.source.rawAttributes[primaryKeysSource[0]].field;
        }
        if (subQuery && !include.subQuery && !include.parent.subQuery && include.parent.model !== mainModel) {
          attrSource = association.source.rawAttributes[primaryKeysSource[0]].field;
        }

        // Filter statement for left side of through
        // Used by both join and subquery where

        // If parent include was in a subquery need to join on the aliased attribute

        if (subQuery && !include.subQuery && include.parent.subQuery && !parentIsTop) {
          sourceJoinOn = this.quoteIdentifier(tableSource + '.' + attrSource) + ' = ';
        } else {
          sourceJoinOn = this.quoteTable(tableSource) + '.' + this.quoteIdentifier(attrSource) + ' = ';
        }
        sourceJoinOn += this.quoteIdentifier(throughAs) + '.' + this.quoteIdentifier(identSource);

        // Filter statement for right side of through
        // Used by both join and subquery where
        targetJoinOn = this.quoteIdentifier(tableTarget) + '.' + this.quoteIdentifier(attrTarget) + ' = ';
        targetJoinOn += this.quoteIdentifier(throughAs) + '.' + this.quoteIdentifier(identTarget);

        if (include.through.where) {
          throughWhere = this.getWhereConditions(include.through.where, this.sequelize.literal(this.quoteIdentifier(throughAs)), include.through.model);
        }

        if (this._dialect.supports.joinTableDependent) {
          // Generate a wrapped join so that the through table join can be dependent on the target join
          joinQueryItem += joinType + '(';
          joinQueryItem += this.quoteTable(throughTable, throughAs);
          joinQueryItem += ' INNER JOIN ' + this.quoteTable(table, as) + ' ON ';
          joinQueryItem += targetJoinOn;

          if (throughWhere) {
            joinQueryItem += ' AND ' + throughWhere;
          }

          joinQueryItem += ') ON '+sourceJoinOn;
        } else {
          // Generate join SQL for left side of through
          joinQueryItem += joinType + this.quoteTable(throughTable, throughAs)  + ' ON ';
          joinQueryItem += sourceJoinOn;

          // Generate join SQL for right side of through
          joinQueryItem += joinType + this.quoteTable(table, as) + ' ON ';
          joinQueryItem += targetJoinOn;

          if (throughWhere) {
            joinQueryItem += ' AND ' + throughWhere;
          }

        }

        if (include.where || include.through.where) {
          if (include.where) {
            targetWhere = this.getWhereConditions(include.where, this.sequelize.literal(this.quoteIdentifier(as)), include.model, whereOptions);
            if (targetWhere) {
              joinQueryItem += ' AND ' + targetWhere;
            }
          }
          if (subQuery && include.required) {

            if (!options.where) options.where = {};

            let parent = include;
            let child = include;
            let nestedIncludes = [];
            let $query;

            while (parent = parent.parent) {
              nestedIncludes = [_.extend({}, child, {include: nestedIncludes})];
              child = parent;
            }

            const topInclude = nestedIncludes[0];
            const topParent = topInclude.parent;

            if (topInclude.through && Object(topInclude.through.model) === topInclude.through.model) {
              $query = this.selectQuery(topInclude.through.model.getTableName(), {
                attributes: [topInclude.through.model.primaryKeyField],
                include: Model.$validateIncludedElements({
                  model: topInclude.through.model,
                  include: [{
                    association: topInclude.association.toTarget,
                    required: true
                  }]
                }).include,
                model: topInclude.through.model,
                where: { $and: [
                  this.sequelize.asIs([
                    this.quoteTable(topParent.model.name) + '.' + this.quoteIdentifier(topParent.model.primaryKeyField),
                    this.quoteIdentifier(topInclude.through.model.name) + '.' + this.quoteIdentifier(topInclude.association.identifierField)
                  ].join(' = ')),
                  topInclude.through.where
                ]},
                limit: 1,
                includeIgnoreAttributes: false
              }, topInclude.through.model);
            } else {
              $query = this.selectQuery(topInclude.model.tableName, {
                attributes: [topInclude.model.primaryKeyAttributes[0]],
                include: topInclude.include,
                where: {
                  $join: this.sequelize.asIs([
                    this.quoteTable(topParent.model.name) + '.' + this.quoteIdentifier(topParent.model.primaryKeyAttributes[0]),
                    this.quoteIdentifier(topInclude.model.name) + '.' + this.quoteIdentifier(topInclude.association.identifierField)
                  ].join(' = '))
                },
                limit: 1,
                includeIgnoreAttributes: false
              }, topInclude.model);
            }

            options.where['__' + throughAs] = this.sequelize.asIs([
              '(',
              $query.replace(/\;$/, ''),
              ')',
              'IS NOT NULL'
            ].join(' '));
          }
        }
      } else {
        if (subQuery && include.subQueryFilter) {
          const associationWhere = {};

          associationWhere[association.identifierField] = {
            $raw: this.quoteTable(parentTable) + '.' + this.quoteIdentifier(association.source.primaryKeyField)
          };

          if (!options.where) options.where = {};

          // Creating the as-is where for the subQuery, checks that the required association exists
          const $query = this.selectQuery(include.model.getTableName(), {
            attributes: [association.identifierField],
            where: {
              $and: [
                associationWhere,
                include.where || {}
              ]
            },
            limit: 1
          }, include.model);

          const subQueryWhere = this.sequelize.asIs([
            '(',
            $query.replace(/\;$/, ''),
            ')',
            'IS NOT NULL'
          ].join(' '));

          if (Utils._.isPlainObject(options.where)) {
            options.where['__' + as] = subQueryWhere;
          } else {
            options.where = { $and: [options.where, subQueryWhere] };
          }
        }

        joinQueryItem = ' ' + this.joinIncludeQuery({
          model: mainModel,
          subQuery: options.subQuery,
          include,
          groupedLimit: options.groupedLimit
        });
      }

      if (include.subQuery && subQuery) {
        joinQueries.subQuery.push(joinQueryItem);
      } else {
        joinQueries.mainQuery.push(joinQueryItem);
      }

      if (include.include) {
        for (const childInclude of include.include) {

          if (childInclude.separate || childInclude._pseudo) {
            continue;
          }

          const childJoinQueries = generateJoinQueries(childInclude, as);

          if (childInclude.subQuery && subQuery) {
            joinQueries.subQuery = joinQueries.subQuery.concat(childJoinQueries.subQuery);
          }
          if (childJoinQueries.mainQuery) {
            joinQueries.mainQuery = joinQueries.mainQuery.concat(childJoinQueries.mainQuery);
          }

        }
      }

      return joinQueries;
    };

    // Loop through includes and generate subqueries
    for (const include of options.include) {
      if (include.separate) {
        continue;
      }

      const joinQueries = generateJoinQueries(include, mainTableAs);

      subJoinQueries = subJoinQueries.concat(joinQueries.subQuery);
      mainJoinQueries = mainJoinQueries.concat(joinQueries.mainQuery);

    }
  }

  // If using subQuery select defined subQuery attributes and join subJoinQueries
  if (subQuery) {
    subQueryItems.push(this.selectFromTableFragment(options, model, subQueryAttributes, table, mainTableAs));
    subQueryItems.push(subJoinQueries.join(''));

  // Else do it the reguar way
  } else {
    if (options.groupedLimit) {
      if (!mainTableAs) {
        mainTableAs = table;
      }
      mainQueryItems.push(this.selectFromTableFragment(options, model, mainAttributes, '('+
        options.groupedLimit.values.map(value => {
          const where = _.assign({}, options.where);
          where[options.groupedLimit.on] = value;

          return '('+this.selectQuery(
            table,
            {
              attributes: options.attributes,
              limit: options.groupedLimit.limit,
              order: options.order,
              where
            },
            model
          ).replace(/;$/, '')+')';
        }).join(
          this._dialect.supports['UNION ALL'] ?' UNION ALL ' : ' UNION '
        )
      +')', mainTableAs));
    } else {
      mainQueryItems.push(this.selectFromTableFragment(options, model, mainAttributes, table, mainTableAs));
    }
    mainQueryItems.push(mainJoinQueries.join(''));
  }

  // Add WHERE to sub or main query
  if (options.hasOwnProperty('where') && !options.groupedLimit) {
    options.where = this.getWhereConditions(options.where, mainTableAs || tableName, model, options);
    if (options.where) {
      if (subQuery) {
        subQueryItems.push(' WHERE ' + options.where);
      } else {
        mainQueryItems.push(' WHERE ' + options.where);
        // Walk the main query to update all selects
        _.each(mainQueryItems, (value, key) => {
          if(value.match(/^SELECT/)) {
            mainQueryItems[key] = this.selectFromTableFragment(options, model, mainAttributes, table, mainTableAs, options.where);
          }
        });
      }
    }
  }

  // Add GROUP BY to sub or main query
  if (options.group) {
    options.group = Array.isArray(options.group) ? options.group.map(t => this.quote(t, model)).join(', ') : options.group;
    if (subQuery) {
      subQueryItems.push(' GROUP BY ' + options.group);
    } else {
      mainQueryItems.push(' GROUP BY ' + options.group);
    }
  }

  // Add HAVING to sub or main query
  if (options.hasOwnProperty('having')) {
    options.having = this.getWhereConditions(options.having, tableName, model, options, false);
    if (subQuery) {
      subQueryItems.push(' HAVING ' + options.having);
    } else {
      mainQueryItems.push(' HAVING ' + options.having);
    }
  }
  // Add ORDER to sub or main query
  if (options.order && !options.groupedLimit) {
    const orders = this.getQueryOrders(options, model, subQuery);

    if (orders.mainQueryOrder.length) {
      mainQueryItems.push(' ORDER BY ' + orders.mainQueryOrder.join(', '));
    }
    if (orders.subQueryOrder.length) {
      subQueryItems.push(' ORDER BY ' + orders.subQueryOrder.join(', '));
    }
  }

  // Add LIMIT, OFFSET to sub or main query
  const limitOrder = this.addLimitAndOffset(options, model);
  if (limitOrder && !options.groupedLimit) {
    if (subQuery) {
      subQueryItems.push(limitOrder);
    } else {
      mainQueryItems.push(limitOrder);
    }
  }

  // If using subQuery, select attributes from wrapped subQuery and join out join tables
  if (subQuery) {
    query = 'SELECT ' + mainAttributes.join(', ') + ' FROM (';
    query += subQueryItems.join('');
    query += ') AS ' + mainTableAs;
    query += mainJoinQueries.join('');
    query += mainQueryItems.join('');
  } else {
    query = mainQueryItems.join('');
  }

  if (options.lock && this._dialect.supports.lock) {
    let lock = options.lock;
    if (typeof options.lock === 'object') {
      lock = options.lock.level;
    }
    if (this._dialect.supports.lockKey && (lock === 'KEY SHARE' || lock === 'NO KEY UPDATE')) {
      query += ' FOR ' + lock;
    } else if (lock === 'SHARE') {
      query += ' ' + this._dialect.supports.forShare;
    } else {
      query += ' FOR UPDATE';
    }
    if (this._dialect.supports.lockOf && options.lock.of && options.lock.of.prototype instanceof Model) {
      query += ' OF ' + this.quoteTable(options.lock.of.name);
    }
  }

  query += ';';

  return query;
};
