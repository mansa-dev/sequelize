'use strict';

/* jshint -W030 */
const chai = require('chai');
const expect = chai.expect;
const Support = require('./../support');
const DataTypes = require('./../../../lib/data-types');
const Sequelize = require('./../../../index');
const _ = require('lodash');

describe(Support.getTestDialectTeaser('Self'), () => {
  it('supports freezeTableName', function() {
    const Group = this.sequelize.define('Group', {}, {
      tableName: 'user_group',
      timestamps: false,
      underscored: true,
      freezeTableName: true
    });

    Group.belongsTo(Group, { as: 'Parent', foreignKey: 'parent_id' });
    return Group.sync({force: true}).then(() => Group.findAll({
      include: [{
        model: Group,
        as: 'Parent'
      }]
    }));
  });

  it('can handle 1:m associations', function() {
    const Person = this.sequelize.define('Person', { name: DataTypes.STRING });

    Person.hasMany(Person, { as: 'Children', foreignKey: 'parent_id'});

    expect(Person.rawAttributes.parent_id).to.be.ok;

    return this.sequelize.sync({force: true}).then(() => Sequelize.Promise.all([
      Person.create({ name: 'Mary' }),
      Person.create({ name: 'John' }),
      Person.create({ name: 'Chris' })
    ])).spread((mary, john, chris) => mary.setChildren([john, chris]));
  });

  it('can handle n:m associations', function() {
    const self = this;

    const Person = this.sequelize.define('Person', { name: DataTypes.STRING });

    Person.belongsToMany(Person, { as: 'Parents', through: 'Family', foreignKey: 'ChildId', otherKey: 'PersonId' });
    Person.belongsToMany(Person, { as: 'Childs', through: 'Family', foreignKey: 'PersonId', otherKey: 'ChildId' });

    const foreignIdentifiers = _.map(_.values(Person.associations), 'foreignIdentifier');
    const rawAttributes = _.keys(this.sequelize.models.Family.rawAttributes);

    expect(foreignIdentifiers.length).to.equal(2);
    expect(rawAttributes.length).to.equal(4);

    expect(foreignIdentifiers).to.have.members(['PersonId', 'ChildId']);
    expect(rawAttributes).to.have.members(['createdAt', 'updatedAt', 'PersonId', 'ChildId']);

    return this.sequelize.sync({ force: true }).then(() => self.sequelize.Sequelize.Promise.all([
      Person.create({ name: 'Mary' }),
      Person.create({ name: 'John' }),
      Person.create({ name: 'Chris' })
    ]).spread((mary, john, chris) => mary.setParents([john]).then(() => chris.addParent(john)).then(() => john.getChilds()).then(children => {
      expect(_.map(children, 'id')).to.have.members([mary.id, chris.id]);
    })));
  });

  it('can handle n:m associations with pre-defined through table', function() {
    const Person = this.sequelize.define('Person', { name: DataTypes.STRING });
    const Family = this.sequelize.define('Family', {
      preexisting_child: {
        type: DataTypes.INTEGER,
        primaryKey: true
      },
      preexisting_parent: {
        type: DataTypes.INTEGER,
        primaryKey: true
      }
    }, { timestamps: false });

    Person.belongsToMany(Person, { as: 'Parents', through: Family, foreignKey: 'preexisting_child', otherKey: 'preexisting_parent' });
    Person.belongsToMany(Person, { as: 'Children', through: Family, foreignKey: 'preexisting_parent', otherKey: 'preexisting_child' });

    const foreignIdentifiers = _.map(_.values(Person.associations), 'foreignIdentifier');
    const rawAttributes = _.keys(Family.rawAttributes);

    expect(foreignIdentifiers.length).to.equal(2);
    expect(rawAttributes.length).to.equal(2);

    expect(foreignIdentifiers).to.have.members(['preexisting_parent', 'preexisting_child']);
    expect(rawAttributes).to.have.members(['preexisting_parent', 'preexisting_child']);

    let count = 0;
    return this.sequelize.sync({ force: true }).bind(this).then(() => Sequelize.Promise.all([
      Person.create({ name: 'Mary' }),
      Person.create({ name: 'John' }),
      Person.create({ name: 'Chris' })
    ])).spread(function(mary, john, chris) {
      this.mary = mary;
      this.chris = chris;
      this.john = john;
      return mary.setParents([john], {
        logging(sql) {
          if (sql.match(/INSERT/)) {
            count++;
            expect(sql).to.have.string('preexisting_child');
            expect(sql).to.have.string('preexisting_parent');
          }
        }
      });
    }).then(function() {
      return this.mary.addParent(this.chris, {
        logging(sql) {
          if (sql.match(/INSERT/)) {
              count++;
              expect(sql).to.have.string('preexisting_child');
              expect(sql).to.have.string('preexisting_parent');
          }
        }
      });
    }).then(function() {
      return this.john.getChildren({
        logging(sql) {
          count++;
          const whereClause = sql.split('FROM')[1]; // look only in the whereClause
          expect(whereClause).to.have.string('preexisting_child');
          expect(whereClause).to.have.string('preexisting_parent');
        }
      });
    }).then(function(children) {
      expect(count).to.be.equal(3);
      expect(_.map(children, 'id')).to.have.members([this.mary.id]);
    });
  });
});
