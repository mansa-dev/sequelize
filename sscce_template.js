/*
 * Copy this file to ./sscce.js
 * Add code from issue
 * npm run sscce-{dialect}
 */

var Sequelize = require('./index');
var sequelize = require('./test/support').createSequelizeInstance();

let User = sequelize.define('User', {
  name: Sequelize.STRING,
  surname: Sequelize.STRING
});

return sequelize.sync({ force: true })
.then(() => (
  User.bulkCreate([
    { name: 'Hakue', surname: 'Shake'},
    { name: 'Parse karo', surname: 'ddsd'},
    { name: 'Parse karo', surname: 'dssd'}
  ])
))
.then(() => {
  return User.findAll({
    where: {
      name: 'Parse karo'
    }
  });
});
