require('dotenv').load({ silent: true })
const defaultConfig = {
  client: 'pg',
  connection: process.env.DATABASE_URL,
  migrations: {
    tableName: 'knex.knex_migrations'
  },
  searchPath: 'public, people, organizations, pledges'
}

module.exports = {
  development: Object.assign({}, defaultConfig, {
    pool: {
      min: 2,
      max: 10
    }
  }),
  staging: Object.assign({}, defaultConfig, {
    pool: {
      min: 2,
      max: 10
    }
  }),
  production: Object.assign({}, defaultConfig, {
    pool: {
      min: 2,
      max: 10
    }
  })
}
