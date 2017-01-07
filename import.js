require('dotenv').load({ silent: true })
const console = require('better-console')
const Promise = require('bluebird')

const knex = require('knex')(require('./knexfile')[process.env.NODE_ENV || 'development'])

const moment = require('moment').utc

const exportData = require('./export')

// run if called from the command line
if (require.main !== 'module') {
  run({
    save: true
  })
}

const ROW_LIMIT = parseInt(process.argv[2], 10) || false

// Controlling function
function run () {
  const importedData = {
    people: false,
    organizations: false
  }
  return Promise.resolve()
    .then(() => {
      return exportData.run({
        limit: ROW_LIMIT
      })
    })
    .then(data => {
      // data import!
      return Promise.resolve()
        .then(() => {
          // import people
          return importPeople(data.contacts)
        })
        .then(() => {
          return Promise.all([
            importProfiles(data.contacts),
            importAddresses(data.contacts),
            importPledges(data.contacts)
          ])
        })
        .then(() => {
          // import organizations
          return importOrganizations(data.charities)
            .then(organizationData => {
              importedData.organizations = {
                idByName: {}
              }
              organizationData.forEach(organization => {
                importedData.organizations.idByName[organization.name] = organization.id
              })
            })
        })
        .then(() => {
          // import donations
          return importDonations(data.donations)
        })
        .then(() => {
          return importIncome(data.reportedIncome)
        })
    })
    .catch(err => {
      console.error(err)
    })
    .then(() => {
      return knex.destroy()
    })

  // METHODS

  // people
  function importPeople (people) {
    return Promise.resolve()
    .then(() => {
      console.info(`Importing ${people.length} People`)
      // insert everyone
      return knex.raw(`
          INSERT INTO people.person(email)
          VALUES ${people.map(person => {
            return `(${value(person.email)})`
          }).join(',\n')}
          ON CONFLICT(email) 
            DO UPDATE 
            SET email=EXCLUDED.email
          RETURNING id, email
        `)
        .then(result => result.rows.map(row => Object.assign({}, row)))
        .then(rows => {
          console.log(rows)
          if (rows.length !== people.length) throw new RangeError(`Mismatch between number of people and number of inserted rows`)
          // map IDs to entities
          importedData.people = {
            idbyEntityID: {}
          }
          rows.forEach((row, index) => {
            const entityId = people[index].id
            importedData.people.idbyEntityID[entityId] = row.id
          })
          // map merged IDs to existing records
        })
    })
  }

  function importProfiles (people) {
    // update profile
    return Promise.resolve()
      .then(() => {
        console.info(`Importing ${people.length} Profiles`)
        return knex.raw(`
          INSERT INTO people.profile(
            person_id,
            first_name,
            last_name,
            birth_date
          )
          VALUES ${people.map(person => {
            return `(
              ${value(importedData.people.idbyEntityID[person.id])},
              ${value(person.first_name)},
              ${value(person.last_name)},
              ${value(moment(person.birth_date).format('YYYY-MM-DD'))}
            )`
          }).join(',\n')}
          ON CONFLICT(person_id) 
            DO UPDATE 
            SET person_id=EXCLUDED.person_id
        `)
      })
  }

  // addresses
  function importAddresses (people) {
    return Promise.resolve()
      .then(() => {
        console.info(`Importing ${people.length} Addresses`)
        return knex.raw(`
          INSERT INTO people.address(
            person_id,
            address_1,
            address_2,
            address_3,
            city,
            postal_code,
            region,
            country_code
          )
          VALUES ${people.map(person => {
            return `(
              ${value(importedData.people.idbyEntityID[person.id])},
              ${value(person.street_address)},
              ${value(person.supplemental_address_1)},
              ${value(person.supplemental_address_2)},
              ${value(person.city)},
              ${value(person.postal_code)},
              ${value(person.state_province)},
              ${value(person.country)}
            )`
          }).join(',\n')}
          ON CONFLICT(person_id, address_1, city, postal_code) 
            DO UPDATE 
            SET person_id=EXCLUDED.person_id
        `)
      })
  }

  // pledges
  function importPledges (people) {
    return Promise.resolve()
      .then(() => {
        console.info(`Importing ${people.length} Pledges`)
        knex.raw(`
            INSERT INTO pledges.pledge(
              person_id,
              start_date,
              pledge_percentage
            )
            VALUES ${people.filter(person => person.pledgedamounts__pledge_percentage > 0)
            .map(person => {
              return `(
                ${value(importedData.people.idbyEntityID[person.id])},
                ${value(moment(person.dates__joining_date).toISOString())},
                ${value(person.pledgedamounts__pledge_percentage)}
              )`
            }).join(',\n')}
            ON CONFLICT(person_id, start_date) 
              DO UPDATE 
              SET person_id=EXCLUDED.person_id
          `)
      })
  }

  // orgs
  function importOrganizations (_organizations) {
    const organizations = _organizations.map(charity => ({
      'name': charity.charity_name
    }))
    return Promise.resolve()
      .then(() => {
        console.info(`Importing ${organizations.length} Organizations`)
        return knex.raw(`
          INSERT INTO organizations.organization(
            name
          )
          VALUES ${organizations.map(organization => {
            return `(${value(organization.name)})`
          })}
          ON CONFLICT (name)
            DO UPDATE
            SET name=EXCLUDED.name
          RETURNING id, name
        `)
          .then(data => data.rows.map(row => Object.assign({}, row)))
      })
  }

  // donations
  function importDonations (_donations) {
    const donations = _donations.map(donation => {
      let donationDate = moment(donation.donation_timestamp)
      donationDate = donationDate.isValid() ? donationDate.toISOString() : null
      return {
        person_id: importedData.people.idbyEntityID[donation.donation_contact_id],
        organization_id: importedData.organizations.idByName[donation.donation_target],
        timestamp: donationDate,
        amount: donation.donation_amount,
        currency_code: donation.donation_currency
      }
    })
    console.info(`Importing ${donations.length} Donations`)
    return Promise.resolve()
      .then(() => {
        const q = knex.raw(`
          INSERT INTO pledges.reported_donation(
            person_id,
            organization_id,
            timestamp,
            amount,
            currency_code
          )
          VALUES ${donations.map(donation => {
            return `(
              ${value(donation.person_id)},
              ${value(donation.organization_id)},
              ${value(donation.timestamp)},
              ${value(donation.amount)},
              ${value(donation.currency_code)}
            )`
          }).join(',\n')}
          ON CONFLICT (person_id, organization_id, timestamp, amount)
            DO UPDATE
            SET person_id=EXCLUDED.person_id
          RETURNING id
        `).toSQL().sql
        const fs = require('fs')
        fs.writeFileSync('w.sql', q)
        return knex.raw(q)
      })
        .then(result => result.rows.map(row => row.id))
  }

  // income
  function importIncome (_reportedIncome) {
    const reportedIncome = _reportedIncome.map(income => {
      return {
        person_id: importedData.people.idbyEntityID[income.income_contact_id],
        start_date: moment(income.income_start_date).toISOString(),
        end_date: moment(income.income_end_date).toISOString(),
        amount: income.income_amount,
        currency_code: income.income_currency
      }
    })
    console.info(`Importing ${reportedIncome.length} Income Records`)
    return Promise.resolve()
      .then(() => {
        return knex.raw(`
          INSERT INTO pledges.income(
            person_id,
            start_date,
            end_date,
            amount,
            currency_code
          )
          VALUES ${reportedIncome.map(income => {
            return `(
              ${value(income.person_id)},
              ${value(income.start_date)},
              ${value(income.end_date)},
              ${value(income.amount)},
              ${value(income.currency_code)}
            )`
          }).join(',\n')}
          ON CONFLICT (person_id, start_date, end_date, currency_code)
            DO UPDATE
            SET person_id=EXCLUDED.person_id
          RETURNING id
        `)
      })
        .then(result => result.rows.map(row => row.id))
  }

  // HELPERS
  function value (input) {
    return input && input !== 'Invalid date' ? `$delimiter$${input}$delimiter$` : 'NULL'
  }

  // -
}
