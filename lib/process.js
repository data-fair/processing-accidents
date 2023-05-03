const fs = require('fs-extra')
const path = require('path')
const processFormat2018 = require('./processFormat2018')// Pour traiter les fichiers de 2005 à 2018
const processFormat2021 = require('./processFormat2021')// Pour traiter les fichiers de 2019 à 2021

// fields to keep for the final file in order to have the same output format
const fields = {
  // caracteristiques
  Num_Acc: 'identifiant accident',
  date: 'date',
  an: 'annee',
  mois: 'mois',
  jour: 'jour',
  hrmn: 'heure',
  dep: 'departement', // need improvements
  com: 'commune',
  lat: 'lat',
  long: 'lon',
  agg: 'en agglomeration',
  int: 'type intersection',
  col: 'type collision',
  lum: 'luminosite',
  atm: 'conditions atmosperiques',
  // lieux
  catr: 'type route',
  circ: 'circulation',
  nbv: 'nb voies',
  prof: 'profil long route',
  plan: 'trace plan route',
  lartpc: 'largeur TPC',
  larrout: 'largeur route',
  surf: 'etat surface',
  infra: 'amenagement',
  situ: 'situation',
  // usagers
  grav: 'gravite accident',
  sexe: 'sexe',
  age: 'age',
  trajet: 'motif deplacement',
  secuexist: 'existence securite',
  equipement: 'equipement(s) securite',
  // vehicules
  obs: 'obstacle fixe heurte',
  obsm: 'obstacle mobile heurte',
  choc: 'localisation choc',
  manv: 'manoeuvre avant accident',
  vehiculeid: 'identifiant vehicule',
  typevehicules: 'type autres vehicules',
  manoeuvehicules: 'manoeuvre autres vehicules',
  numVehicules: 'nombre autres vehicules'
}

const parseVehicules = (vehicules) => {
  let vehiculesParsed = ''
  const vehiculesTab = vehicules.split(',')
  vehiculesTab.forEach(vehicule => {
    vehiculesParsed += vehicule + '/'
  })
  // remove last /
  vehiculesParsed = vehiculesParsed.slice(0, -1)
  return vehiculesParsed
}

const parseManoeuvehicules = (manoeuvres) => {
  let manoeuvehiculesParsed = ''
  const manoeuvehiculesTab = manoeuvres.split(',')
  manoeuvehiculesTab.forEach(manoeuvehicule => {
    manoeuvehiculesParsed += manoeuvehicule.replace(' ', '') + '/'
  })
  // remove last /
  manoeuvehiculesParsed = manoeuvehiculesParsed.slice(0, -1)
  return manoeuvehiculesParsed
}

const years = [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2016, 2017, 2018, 2019, 2020, 2021]
module.exports = async (processingConfig, dir, log) => {
  const writeStream = fs.createWriteStream(path.join(dir, 'accidentsVelo.csv'), { flags: 'w' })
  writeStream.write(Object.keys(fields).join(',') + '\n')
  try {
    for (const year of years) {
      const accidentsVelos = (year < 2019) ? await processFormat2018(year, dir, log) : await processFormat2021(year, dir, log) // change her for a new format
      const usagersVelo = []
      accidentsVelos.forEach(accident => {
        Object.values(accident.vehicules).forEach((vehicule, i, vehicules) => {
          if (vehicule.catv === '1') {
            const autreVehicules = vehicules.filter((v, j) => i !== j)
            const autreVehiculesNonVelos = autreVehicules.filter(v => v.catv !== '1')
            if (vehicule.usagers) {
              vehicule.usagers.forEach(usager => {
                usagersVelo.push(Object.assign({
                  obs: vehicule.obs,
                  obsm: vehicule.obsm,
                  choc: vehicule.choc,
                  manv: vehicule.manv,
                  vehiculeid: vehicule.id,
                  typevehicules: autreVehicules.length ? (autreVehiculesNonVelos.length === 0 ? '1' : autreVehiculesNonVelos.map(v => v.catv).filter((v, i, s) => s.indexOf(v) === i).join(', ')) : undefined,
                  manoeuvehicules: ((autreVehiculesNonVelos.length === 0) ? autreVehicules.filter(v => v.catv === '1') : autreVehiculesNonVelos).map(v => v.manv).filter((v, i, s) => s.indexOf(v) === i).join(', '),
                  numVehicules: (autreVehiculesNonVelos.length === 0) ? autreVehicules.length : autreVehiculesNonVelos.length
                }, usager, accident))
              })
            }
          }
        })
      })

      Object.values(usagersVelo).forEach(accident => {
        if (accident.typevehicules !== undefined && accident.typevehicules.length > 2) {
          accident.typevehicules = parseVehicules(accident.typevehicules)
        }
        if (accident.manoeuvehicules !== undefined && accident.manoeuvehicules.length > 2) {
          accident.manoeuvehicules = parseManoeuvehicules(accident.manoeuvehicules)
        }
        if (processingConfig.filter === undefined) {
          writeStream.write(Object.keys(fields).map(f => accident[f] ? `"${accident[f]}"` : '').join(',') + '\n')
        } else {
          if (accident.dep === processingConfig.filter) {
            writeStream.write(Object.keys(fields).map(f => accident[f] ? `"${accident[f]}"` : '').join(',') + '\n')
          }
        }
      })
      await log.info(`Accidents velo ${year} done`)
    }
    writeStream.end()
  } catch (err) {
    console.error(err)
  }
}
