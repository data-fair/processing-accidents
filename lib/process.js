const fs = require('fs-extra')
const path = require('path')
const converter = require('json-2-csv')
const csv = require('csv-parser')

module.exports = async (processingConfig, dir, log) => {
  const accidents = {}
  const year = processingConfig.year
  let separateur = ''
  if (year <= 2018) {
    separateur = ','
  } else {
    separateur = ';'
  }
  const readStreamC = fs.createReadStream(path.join(dir, `caracteristiques-${year}.csv`))
  const csvParserC = csv({ columns: true, separator: separateur })

  for await (const row of readStreamC.pipe(csvParserC)) {
    if (row.Num_Acc !== undefined) {
      accidents[row.Num_Acc] = row
    }
  }

  const readStreamL = fs.createReadStream(path.join(dir, `lieux-${year}.csv`))
  const csvParserL = csv({ columns: true, separator: separateur })
  for await (const row of readStreamL.pipe(csvParserL)) {
    if (row.Num_Acc !== undefined) {
      for (const [key, value] of Object.entries(row)) {
        accidents[row.Num_Acc][key] = value
      }
    }
  }
  const readStreamU = fs.createReadStream(path.join(dir, `usagers-${year}.csv`))
  const csvParserU = csv({ columns: true, separator: separateur })
  for await (const row of readStreamU.pipe(csvParserU)) {
    if (row.Num_Acc !== undefined) {
      for (const [key, value] of Object.entries(row)) {
        accidents[row.Num_Acc][key] = value
      }
    }
  }

  const readStreamV = fs.createReadStream(path.join(dir, `vehicules-${year}.csv`))
  const csvParserV = csv({ columns: true, separator: separateur })
  for await (const row of readStreamV.pipe(csvParserV)) {
    if (row.Num_Acc !== undefined) {
      for (let [key, value] of Object.entries(row)) {
        if (key === 'catv' && value.length === 1) {
          value = '0' + value
        }
        accidents[row.Num_Acc][key] = value
      }
    }
  }

  const writeStream = fs.createWriteStream(path.join(dir, `accidentsVelo${year}.csv`), { flags: 'w' })
  const accidentsVelos = Object.values(accidents).filter(accident => accident.catv === '01') // correspond à bicyclette
  console.log(accidentsVelos)
  const csvWriter = await converter.json2csv(accidentsVelos, { delimiter: ',' })
  writeStream.write(csvWriter)
  writeStream.end()
}
