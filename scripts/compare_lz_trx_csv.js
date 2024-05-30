const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const firstCsvFile = './unique_addresses_lz_trx_snapshot.csv';
const secondCsvFile = './current_holders_may_29.csv';
const outputCsvFile = './unique_addresses_lz_trx_1_excluding_sold.csv';

const snapshot1Data = {};
const currentData = {};

// Read the first CSV file
fs.createReadStream(firstCsvFile)
  .pipe(csv())
  .on('data', (row) => {
    snapshot1Data[row.addresses] = 1;
  })
  .on('end', () => {
    // Read the second CSV file
    fs.createReadStream(secondCsvFile)
      .pipe(csv())
      .on('data', (row) => {
        currentData[row.holder] = 1;
      })
      .on('end', () => {
        // Compare and create the output data
        const outputData = [];
        
        for (const holder in snapshot1Data) {
          if (currentData[holder] !== undefined) {
            outputData.push({ holder, stillHolding: 1 });
          } else {
            outputData.push({ holder, stillHolding: 0 });
          }
        }

        // Write the output data to a new CSV file
        const csvWriter = createCsvWriter({
          path: outputCsvFile,
          header: [
            { id: 'holder', title: 'holder' },
            { id: 'stillHolding', title: 'stillHolding' }
          ]
        });

        csvWriter.writeRecords(outputData)
          .then(() => {
            console.log('The CSV file was written successfully');
          });
      });
  });
