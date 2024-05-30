const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const firstCsvFile = '../holder_snapshot_at_lz_1.csv';
const secondCsvFile = '../current_holders_may_29.csv';
const outputCsvFile = '../lz_1_excluding_sold.csv';

const snapshot1Data = {};
const currentData = {};

// Read the first CSV file
fs.createReadStream(firstCsvFile)
  .pipe(csv())
  .on('data', (row) => {
    snapshot1Data[row.holder] = parseInt(row.token_count, 10);
  })
  .on('end', () => {
    // Read the second CSV file
    fs.createReadStream(secondCsvFile)
      .pipe(csv())
      .on('data', (row) => {
        currentData[row.holder] = parseInt(row.token_count, 10);
      })
      .on('end', () => {
        // Compare and create the output data
        const outputData = [];
        
        for (const holder in snapshot1Data) {
          if (currentData[holder] !== undefined) {
            const lowerTokenCount = Math.min(snapshot1Data[holder], currentData[holder]);
            outputData.push({ holder, token_count: lowerTokenCount });
          } else {
            outputData.push({ holder, token_count: 0 });
          }
        }

        // Write the output data to a new CSV file
        const csvWriter = createCsvWriter({
          path: outputCsvFile,
          header: [
            { id: 'holder', title: 'holder' },
            { id: 'token_count', title: 'token_count' }
          ]
        });

        csvWriter.writeRecords(outputData)
          .then(() => {
            console.log('The CSV file was written successfully');
          });
      });
  });
