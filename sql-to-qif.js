require('dotenv').config();
const sql = require('mssql/msnodesqlv8');
const fs = require('fs/promises');
const path = require('path');

module.exports = {
    main: async function(argv) {
        const config = {
            driver: 'msnodesqlv8',
            server: process.env.DB_SERVER, 
            database: process.env.DB_DATABASE,
            options: {
                trustedConnection: true
            }
        };

        let pool = await sql.connect(config);

        let query = "SELECT * FROM vLegacyExport ORDER BY AccountID, LegacySpinelfinRef";

        let response = await pool.request().query(query);

        let text = "!Option:AutoSwitch\n";
        let curAccountID = 0
        let curSpinelfinRef = 0

        for (row of response.recordset) {
            let accountID = row.AccountID;
            let qifType = row.QIFType;
            if (accountID != curAccountID) {
                text += `!Account\nN${row.AccountName}\nT${qifType}\n^\n!Type:${qifType}\n`
                curAccountID = accountID;
                curSpinelfinRef = 0;
            }

            let spinelfinRef = row.LegacySpinelfinRef
            let category = row.LegacyCategory;

            if (spinelfinRef != curSpinelfinRef) {
                let date = row.ReferenceDate.toLocaleDateString("en-us", { timeZone: "UTC" })

                text += `D${date}\nN${spinelfinRef}\nT${row.Amount}\nP${row.Payee}\n`
                
                if (row.Reconciled == 1) {
                    text += `CX\n`
                }
                else if (row.Cleared == 1) {
                    text += `C*\n`
                }

                if (category != "") {
                    text += `L${category}\n`
                }
            }
            let splitNum = row.SplitNum;
            let splitTotal = row.SplitTotal;

            if (splitTotal > 1) {
                text += `S${category}\nE${row.SplitDescription}\n\$${row.SplitAmount}\n`
            }
            if (splitNum == splitTotal) {
                text += `^\n`
            }
        }

        let legacyImportFolder = process.env.QIF_IMPORT_FOLDER;
        let outputPath = path.join(legacyImportFolder, `transactions-full.qif`);

        await fs.writeFile(outputPath, text);
    }
};
require('make-runnable/custom')({
    printOutputFrame: false
})