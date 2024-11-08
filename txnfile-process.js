require('dotenv').config();
const sql = require('mssql/msnodesqlv8');
const csv = require("csv-parse/sync");

let { v4: uuidv4 } = require('uuid');

let _exportToQIF = true;
let _exportToSQL = true;
let _batchGUID = uuidv4();

function qifEntry(date, amount, payee) {
    return `
D${date}
T${amount}
P${payee}
^
`.trim();
}

async function processFile(lines, importAlgorithm, reverseSign, accountID, pool) {
    let text = '';

    for (let line of lines) {
        let date, payee, amount, checkNo;
        let skipEntry = false;

        if (importAlgorithm.toLowerCase() == "citizens") {
            date = new Date(line["Date"]);
            payee = line["Description"];
            amount = line["Amount"];

            //if (line["Transaction Type"] == "CHECK") {
                checkNo = line["Reference No."];
            //}
        }
        else if (importAlgorithm.toLowerCase() == "amco") {
            if (line.hasOwnProperty("Transaction Date")) {
                date = new Date(line["Transaction Date"]);
            }
            else if (line.hasOwnProperty("Date")) {
                date = new Date(line["Date"]);
            }
            else {
                throw new Error("Date field not found in file with amco algorithm");
            }
            payee = line["Merchant Name"];
            amount = line["Amount"].replace(/[\$,]/g,"");

            // if unclear, don't skip and deal with it later
            if (line["Activity Status"] && line["Activity Status"] != "APPROVED") 
            {
                skipEntry = true;
            }
            else if (line["Status"] && line["Status"] != "APPROVED") 
            {
                skipEntry = true;
            }
        }
        else if (importAlgorithm.toLowerCase() == "capitalone") {
            date = new Date(line["Transaction Date"]);
            payee = line["Description"];
            if (line["Debit"] != "") {
                amount = line["Debit"] * -1;
            }
            else if (line["Credit"] != "") {
                amount = line["Credit"];
            }
        }
        else if (importAlgorithm.toLowerCase() == "discover") {
            date = new Date(line["Trans. Date"]);
            payee = line["Description"];
            amount = line["Amount"];
        }
        else if (importAlgorithm.toLowerCase() == "chase") {
            date = new Date(line["Transaction Date"]);
            payee = line["Description"];
            amount = line["Amount"];
        }
        
        if (!skipEntry) {
            payee = payee.replace(/\s+/g," ");

            if (reverseSign && !isNaN(amount)) amount = amount * -1;

            let dateStr = date.toLocaleDateString("en-us", { timeZone: "UTC" });

            if (_exportToQIF) {
                text += qifEntry(dateStr, amount, payee) + '\n';
            }

            if (_exportToSQL) {
                let stmt = "INSERT INTO BankStagingTransaction(AccountID, TransactionDate, Payee, Amount, BatchGUID, CheckNumber) VALUES(@AccountID, @TransactionDate, @Payee, @Amount, @BatchGUID, @CheckNumber)"

                await pool.request()
                    .input('AccountID', sql.Int, accountID)
                    .input('TransactionDate', sql.Date, date)
                    .input('Payee', sql.NVarChar, payee)
                    .input('Amount', sql.Decimal(10, 2), amount)
                    .input('BatchGUID', sql.UniqueIdentifier, _batchGUID)
                    .input('CheckNumber', sql.NVarChar, checkNo)
                    .query(stmt);
            }
        }
    }

    return text;
}

module.exports = {
    main: async function(argv) {
        console.log("Transaction file processing started.");

        const config = {
            driver: 'msnodesqlv8',
            server: process.env.DB_SERVER, 
            database: process.env.DB_DATABASE,
            options: {
                trustedConnection: true
            }
        };

        let pool = await sql.connect(config);

        const fs = require('fs/promises');
        const path = require('path');

        let legacyImportFolder = process.env.QIF_IMPORT_FOLDER;
        let archiveFolder = process.env.BANK_CSV_ARCHIVE;
        let activeFolder = process.env.BANK_CSV_ACTIVE;

        let combineAccounts = ((argv ?? false) && argv.combineAccounts)
        _exportToQIF = (process.env.TXNFILE_EXPORT_TO_QIF === 'true');
        _exportToSQL = (process.env.TXNFILE_EXPORT_TO_SQL === 'true');

        await require('./fetch-txnfiles').fetch();

        let outputFileMap = new Map();
        let procFileInfos = [];
        
        let fileInfoJson = JSON.parse(await fs.readFile(path.join(activeFolder, "fileinfo.json")));
        let fileInfosWrite = fileInfoJson.fileInfos.slice();

        for (fileInfo of fileInfoJson.fileInfos) {
            let fileType = fileInfo.fileType;
            let inputFileName = path.join(activeFolder, fileInfo.fileName);

            try {
                if (!fileType)
                    throw new Error(`Input file ${inputFileName} has an undefined file type.  File is being skipped.  Update fileinfo.json appropriately.`);

                let query = "SELECT a.AccountID, a.AccountName, a.ImportAlgorithm, a.SkipFirstLine, a.ReverseSign, qt.QIFType, qt.AccountType " +
                            "FROM Account a " +
                            "INNER JOIN QIFType qt ON a.QIFTypeID = qt.QIFTypeID " +
                            "WHERE a.AWSFileType = @FileType"

                let response = await pool.request().input('FileType', sql.NVarChar, fileType).query(query);
                let accountID = response.recordset[0].AccountID
                let accountName = response.recordset[0].AccountName
                let importAlgorithm = response.recordset[0].ImportAlgorithm
                let skipFirstLine = response.recordset[0].SkipFirstLine
                let reverseSign = response.recordset[0].ReverseSign
                let qifType = response.recordset[0].QIFType
                let accountType = response.recordset[0].AccountType

                let columnOption = true;
                if (!skipFirstLine) {
                    // not implemented until necessary
                    // columnOption needs to be an array of the column names generated manually somehow (presumably from the db)
                    // see https://csv.js.org/parse/options/columns/
                }

                let fileData = (await fs.readFile(inputFileName, 'utf-8'));
                let lines = csv.parse(fileData, {
                    columns: columnOption,
                    skip_empty_lines: true,
                    bom: true
                  });

                let output = await processFile(lines, importAlgorithm, reverseSign, accountID, pool);

                if (_exportToQIF) {
                    if (!outputFileMap.has(accountName)) {
                        outputFileMap.set(accountName, { "accountType": accountType, "text": `!Type:${qifType}\n` });
                    }
                    outputFileMap.get(accountName).text += output;
                }

                procFileInfos.push({
                    fileName: fileInfo.fileName
                });

                console.log(`Input file ${inputFileName} processed.`);
            }
            catch (err) {
                console.log('\x1b[1m%s\x1b[0m', `WARNING: ${err}`)
            }
        }

        if (_exportToQIF) {
            let combinedOutputText;

            if (combineAccounts) {
                combinedOutputText = "!Option:AutoSwitch\n";
            }

            for (let [accountName, value] of outputFileMap) {
                if (combineAccounts) {
                    combinedOutputText += `!Account\nN${accountName}\nT${value.accountType}\n^\n${value.text}\n`
                }
                else
                {
                    let accountOutputPath = path.join(legacyImportFolder, `transactions-${accountName}.qif`);

                    await fs.writeFile(accountOutputPath, value.text);
                    console.log(`QIF file generated. Account: ${accountName} Output File: ${accountOutputPath}`);
                }
            }

            if (combineAccounts) {
                let combinedOutputPath = path.join(legacyImportFolder, `transactions-from-bank-file.qif`);

                await fs.writeFile(combinedOutputPath, combinedOutputText);
                console.log(`Combined QIF file generated. Output File: ${combinedOutputPath}`);
            }
        }

        if (_exportToSQL) {
            let exportToLegacy = (process.env.EXPORT_TO_LEGACY === 'true');

            await pool.request()
                .input('BatchGUID', sql.UniqueIdentifier, _batchGUID)
                .input('ExportToLegacy', sql.Bit, exportToLegacy)
                .input('CheckNumberSequence', sql.Int, process.env.CHECK_NUMBER_SEQUENCE)
                .execute("spPopulateFromBankStaging");

            console.log(`AccountTransaction table populated.`)
        }

        let jsonModified = false;
        const archiveKeyword = ".archive";

        for (let fileInfo of procFileInfos) {
            let inputFileName = fileInfo.fileName;
            let pathObj = path.parse(inputFileName);

            let fileName = pathObj.name;
            let archiveKeywordPos = fileName.indexOf(archiveKeyword);
            if (archiveKeywordPos > 0) {
                fileName = fileName.substring(0, archiveKeywordPos);
            }

            let archiveFileName = path.join(archiveFolder, `${fileName}${archiveKeyword}${(new Date()).toJSON().replace(/[:-]/g, "")}${pathObj.ext}`);
            await fs.rename(path.join(activeFolder, inputFileName), archiveFileName)
            console.log(`Input file ${inputFileName} archived to ${archiveFileName}`);

            fileInfosWrite = fileInfosWrite.filter(fileInfoWrite => !(fileInfoWrite.fileName == fileInfo.fileName));
            jsonModified = true;
        }

        if (jsonModified) {
            await fs.writeFile(path.join(activeFolder, "fileinfo.json"), JSON.stringify({ fileInfos: fileInfosWrite }));
        }

        return "Transaction file processing complete."
    }
};
require('make-runnable/custom')({
    printOutputFrame: false
})