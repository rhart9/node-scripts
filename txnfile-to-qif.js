require('dotenv').config();
const sql = require('mssql/msnodesqlv8');
const csv = require("csv-parse/sync");

function qifEntry(date, amount, payee) {
    return `
D${date}
T${amount}
P${payee}
^
`.trim();
}

function processFile(lines, importAlgorithm, reverseSign) {
    let text = '';

    lines.forEach(function (line, i) {
        let date, payee, amount;
        let skipEntry = false;

        if (importAlgorithm.toLowerCase() == "citizens") {
            date = new Date(line["Date"]);
            payee = line["Description"];
            amount = line["Amount"];
        }
        else if (importAlgorithm.toLowerCase() == "amco") {
            date = new Date(line["Date"]);
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

            text += qifEntry(dateStr, amount, payee) + '\n';
        }
    });

    return text;
}

module.exports = {
    main: async function(argv) {
        console.log("QIF file generation started");

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

                let query = "SELECT a.AccountName, a.ImportAlgorithm, a.SkipFirstLine, a.ReverseSign, qt.QIFType, qt.AccountType " +
                            "FROM Account a " +
                            "INNER JOIN QIFType qt ON a.QIFTypeID = qt.QIFTypeID " +
                            "WHERE a.AWSFileType = @FileType"

                let response = await pool.request().input('FileType', sql.NVarChar, fileType).query(query);
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

                if (!outputFileMap.has(accountName)) {
                    outputFileMap.set(accountName, { "accountType": accountType, "text": `!Type:${qifType}\n` });
                }

                outputFileMap.get(accountName).text += processFile(lines, importAlgorithm, reverseSign);

                procFileInfos.push({
                    fileName: fileInfo.fileName
                });

                console.log(`Input file ${inputFileName} processed.`);
            }
            catch (err) {
                console.log('\x1b[1m%s\x1b[0m', `WARNING: ${err}`)
            }
        }

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
            let combinedOutputPath = path.join(legacyImportFolder, `transactions-new-allaccts.qif`);

            await fs.writeFile(combinedOutputPath, combinedOutputText);
            console.log(`Combined QIF file generated. Output File: ${combinedOutputPath}`);
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

        return "QIF file generation complete."
    }
};
require('make-runnable/custom')({
    printOutputFrame: false
})