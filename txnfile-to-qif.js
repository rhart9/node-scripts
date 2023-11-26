require('dotenv').config();
const sql = require('mssql/msnodesqlv8');

function qifEntry(date, amount, payee) {
    return `
D${date}
T${amount}
P${payee}
^
`.trim();
}

function processFile(lines, importAlgorithm, skipFirstLine, reverseSign) {
    let text = '';

    lines.forEach(function (line, i) {
        if (line.length > 0 && (!skipFirstLine || i > 0)) {
            let date, payee, amount;
            let skipEntry = false;

            if (importAlgorithm.toLowerCase() == "citizens") {
                [, date, , payee, amount, , , ] = line.split(',').map(col => col.replace(/"/g,""));
                
                let dateObj = new Date(date);
                date = `${dateObj.getMonth() + 1}/${dateObj.getDate()}/${dateObj.getFullYear()}`;

                payee = payee.replace(/\s+/g," ");
            }
            else if (importAlgorithm.toLowerCase() == "amco") {
                line = "\"," + line + ",\"";  // so dumb
                
                let reward;
                [, date, , , , , , , payee, , , , , amount, reward, ] = line.split("\",\"").map(col => col.replace(/"/g,""));
                
                let [year, month, day] = date.split('-')
                date = `${month}/${day}/${year}`;

                amount = amount.replace("$","");

                if (reward == '' && amount >= 0 && !payee.startsWith('INTEREST'))
                {
                    skipEntry = true;
                }
            }
            
            if (!skipEntry) {
                if (reverseSign && !isNaN(amount)) amount = amount * -1;

                text += qifEntry(date, amount, payee) + '\n';
            }
        }
    });

    return text;
}

module.exports = {
    main: async function() {
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

        let quickenFolder = process.env.QIF_IMPORT_FOLDER;
        let archiveFolder = process.env.BANK_CSV_ARCHIVE;
        let activeFolder = process.env.BANK_CSV_ACTIVE;

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

                let lines = (await fs
                    .readFile(inputFileName, 'utf-8'))
                    .split('\n');

                let query = "SELECT a.AccountName, a.ImportAlgorithm, a.SkipFirstLine, a.ReverseSign, qt.QIFType " +
                            "FROM Account a " +
                            "INNER JOIN QIFType qt ON a.QIFTypeID = qt.QIFTypeID " +
                            "WHERE a.AWSFileType = @FileType"

                let response = await pool.request().input('FileType', sql.NVarChar, fileType).query(query);
                let accountName = response.recordset[0].AccountName
                let importAlgorithm = response.recordset[0].ImportAlgorithm
                let skipFirstLine = response.recordset[0].SkipFirstLine
                let reverseSign = response.recordset[0].ReverseSign
                let qifType = response.recordset[0].QIFType

                if (!outputFileMap.has(accountName)) {
                    outputFileMap.set(accountName, { "text": `!Type:${qifType}\n` });
                }

                outputFileMap.get(accountName).text += processFile(lines, importAlgorithm, skipFirstLine, reverseSign);

                procFileInfos.push({
                    fileName: fileInfo.fileName
                });

                console.log(`Input file ${inputFileName} processed.`);
            }
            catch (err) {
                console.log('\x1b[1m%s\x1b[0m', `WARNING: ${err}`)
            }
        }

        for (let [accountName, value] of outputFileMap) {
            let outputPath = path.join(quickenFolder, `transactions-${accountName}.qif`);

            await fs.writeFile(outputPath, value.text);
            console.log(`QIF file generated. Account: ${accountName} Output File: ${outputPath}`);
        }

        let jsonModified = false;

        for (let fileInfo of procFileInfos) {
            let inputFileName = fileInfo.fileName;
            let pathObj = path.parse(inputFileName);
            let archiveFileName = path.join(archiveFolder, `${pathObj.name}.${(new Date()).toJSON().replace(/:/g, "")}${pathObj.ext}`);
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