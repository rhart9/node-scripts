require('dotenv').config();
let config;

function qifEntry(date, amount, payee) {
    return `
D${date}
T${amount}
P${payee}
^
`.trim();
}

function processFile(lines, fileType) {
    let fileConfig = config.get(fileType);

    let qifType = fileConfig.qifType;
    let skipFirstLine = fileConfig.skipFirstLine;
    let reverseSign = fileConfig.reverseSign;

    let text = `!Type:${qifType}\n`

    lines.forEach(function (line, i) {
        if (line.length > 0 && (!skipFirstLine || i > 0)) {
            let date, payee, amount;
            let skipEntry = false;

            if (fileType.toLowerCase() == "citizens") {
                [, date, , payee, amount, , , ] = line.split(',').map(col => col.replace(/"/g,""));
                
                let dateObj = new Date(date);
                date = `${dateObj.getMonth() + 1}/${dateObj.getDate()}/${dateObj.getFullYear()}`;

                payee = payee.replace(/\s+/g," ");
            }
            else if (fileType.toLowerCase() == "citizenscc") {
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

                text = text + qifEntry(date, amount, payee) + '\n';
            }
        }
    });

    return text;
}

module.exports = {
    main: function(fileType, inputFileName) {
        console.log("QIF file generation started");
        
        config = new Map(Object.entries(require('./txnfile-config')));

        const fs = require('fs');
        const path = require('path');

        let quickenFolder = process.env.QIF_IMPORT_FOLDER;
        let archiveFolder = process.env.BANK_CSV_ARCHIVE;

        let lines = fs
            .readFileSync(inputFileName, 'utf-8')
            .split('\n');

        let text = processFile(lines, fileType);

        let outputPath = path.join(quickenFolder, `transactions-${fileType}.qif`);

        fs.writeFileSync(outputPath, text);
        console.log(`QIF file generation complete.\nType: ${fileType}\nOutput File: ${outputPath}`);

        let pathObj = path.parse(inputFileName);
        let archiveFileName = path.join(archiveFolder, `${pathObj.name}.${(new Date()).toJSON().replace(/:/g, "")}${pathObj.ext}`);
        fs.renameSync(inputFileName, archiveFileName)
        console.log(`Input file archived to ${archiveFileName}`);

        return "Process complete."
    }
};
require('make-runnable/custom')({
    printOutputFrame: false
})