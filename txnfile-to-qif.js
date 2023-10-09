require('dotenv').config();

function qifEntry(date, amount, payee) {
    return `
D${date}
T${amount}
P${payee}
^
`.trim();
}

function processFile(lines, fileType, fileConfig) {
    let skipFirstLine = fileConfig.skipFirstLine;
    let reverseSign = fileConfig.reverseSign;

    let text = '';

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

                text += qifEntry(date, amount, payee) + '\n';
            }
        }
    });

    return text;
}

module.exports = {
    main: function() {
        console.log("QIF file generation started");
        
        let configs = new Map(Object.entries(require('./txnfile-config')));
        let inputConfigs = new Map(Object.entries(configs.get("inputTypes")));
        let outputConfigs = new Map(Object.entries(configs.get("outputTypes")));

        const fs = require('fs');
        const path = require('path');

        let quickenFolder = process.env.QIF_IMPORT_FOLDER;
        let archiveFolder = process.env.BANK_CSV_ARCHIVE;
        let activeFolder = process.env.BANK_CSV_ACTIVE;

        let outputFileMap = new Map();
        
        let files = JSON.parse(fs.readFileSync(path.join(activeFolder, "fileinfo.json"))).files;

        files.forEach(fileInfo => {
            let fileType = fileInfo.fileType;
            let inputFileName = path.join(activeFolder, fileInfo.fileName);

            let lines = fs
                .readFileSync(inputFileName, 'utf-8')
                .split('\n');

            let inputConfig = inputConfigs.get(fileType);
            let outputType = inputConfig.outputType;
            let outputConfig = outputConfigs.get(outputType);
            let qifType = outputConfig.qifType;

            if (!outputFileMap.has(outputType)) {
                outputFileMap.set(outputType, { "text": `!Type:${qifType}\n` });
            }

            outputFileMap.get(outputType).text += processFile(lines, fileType, inputConfig);
        })

        outputFileMap.forEach((value, outputType) => {
            let outputPath = path.join(quickenFolder, `transactions-${outputType}.qif`);

            fs.writeFileSync(outputPath, value.text);
            console.log(`QIF file generated.\nType: ${outputType}\nOutput File: ${outputPath}`);
        })

        files.forEach(fileInfo => {
            let inputFileName = fileInfo.fileName;
            let pathObj = path.parse(inputFileName);
            let archiveFileName = path.join(archiveFolder, `${pathObj.name}.${(new Date()).toJSON().replace(/:/g, "")}${pathObj.ext}`);
            fs.renameSync(path.join(activeFolder, inputFileName), archiveFileName)
            console.log(`Input file archived to ${archiveFileName}`);
        })

        return "Process complete."
    }
};
require('make-runnable/custom')({
    printOutputFrame: false
})