require('dotenv').config();
const sql = require('mssql/msnodesqlv8');

let _tranDate = null, _amount = '', _reconciled = false, _description = '', _tranMemo = '', _checkNumber = '';
let _category = '', _splitDescription = '', _splitAmount = '';
let _linesProcessed = 0, _transactionsProcessed = 0

async function writeTransaction(pool, account) {
    let response = await pool.request()
        .input('AccountDescription', sql.NVarChar, account)
        .input('TransactionDate', sql.DateTime, _tranDate)
        .input('FriendlyDescription', sql.NVarChar, _description)
        .input('Amount', sql.Decimal(10, 2), _amount)
        .input('Reconciled', sql.Bit, _reconciled)
        .input('QuickenCheckNumber', sql.NVarChar, _checkNumber)
        .input('QuickenMemo', sql.NVarChar, _tranMemo)
        .execute('spInsertTransactionFromQuicken');
    
    _transactionsProcessed++;

    /*
    if (_reconciled) {
        _transactionSerialNumber++;
        _balance += parseFloat(_amount);
    }

    let str = `${_transactionSerialNumber},${_tranDate.toString()},${_description.replace(',','')},${_amount},${_balance},${_reconciled.toString()},${_tranMemo},${_checkNumber}\n`
    
    await writeStream.write(str);
    */

    return response.recordset[0].TransactionID;
}

async function writeZeroRecord(pool, account) {
    let response = await pool.request()
        .input('AccountDescription', sql.NVarChar, account)
        .input('ReferenceDate', sql.DateTime, _tranDate)
        .execute('spInsertZeroRecordFromQuicken');
    
    _transactionsProcessed++;

    return response.recordset[0].ZeroRecordID;
}

async function writeTransactionSplit(pool, transactionID, zeroRecordID) {
    let response = await pool.request()
        .input('TransactionID', sql.Int, transactionID)
        .input('ZeroRecordID', sql.Int, zeroRecordID)
        .input('CategoryName', sql.NVarChar, _category)
        .input('Amount', sql.Decimal(10, 2), _splitAmount)
        .input('ReferenceDate', sql.Date, _tranDate)
        .input('Description', sql.NVarChar, _splitDescription)
        .execute('spInsertTransactionSplitFromQuicken');

    /*
    let str = `,,,,,,,,${transactionID},${_category},${_splitAmount},${_splitDescription.replace(/,/g,'')},${_random}\n`
    
    await writeStream.write(str);
    */

    return response.recordset[0].TransactionSplitID;
}

module.exports = {
    main: async function() {
        const config = {
            driver: 'msnodesqlv8',
            server: process.env.DB_SERVER, 
            database: process.env.DB_DATABASE,
            options: {
                trustedConnection: true
            }
        };

        let pool = await sql.connect(config);
        let path = require('path');
        let quickenFolder = process.env.QIF_EXPORT_FOLDER;
        let accountTypes = ['citizens', 'citizenscc']
        let startTime = Date.now();

        const lineByLine = require('n-readlines');

        await pool.request().execute('spClearAllTransactions');

        for (let i = 0; i < accountTypes.length; i++) {
            accountType = accountTypes[i];

            [_linesProcessed, _transactionsProcessed] = [0, 0]

            /*
            let writeStream = fs.createWriteStream(path.join(quickenFolder, `${accountType}-export.csv`));
            writeStream.write("Serial,Date,Desc,Amount,Balance,Reconciled,TranMemo,CheckNum,TxnID,Cat,Amount,Desc,Random\n");
            */

            let liner = new lineByLine(path.join(quickenFolder, `${accountType}-export.qif`));
            
            let line;
            let dataType, dataValue;
            
            let transactionID = 0, zeroRecordID = 0;

            while (line = liner.next()) {
                let lineStr = line.toString('ascii');
                dataType = lineStr.substring(0, 1);
                dataValue = lineStr.substring(1).replace('\r','').replace('\n','');

                switch (dataType) {
                    case 'D':
                        let delim1 = dataValue.indexOf("/");
                        let delim2 = dataValue.indexOf("'");

                        _tranDate = new Date(parseInt(dataValue.substring(delim2 + 1, dataValue.length)) + 2000, parseInt(dataValue.substring(0, delim1)) - 1, parseInt(dataValue.substring(delim1 + 1, delim2)));
                        break;
                    case 'U':
                        _amount = dataValue.replace(',','');
                        break;
                    case 'T':
                        // Ignore - same value as U
                        break;
                    case 'C':
                        if (dataValue == 'X') {
                            _reconciled = true;
                        }
                        break;
                    case 'P':
                        _description = dataValue;
                        break;
                    case 'M':
                        _tranMemo = dataValue.replace('`', '');
                        break;
                    case 'L':
                        _category = dataValue;
                        break;
                    case 'S':
                        if (_description == "Zero Record") {
                            if (zeroRecordID == 0) {
                                zeroRecordID = await writeZeroRecord(pool, accountType);
                            }
                        }
                        else if (transactionID == 0) {
                            transactionID = await writeTransaction(pool, accountType);
                        }

                        if (_splitAmount != '') {
                            await writeTransactionSplit(pool, transactionID, zeroRecordID);

                            _category = '';
                            _splitAmount = '';
                            _splitDescription = '';
                        }
                        _category = dataValue;
                        break;
                    case 'E':
                        _splitDescription = dataValue;
                        break;
                    case '$':
                        _splitAmount = dataValue.replace(',','');
                        break;
                    case 'N':
                        _checkNumber = dataValue.replace('`','');
                        break;
                    case '^':
                        if (_description == "Zero Record") {
                            if (zeroRecordID == 0) {
                                zeroRecordID = await writeZeroRecord(pool, accountType);
                                _splitAmount = _amount;
                            }
                        }
                        else if (transactionID == 0) {
                            transactionID = await writeTransaction(pool, accountType);
                            _splitAmount = _amount;
                        }

                        await writeTransactionSplit(pool, transactionID, zeroRecordID);
                        
                        [_tranDate, _amount, _reconciled, _description, _tranMemo, _checkNumber, _category, _splitDescription, _splitAmount, transactionID, zeroRecordID] =
                            [null, '', false, '', '', '', '', '', '', 0, 0];

                        break; 
                }

                process.stdout.write(`Type: ${accountType} Lines processed: ${++_linesProcessed}, TransactionsProcessed: ${_transactionsProcessed}\r`);

            }
            process.stdout.write("\n");
        }
        process.stdout.write(`Elapsed: ${(Date.now() - startTime) / 1000} sec\n`)

        return;
    }
};
require('make-runnable/custom')({
    printOutputFrame: false
})