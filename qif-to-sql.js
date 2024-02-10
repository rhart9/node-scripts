require('dotenv').config();
const sql = require('mssql/msnodesqlv8');

let _tranDate = null, _amount = '', _reconciled = false, _cleared = false, _description = '', _tranMemo = '', _checkNumber = '';
let _category = '', _splitDescription = '', _splitAmount = '';
let _linesProcessed = 0, _transactionsProcessed = 0, _startTime, _accountName, _accountNames, _validAccount, _progressLogged = false;

async function writeTransaction(pool) {
    let response = await pool.request()
        .input('AccountName', sql.NVarChar, _accountName)
        .input('TransactionDate', sql.DateTime, _tranDate)
        .input('FriendlyDescription', sql.NVarChar, _description)
        .input('Amount', sql.Decimal(10, 2), _amount)
        .input('Reconciled', sql.Bit, _reconciled)
        .input('Cleared', sql.Bit, _cleared)
        .input('QuickenCheckNumber', sql.NVarChar, _checkNumber)
        .input('QuickenMemo', sql.NVarChar, _tranMemo)
        .execute('spInsertTransactionFromQuicken');
    
    _transactionsProcessed++;

    return response.recordset[0].TransactionID;
}

async function writeZeroRecord(pool) {
    let response = await pool.request()
        .input('AccountName', sql.NVarChar, _accountName)
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

    return response.recordset[0].TransactionSplitID;
}

function resetForNewAccount(accountName) {
    _accountName = accountName;
    _linesProcessed = 0;
    _transactionsProcessed = 0;
    _startTime = Date.now();
    _validAccount = (_accountNames.includes(accountName));
    if (_progressLogged) {
        process.stdout.write("\n");
        _progressLogged = false;
    }
}

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
        let path = require('path');
        let quickenFolder = process.env.QIF_EXPORT_FOLDER;

        const lineByLine = require('n-readlines');

        response = await pool.request().query("SELECT AccountName FROM Account");
        _accountNames = response.recordset.map(a => a.AccountName)

        await pool.request().execute('spClearAllTransactions');
        await pool.request().execute('spExtendQuickenSwitchoverDate'); // if we're still running this script, it should be extended
        await pool.request().execute('spExtendCategories');

        let filesToProcess;

        let combineAccounts = ((argv ?? false) && argv.combineAccounts)

        if (combineAccounts) {
            filesToProcess = [{ "accountName": null, "fileName": "all-export.qif" }];
        }
        else {
            filesToProcess = _accountNames.map(accountName => ({
                "accountName": accountName,
                "fileName": `${accountName.replaceAll(" ", "")}-export.qif`
            }));
        }

        for (let i = 0; i < filesToProcess.length; i++) {
            if (!combineAccounts) {
                resetForNewAccount(filesToProcess[i].accountName);
            }

            let liner;

            try {
                liner = new lineByLine(path.join(quickenFolder, filesToProcess[i].fileName));
            }
            catch (ex) {
                process.stdout.write(`Error opening file ${filesToProcess[i].fileName}.  Message: ${ex.message}\n`);
                continue;
            }
            let line;
            let dataType, dataValue;
            
            let transactionID = 0, zeroRecordID = 0;

            let accountInfoMode = false;

            while (line = liner.next()) {
                let lineStr = line.toString('ascii');
                dataType = lineStr.substring(0, 1);
                dataValue = lineStr.substring(1).replace('\r','').replace('\n','');

                switch (dataType) {
                    case '!':
                        accountInfoMode = (dataValue == "Account");
                        break;
                    case 'D':
                        if (!accountInfoMode && _validAccount) {
                            let delim1 = dataValue.indexOf("/");
                            let delim2 = dataValue.indexOf("'");

                            _tranDate = new Date(parseInt(dataValue.substring(delim2 + 1, dataValue.length)) + 2000, parseInt(dataValue.substring(0, delim1)) - 1, parseInt(dataValue.substring(delim1 + 1, delim2)));
                        }
                        break;
                    case 'U':
                        if (!accountInfoMode && _validAccount) {
                            _amount = dataValue.replaceAll(',','');
                        }
                        break;
                    case 'T':
                        // Ignore - same value as U
                        break;
                    case 'C':
                        if (!accountInfoMode && _validAccount) {
                            if (dataValue == 'X') {
                                _reconciled = true;
                            }
                            else if (dataValue == '*') {
                                _cleared = true;
                            }
                        }
                        break;
                    case 'P':
                        if (!accountInfoMode && _validAccount) {
                            _description = dataValue;
                        }
                        break;
                    case 'M':
                        if (!accountInfoMode && _validAccount) {
                            _tranMemo = dataValue.replaceAll('`', '');
                        }
                        break;
                    case 'L':
                        if (!accountInfoMode && _validAccount) {
                            _category = dataValue;
                        }
                        break;
                    case 'S':
                        if (!accountInfoMode && _validAccount) {
                            if (_description == "Zero Record") {
                                if (zeroRecordID == 0) {
                                    zeroRecordID = await writeZeroRecord(pool);
                                }
                            }
                            else if (transactionID == 0) {
                                transactionID = await writeTransaction(pool);
                            }

                            if (_splitAmount != '') {
                                await writeTransactionSplit(pool, transactionID, zeroRecordID);

                                _category = '';
                                _splitAmount = '';
                                _splitDescription = '';
                            }
                            _category = dataValue;
                        }
                        break;
                    case 'E':
                        if (!accountInfoMode && _validAccount) {
                            _splitDescription = dataValue;
                        }
                        break;
                    case '$':
                        if (!accountInfoMode && _validAccount) {
                            _splitAmount = dataValue.replaceAll(',','');
                        }
                        break;
                    case 'N':
                        if (accountInfoMode) {
                            resetForNewAccount(dataValue);
                        }
                        else if (_validAccount) {
                            _checkNumber = dataValue.replaceAll('`','');
                        }
                        break;
                    case '^':
                        if (!accountInfoMode && _validAccount) {
                            if (_description == "Zero Record") {
                                if (zeroRecordID == 0) {
                                    zeroRecordID = await writeZeroRecord(pool);
                                    _splitAmount = _amount;
                                }
                            }
                            else if (transactionID == 0) {
                                transactionID = await writeTransaction(pool);
                                _splitAmount = _amount;
                            }

                            await writeTransactionSplit(pool, transactionID, zeroRecordID);
                            
                            [_tranDate, _amount, _reconciled, _cleared, _description, _tranMemo, _checkNumber, _category, _splitDescription, _splitAmount, transactionID, zeroRecordID] =
                                [null, '', false, false, '', '', '', '', '', '', 0, 0];
                        }
                        break; 
                }

                if (!accountInfoMode && _validAccount) {
                    let elapsedTimeStr = 'unknown'
                    let elapsedSecTotal = Math.floor((Date.now() - _startTime) / 1000)
                    let elapsedSecMod = elapsedSecTotal % 60
                    let elapsedSecModStr = elapsedSecMod < 10 ? `0${elapsedSecMod}` : `${elapsedSecMod}`;
                    elapsedTimeStr = `${Math.floor(elapsedSecTotal / 60)}:${elapsedSecModStr}`
                    
                    process.stdout.write(`Account: ${_accountName}, Lines processed: ${++_linesProcessed}, Transactions processed: ${_transactionsProcessed}, Elapsed time: ${elapsedTimeStr}, ${ `${elapsedSecTotal == 0 ? "---" : (Math.round(_transactionsProcessed / elapsedSecTotal * 10) / 10).toFixed(1)}`.padStart(5) } txn/s\r`);
                    _progressLogged = true;
                }
            }
        }

        return;
    }
};
require('make-runnable/custom')({
    printOutputFrame: false
})