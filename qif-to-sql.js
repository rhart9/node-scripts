require('dotenv').config();
const sql = require('mssql/msnodesqlv8');

let _tranDate = null, _amount = '', _reconciled = false, _cleared = false, _description = '', _tranMemo = '', _checkNumber = '';
let _category = '', _splitDescription = '', _splitAmount = '';
let _linesProcessed = 0, _transactionsProcessed = 0, _startTime, _accountName, _accountNames, _validAccount, _progressLogged = false;

let _bcp, _transactionIDSequence = 0, _zeroRecordIDSequence = 0;
let _bcpTransactions = [], _bcpZeroRecords = [], _bcpTransactionSplits = [];

async function writeTransaction(pool) {
    if (_bcp) {
        let transactionID = ++_transactionIDSequence;

        _bcpTransactions.push({
            ImportedTransactionID: transactionID,
            AccountName: _accountName,
            TransactionDate: _tranDate,
            FriendlyDescription: _description,
            Amount: _amount,
            Reconciled: _reconciled,
            Cleared: _cleared,
            QuickenCheckNumber: _checkNumber,
            QuickenMemo: _tranMemo
        });

        return transactionID;
    }
    else {
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
}

async function writeZeroRecord(pool) {
    if (_bcp) {
        let zeroRecordID = ++_zeroRecordIDSequence;

        _bcpZeroRecords.push({
            ImportedZeroRecordID: zeroRecordID,
            AccountName: _accountName,
            ReferenceDate: _tranDate
        });

        return zeroRecordID;
    }
    else {
        let response = await pool.request()
            .input('AccountName', sql.NVarChar, _accountName)
            .input('ReferenceDate', sql.DateTime, _tranDate)
            .execute('spInsertZeroRecordFromQuicken');
        
        _transactionsProcessed++;

        return response.recordset[0].ZeroRecordID;
    }
}

async function writeTransactionSplit(pool, transactionID, zeroRecordID) {
    if (_bcp) {

        _bcpTransactionSplits.push({
            ImportedTransactionID: transactionID,
            ImportedZeroRecordID: zeroRecordID,
            CategoryName: _category,
            Amount: _splitAmount,
            ReferenceDate: _tranDate,
            Description: _splitDescription
        })

        return null;
    }
    else {
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
}

function resetForNewAccount(accountName) {
    _accountName = accountName;
    _linesProcessed = 0;
    _transactionsProcessed = 0;
    _startTime = Date.now();
    _validAccount = (_accountNames.includes(accountName));
    if (!_bcp && _progressLogged) {
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

        _bcp = ((argv ?? false) && argv.bcp)

        if (_bcp) {
            await pool.request().execute('spClearQuickenStagingTables');
        }
        else {
            await pool.request().execute('spClearAllTransactions');
        }
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

                let validAccountContentMode = (!accountInfoMode && _validAccount);

                if (validAccountContentMode) {
                    switch (dataType) {
                        case '!':
                            accountInfoMode = (dataValue == "Account");
                            break;
                        case 'D':
                            let delim1 = dataValue.indexOf("/");
                            let delim2 = dataValue.indexOf("'");

                            _tranDate = new Date(parseInt(dataValue.substring(delim2 + 1, dataValue.length)) + 2000, parseInt(dataValue.substring(0, delim1)) - 1, parseInt(dataValue.substring(delim1 + 1, delim2)));
                            break;
                        case 'U':
                            _amount = dataValue.replaceAll(',','');
                            break;
                        case 'T':
                            // Ignore - same value as U
                            break;
                        case 'C':
                            if (dataValue == 'X') {
                                _reconciled = true;
                            }
                            else if (dataValue == '*') {
                                _cleared = true;
                            }
                            break;
                        case 'P':
                            _description = dataValue;
                            break;
                        case 'M':
                            _tranMemo = dataValue.replaceAll('`', '');
                            break;
                        case 'L':
                            _category = dataValue;
                            break;
                        case 'S':
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
                            break;
                        case 'E':
                            _splitDescription = dataValue;
                            break;
                        case '$':
                            _splitAmount = dataValue.replaceAll(',','');
                            break;
                        case 'N':
                            _checkNumber = dataValue.replaceAll('`','');
                            break;
                        case '^':
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
                        break; 
                    }

                    if (!_bcp) {
                        let elapsedTimeStr = 'unknown'
                        let elapsedSecTotal = Math.floor((Date.now() - _startTime) / 1000)
                        let elapsedSecMod = elapsedSecTotal % 60
                        let elapsedSecModStr = elapsedSecMod < 10 ? `0${elapsedSecMod}` : `${elapsedSecMod}`;
                        elapsedTimeStr = `${Math.floor(elapsedSecTotal / 60)}:${elapsedSecModStr}`
                        
                        process.stdout.write(`Account: ${_accountName}, Lines processed: ${++_linesProcessed}, Transactions processed: ${_transactionsProcessed}, Elapsed time: ${elapsedTimeStr}, ${ `${elapsedSecTotal == 0 ? "---" : (Math.round(_transactionsProcessed / elapsedSecTotal * 10) / 10).toFixed(1)}`.padStart(5) } txn/s\r`);
                        _progressLogged = true;
                    }
                }
                else { // !validAccountContentMode
                    switch (dataType) {
                        case '!':
                            accountInfoMode = (dataValue == "Account");
                            break;
                        case 'N':
                            if (accountInfoMode) {
                                resetForNewAccount(dataValue);
                            }
                            break;
                    }
                }
            }
        }

        if (_bcp) {
            var Bcp = require('bcp');

            var bcpObject = new Bcp({
                server: process.env.DB_SERVER, 
                database: process.env.DB_DATABASE,
                trusted: true,
                fieldTerminator: '\t::\t',
                rowTerminator: '\t::\n',
                unicode: true,
                checkConstraints: true
            });

            bcpObject.exec = JSON.stringify(path.join(process.env.BCP_LOCATION, bcpObject.exec));

            bcpObject.loadTable = async function(tableName, tableData, resolve, reject) {
                this.prepareBulkInsert(tableName, Object.keys(tableData[0]), function (err, imp) {
                    if (err) {
                        process.stdout.write(err.message);
                        reject();
                    }
                    else {
                        imp.writeRows(tableData);
                        imp.execute(function(err) {
                            if (err) {
                                process.stdout.write(err.message);
                                reject();
                            }
                            else {
                                process.stdout.write(`${tableName} loaded.\r\n`)
                                resolve();
                            }
                        });
                    }
                });
            }

            let bcpTables = [
                { tableName: 'QuickenStagingTransaction', tableData: _bcpTransactions },
                { tableName: 'QuickenStagingZeroRecord', tableData: _bcpZeroRecords },
                { tableName: 'QuickenStagingTransactionSplit', tableData: _bcpTransactionSplits }
            ];

            await Promise.all(bcpTables.map(bcpTable => new Promise((resolve, reject) => bcpObject.loadTable(bcpTable.tableName, bcpTable.tableData, resolve, reject))));

            await pool.request().execute('spPopulateFromQuickenStaging');
        }

        return;
    }
};
require('make-runnable/custom')({
    printOutputFrame: false
})