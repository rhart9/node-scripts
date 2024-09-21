require('dotenv').config();

module.exports = {
    fetch: async function () {
        let bucket = process.env.S3_TXNUPLOAD_BUCKET;
        let rootPrefix = process.env.S3_TXNUPLOAD_PREFIX
        console.log(`Fetching from S3 bucket ${bucket}, prefix ${rootPrefix}...`);

        const { 
            S3Client, 
            ListObjectsV2Command, 
            GetObjectCommand,
            DeleteObjectCommand
        } = require('@aws-sdk/client-s3');
        const client = new S3Client({});

        const fs = require('fs');
        const fsPromises = require('fs/promises');
        const path = require('path');

        let activeFolder = process.env.BANK_CSV_ACTIVE;

        let procFileInfos = [];

        if (rootPrefix[rootPrefix.length - 1] != '/') {
            rootPrefix += '/';
        }

        let listObjectsResponse = await client.send(new ListObjectsV2Command({ 
            Bucket: bucket, 
            Prefix: rootPrefix,
            Delimiter: '/'
        }));

        let folderInfos = [{ awsSubfolder: null, contents: (listObjectsResponse.Contents ? listObjectsResponse.Contents.filter(item => item.Key != rootPrefix) : null) }]

        if (listObjectsResponse.CommonPrefixes) {
            let commonPrefixes = listObjectsResponse.CommonPrefixes;
            await Promise.all(commonPrefixes.map(commonPrefix => new Promise(async (resolve, reject) => {
                listObjectsResponse = await client.send(new ListObjectsV2Command({ 
                    Bucket: bucket, 
                    Prefix: commonPrefix.Prefix,
                    Delimiter: '/'
                }));
                
                let awsSubfolder = commonPrefix.Prefix.replace(rootPrefix, '').replace('/', '');

                folderInfos.push({ awsSubfolder: awsSubfolder, contents: (listObjectsResponse.Contents ? listObjectsResponse.Contents.filter(item => item.Key != commonPrefix.Prefix) : null) })
                resolve();
            })));
        }

        await Promise.all(folderInfos.map(folderInfo => new Promise(async (resolve, reject) => {
            let contents = folderInfo.contents;
            let awsSubfolder = folderInfo.awsSubfolder;

            if (contents && contents.length > 0) {
                await Promise.all(contents.map(item => new Promise(async (resolve, reject) => {
                    console.log(`Fetching file ${item.Key}`);

                    let params = {
                        Bucket: bucket,
                        Key: item.Key
                    };
                    try {
                        let response = await client.send(new GetObjectCommand(params));

                        let prefix = awsSubfolder ? `${rootPrefix}${awsSubfolder}/` : rootPrefix;
                        let localFileName = item.Key.replace(prefix, '');

                        let fileType;

                        if (awsSubfolder == null) {
                            awsSubfolder = response.Metadata.filetype;
                        }
                        else {
                            let delimIndex = awsSubfolder.indexOf('-');

                            if (delimIndex > 0 && delimIndex < awsSubfolder.length - 1) {
                                fileType = awsSubfolder.substring(delimIndex + 1);
                            }
                            else {
                                fileType = awsSubfolder;
                            }
                        }

                        let writeStream = fs.createWriteStream(path.join(activeFolder, localFileName));

                        response.Body.pipe(writeStream);
                        response.Body.once('error', (error) => {
                            writeStream.end();

                            console.log('\x1b[1m%s\x1b[0m', `WARNING: ${item.Key} did not download properly. Error: ${JSON.stringify(error)}. Process will continue without this file.`);
                            resolve(); 
                        })
                        response.Body.once('end', async () => {
                            procFileInfos.push({
                                fileName: localFileName,
                                fileType: fileType
                            });
                            writeStream.end();

                            console.log(`Fetching file ${item.Key} complete.`);

                            try {
                                await client.send(new DeleteObjectCommand(params));
                                console.log(`File ${item.Key} deleted from S3.`);
                            }
                            catch (error) {
                                console.log('\x1b[1m%s\x1b[0m', `WARNING: ${item.Key} did not delete properly. Error: ${JSON.stringify(error)}. Make sure this file is manually removed from S3.`);
                            }
                            resolve();
                        });
                    }
                    catch (error) {
                        console.log('\x1b[1m%s\x1b[0m', `WARNING: ${item.Key} did not download properly. Error: ${JSON.stringify(error)}. Process will continue without this file.`);
                        resolve(); 
                    }
                })));
            }
            resolve();
        })));

        let manualDir = process.env.BANK_CSV_MANUAL_LOC
        let subDirs = (await fsPromises.readdir(manualDir, { withFileTypes: true })).filter(entry => entry.isDirectory());

        await Promise.all(subDirs.map(dir => new Promise(async (resolve, reject) => {
            let subDirName = dir.name;
            let fileType = subDirName;

            let delimIndex = subDirName.indexOf('-');

            if (delimIndex > 0 && delimIndex < subDirName.length - 1) {
                fileType = subDirName.substring(delimIndex + 1);
            }

            let files = (await fsPromises.readdir(path.join(manualDir, subDirName)))

            await Promise.all(files.map(file => new Promise(async(resolve, reject) => {
                console.log(`Manual file ${file} found in folder ${subDirName}`);

                await fsPromises.rename(path.join(manualDir, subDirName, file), path.join(activeFolder, file));
                procFileInfos.push({
                    fileName: file,
                    fileType: fileType
                });
                resolve();
            })));
            resolve();
        })));

        if (procFileInfos.length > 0) {
            let fileInfoJson = JSON.parse(await fsPromises.readFile(path.join(activeFolder, "fileinfo.json")));

            procFileInfos.forEach(procFileInfo => {
                if (fileInfoJson.fileInfos == undefined) {
                    fileInfoJson.fileInfos = [];
                }
                let filtered = fileInfoJson.fileInfos.filter(fileInfo => fileInfo.fileName == procFileInfo.fileName);
                if (filtered.length > 0) {
                    filtered[0] = procFileInfo;
                }
                else {
                    fileInfoJson.fileInfos.push(procFileInfo);
                }
            });

            await fsPromises.writeFile(path.join(activeFolder, "fileinfo.json"), JSON.stringify(fileInfoJson));
        }

        console.log("Fetching from S3 complete.");
    }
}