require('dotenv').config();

module.exports = {
    fetch: async function () {
        let bucket = process.env.S3_TXNUPLOAD_BUCKET;
        let prefix = process.env.S3_TXNUPLOAD_PREFIX
        console.log(`Fetching from S3 bucket ${bucket}, prefix ${prefix}...`);

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

        if (prefix[prefix.length - 1] != '/') {
            prefix += '/';
        }

        let listObjectsResponse = await client.send(new ListObjectsV2Command({ 
            Bucket: bucket, 
            Prefix: prefix,
            Delimiter: '/'
        }));

        if (listObjectsResponse.Contents) {
            let contents = listObjectsResponse.Contents.filter(item => item.Key != prefix);

            if (contents.length > 0) {
                await Promise.all(contents.map(item => new Promise(async (resolve, reject) => {
                    console.log(`Fetching file ${item.Key}`);

                    let params = {
                        Bucket: bucket,
                        Key: item.Key
                    };
                    try {
                        let response = await client.send(new GetObjectCommand(params));

                        let localFileName = item.Key.replace(prefix, '');

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
                                fileType: response.Metadata.filetype
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
                })

                await fsPromises.writeFile(path.join(activeFolder, "fileinfo.json"), JSON.stringify(fileInfoJson));
            }
            else {
                console.log("No files found on S3.")
            }
        }
        else {
            console.log("Folder not found on S3.")
        }

        console.log("Fetching from S3 complete.");
    }
}