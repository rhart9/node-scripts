require('dotenv').config();

module.exports = {
    fetch: async function () {
        let bucket = process.env.S3_TXNUPLOAD_BUCKET;
        console.log(`Fetching from S3 bucket ${bucket}...`);

        const { 
            S3Client, 
            ListObjectsV2Command, 
            GetObjectCommand,
            DeleteObjectCommand
        } = require('@aws-sdk/client-s3');
        const client = new S3Client({});

        const fs = require('fs');
        const path = require('path');

        let activeFolder = process.env.BANK_CSV_ACTIVE;

        let procFileInfos = [];

        let listObjectsResponse = await client.send(new ListObjectsV2Command({ Bucket: bucket }));

        if (listObjectsResponse.Contents && listObjectsResponse.Contents.length > 0) {
            await Promise.all(listObjectsResponse.Contents.map(item => new Promise(async (resolve, reject) => {
                console.log(`Fetching file ${item.Key}`);

                let params = {
                    Bucket: bucket,
                    Key: item.Key
                };
                try {
                    let response = await client.send(new GetObjectCommand(params));

                    let writeStream = fs.createWriteStream(path.join(activeFolder, item.Key));

                    response.Body.pipe(writeStream);
                    response.Body.once('error', (error) => {
                        writeStream.end();

                        console.log('\x1b[1m%s\x1b[0m', `WARNING: ${item.Key} did not download properly. Error: ${JSON.stringify(error)}. Process will continue without this file.`);
                        resolve(); 
                    })
                    response.Body.once('end', async () => {
                        procFileInfos.push({
                            fileName: item.Key,
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

            let fileInfoJson = JSON.parse(fs.readFileSync(path.join(activeFolder, "fileinfo.json")));

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

            fs.writeFileSync(path.join(activeFolder, "fileinfo.json"), JSON.stringify(fileInfoJson));
        }
        else {
            console.log("No files found on S3.")
        }

        console.log("Fetching from S3 complete.");
    }
}