import { BoxClient, BoxDeveloperTokenAuth } from 'box-typescript-sdk-gen';
import { writeFileSync, appendFileSync, readFileSync, createWriteStream, existsSync, mkdirSync } from 'fs';
import * as dotenv from 'dotenv';
dotenv.config();

const REFRESH_TIME = 61 * 1000;
const REQUESTS_PER_MINUTE = 950;
let RequestsRemainingThisMinute = REQUESTS_PER_MINUTE;
let MinuteStart = Date.now();

const getRequestsRemaining = async () => {
    let currentTimestamp = Date.now();
    if (currentTimestamp - MinuteStart > REFRESH_TIME) {
        RequestsRemainingThisMinute = REQUESTS_PER_MINUTE;
        MinuteStart = currentTimestamp;
    }
    return RequestsRemainingThisMinute;
};

const getMinuteStart = async () => MinuteStart;

const getMsToRefresh = async () => {
    const minuteStart = await getMinuteStart();
    const expectedRefreshTime = minuteStart + REFRESH_TIME;
    return expectedRefreshTime - Date.now();
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

type File = {
    fileId: string;
    fileName: string;
};

type Folder = {
    folderId: string;
    folderName: string;
    files: File[];
    nestedFolders: Folder[];
};

const getFolder = async (
    client: BoxClient,
    folderId: string,
    alreadyProcessedFolders: Map<string, Folder>,
    folderName: string,
): Promise<Folder> => {
    if (alreadyProcessedFolders.has(folderId)) {
        console.log(`Returning previously processed folder with ID ${folderId}.`);
        return alreadyProcessedFolders.get(folderId)!;
    }

    let requestsRemaining = await getRequestsRemaining();
    console.log(`Processing folder with ID ${folderId}. Requests remaining this minute: ${requestsRemaining}`);
    while (requestsRemaining <= 0) {
        console.log(`Out of requests this minute; sleeping.`);
        await sleep(await getMsToRefresh());
        requestsRemaining = await getRequestsRemaining();
    }

    console.log('Making new request.');
    RequestsRemainingThisMinute -= 1;
    const allEntries = await (async () => (await client.folders.getFolderItems(folderId)).entries || [])();
    const boxFolders = allEntries.filter((entry) => entry.type === 'folder');
    const boxFiles = allEntries.filter((entry) => entry.type === 'file');

    let folder: Folder | null = null;
    await Promise.all(
        boxFolders.map(async (boxFolder) => {
            const nestedFolder = await getFolder(
                client,
                boxFolder.id,
                alreadyProcessedFolders,
                boxFolder.name ?? boxFolder.id,
            );
            if (!folder) {
                folder = {
                    folderId,
                    folderName,
                    files: boxFiles.map((file) => ({
                        fileId: file.id,
                        fileName: file.name ?? file.id,
                    })),
                    nestedFolders: [nestedFolder],
                };
            } else {
                folder.nestedFolders.push(nestedFolder);
            }
        }),
    );

    if (!folder) {
        folder = {
            folderId,
            folderName,
            files: boxFiles.map((file) => ({ fileId: file.id, fileName: file.name ?? file.id })),
            nestedFolders: [],
        };
    }

    alreadyProcessedFolders.set(folderId, folder);
    appendFileSync('./foldersProcessed.txt', `${folderId}\t${JSON.stringify(folder)}\n`, {
        encoding: 'utf8',
    });

    return folder;
};

const downloadFolder = async (
    client: BoxClient,
    folder: Folder,
    downloadPath: string,
    alreadyDownloadedFileIds: Set<string>,
) => {
    if (folder.nestedFolders) {
        for (const nestedFolder of folder.nestedFolders) {
            await downloadFolder(
                client,
                nestedFolder,
                `${downloadPath}/${folder.folderName}`,
                alreadyDownloadedFileIds,
            );
        }
    }

    if (!existsSync(downloadPath)) {
        mkdirSync(downloadPath, { recursive: true });
    }

    for (const file of folder.files) {
        if (alreadyDownloadedFileIds.has(file.fileId)) {
            console.log(`Skipping already downloaded file with ID '${file.fileId}': '${file.fileName}'`);
            continue;
        }

        let requestsRemaining = await getRequestsRemaining();
        console.log(
            `Downloading folder with ID ${folder.folderId}. Requests remaining this minute: ${requestsRemaining}`,
        );
        while (requestsRemaining <= 0) {
            console.log(`Out of requests this minute; sleeping.`);
            await sleep(await getMsToRefresh());
            requestsRemaining = await getRequestsRemaining();
        }

        const qualifiedPath = `${downloadPath}/${file.fileName.replace(/[/\\?%*:|"<>]/g, '-')}`;
        const stream = createWriteStream(qualifiedPath);

        console.log(`Downloading file '${file.fileName.replace(/[/\\?%*:|"<>]/g, '-')}' with ID '${file.fileId}'.`);
        RequestsRemainingThisMinute -= 1;
        try {
            const download = await client.downloads.downloadFile(file.fileId);
            download.pipe(stream);
            stream.on('finish', () => {
                stream.close();
                appendFileSync('./filesDownloaded.txt', `${file.fileId}\n`, { encoding: 'utf8' });
            });
        } catch (err: any) {
            console.log(
                `Failed to download file with ID ${file.fileId}: ${file.fileName}. Writing this ID to the "skipped files" document.`,
                err,
            );
            appendFileSync('./filesSkipped.txt', `${file.fileId}\t${qualifiedPath}\t${JSON.stringify(err)}\n`, {
                encoding: 'utf8',
            });
        }
    }
};

const main = async () => {
    const auth = new BoxDeveloperTokenAuth({ token: process.env.BOX_DEVELOPER_TOKEN });
    const client = new BoxClient({ auth });

    if (!existsSync('./foldersProcessed.txt')) {
        writeFileSync('./foldersProcessed.txt', '', { encoding: 'utf8' });
    }
    const alreadyProcessedFoldersSerialized = readFileSync('./foldersProcessed.txt', { encoding: 'utf8' });
    const alreadyProcessedFolders = new Map<string, Folder>(
        alreadyProcessedFoldersSerialized
            .split('\n')
            .filter((line) => !!line)
            .map((line) => {
                const [folderId, folderSerialized] = line.split('\t') as [string, string];
                return [folderId, JSON.parse(folderSerialized) as Folder];
            }),
    );

    if (!existsSync('./filesDownloaded.txt')) {
        writeFileSync('./filesDownloaded.txt', '', { encoding: 'utf8' });
    }
    if (!existsSync('./filesSkipped.txt')) {
        writeFileSync('./filesSkipped.txt', '', { encoding: 'utf8' });
    }
    const alreadyDownloadedFileIdsSerialized = readFileSync('./filesDownloaded.txt', { encoding: 'utf8' });
    const alreadyDownloadedFileIds = alreadyDownloadedFileIdsSerialized.split('\n').filter((line) => !!line);

    const boxFolder = await client.folders.getFolderById('0');
    const rootFolder = await getFolder(client, boxFolder.id, alreadyProcessedFolders, boxFolder.name ?? boxFolder.id);
    writeFileSync('./serializedFileSystem.json', JSON.stringify(rootFolder, null, 2), { encoding: 'utf8' });
    console.log('Done fetching folder and file names, paths, and ids!');

    await downloadFolder(client, rootFolder, './downloads', new Set(alreadyDownloadedFileIds));
    console.log('Done downloading!');
};

main();
