import { BoxClient, BoxDeveloperTokenAuth } from 'box-typescript-sdk-gen';
import { writeFileSync, appendFileSync, readFileSync, createWriteStream, existsSync, mkdirSync } from 'fs';
import * as dotenv from 'dotenv';
import { FileFullOrFolderMiniOrWebLink } from 'box-typescript-sdk-gen/lib/schemas/fileFullOrFolderMiniOrWebLink.generated';
import * as cliProgress from 'cli-progress';
dotenv.config();

const ROOT_FOLDER = '0'; // 43489403829
const REFRESH_TIME = 61 * 1000;
const REQUESTS_PER_MINUTE = 950;
const BOX_SEARCH_LIMIT = 1000;
let RequestsRemainingThisMinute = REQUESTS_PER_MINUTE;
let MinuteStart = Date.now();
let FolderDownloadsCompleted = 0;
let FileDownloadSkipCount = 0;
let FileDownloadErrorCount = 0;

const dumpCachedFolderRequests = () => {
    const forDump: [string, FileFullOrFolderMiniOrWebLink[]][] = [];
    for (const keyValuePair of CachedFolderRequests?.entries() ?? []) {
        forDump.push(keyValuePair);
    }

    writeFileSync('./folderRequestCache.json', JSON.stringify(forDump));
};

const readCachedFolderRequests = (): Map<string, FileFullOrFolderMiniOrWebLink[]> => {
    const fileContents = readFileSync('./folderRequestCache.json', { encoding: 'utf8' });
    const fromDumpArray: [string, FileFullOrFolderMiniOrWebLink[]][] = JSON.parse(fileContents);
    const cachedFolderRequests = new Map<string, FileFullOrFolderMiniOrWebLink[]>();
    for (const keyValuePair of fromDumpArray) {
        const key = keyValuePair[0];
        const value = keyValuePair[1];
        cachedFolderRequests.set(key, value);
    }

    return cachedFolderRequests;
};

const CachedFolderRequests = existsSync('./folderRequestCache.json')
    ? readCachedFolderRequests()
    : new Map<string, FileFullOrFolderMiniOrWebLink[]>();

const FOLDER_REQUEST_CACHE_INTERVAL = setInterval(dumpCachedFolderRequests, 10_000);

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

const getFolderRequestCacheKey = (folderId: string, offset: number) => `${folderId}${offset}`;

const getBoxFolderItems = async (client: BoxClient, folderId: string, offset: number) => {
    const cacheKey = getFolderRequestCacheKey(folderId, offset);
    if (CachedFolderRequests.has(cacheKey)) {
        console.log('Returning cached response.');
        return CachedFolderRequests.get(cacheKey)!;
    }

    console.log(`Fetching batch with offset: ${offset}.`);
    // This ridiculous line of code helps "trick" the JS scheduler into not queueing a hundred thousand promises in parallel in the same nanosecond.
    // It's not pretty, but it seems to do the trick to avoid blowing past rate limits due to threaded execution order.
    await sleep(Math.random() * 1000);
    while ((await getRequestsRemaining()) <= 0) {
        console.log(`Out of requests this minute; sleeping.`);
        await sleep(await getMsToRefresh());
    }

    console.log(`Making new request in folder: ${folderId}`);
    RequestsRemainingThisMinute -= 1;
    const allEntries = await (async () =>
        (
            await client.folders.getFolderItems(folderId, {
                queryParams: { limit: BOX_SEARCH_LIMIT, offset },
            })
        ).entries || [])();

    CachedFolderRequests.set(cacheKey, allEntries as FileFullOrFolderMiniOrWebLink[]);

    return allEntries;
};

const getFolder = async (
    client: BoxClient,
    folderId: string,
    alreadyProcessedFolders: Map<string, Folder>,
    folderName: string,
): Promise<Folder> => {
    if (alreadyProcessedFolders.has(folderId)) {
        console.log(`-- 🟢 Returning previously processed folder with ID ${folderId}.`);
        return alreadyProcessedFolders.get(folderId)!;
    }

    console.log(`-- 🟠 Processing folder with ID ${folderId}.`);
    const allEntries: FileFullOrFolderMiniOrWebLink[] = [];
    let offset = 0;
    let allEntriesBatch = await getBoxFolderItems(client, folderId, offset);
    while (allEntriesBatch && allEntriesBatch.length) {
        allEntries.push(...allEntriesBatch);
        offset += BOX_SEARCH_LIMIT;
        allEntriesBatch = await getBoxFolderItems(client, folderId, offset);
        if (allEntriesBatch.length < BOX_SEARCH_LIMIT) break;
    }
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

    console.log(`-- Done downloading folder with ID ${folderId}!`);
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
    await sleep(Math.random() * 1000);
    if (folder.nestedFolders) {
        const promises = [];
        for (const nestedFolder of folder.nestedFolders) {
            promises.push(
                downloadFolder(
                    client,
                    nestedFolder,
                    `${downloadPath}/${nestedFolder.folderName}`,
                    alreadyDownloadedFileIds,
                ),
            );
        }
        await Promise.all(promises);
    }

    if (!existsSync(downloadPath)) {
        mkdirSync(downloadPath, { recursive: true });
    }

    for (const file of folder.files) {
        if (alreadyDownloadedFileIds.has(file.fileId)) {
            continue;
        }

        while ((await getRequestsRemaining()) <= 0) {
            await sleep(await getMsToRefresh());
        }

        const qualifiedPath = `${downloadPath}/${file.fileName.replace(/[/\\?%*:|"<>]/g, '-')}`;
        const stream = createWriteStream(qualifiedPath);

        RequestsRemainingThisMinute -= 1;
        try {
            const download = await client.downloads.downloadFile(file.fileId);
            download.pipe(stream);
            stream.on('finish', () => {
                stream.close();
                appendFileSync('./filesDownloaded.txt', `${file.fileId}\n`, { encoding: 'utf8' });
            });
        } catch (err: any) {
            appendFileSync('./filesSkipped.txt', `${file.fileId}\t${qualifiedPath}\t${JSON.stringify(err)}\n`, {
                encoding: 'utf8',
            });
        } finally {
            alreadyDownloadedFileIds.add(file.fileId);
        }
    }

    FolderDownloadsCompleted += 1;
};

const countFoldersAndFiles = (folder: Folder): { fileCount: number; folderCount: number } => {
    let fileCount = folder.files.length;
    let folderCount = folder.nestedFolders.length;

    for (const nested of folder.nestedFolders) {
        const nestedCount = countFoldersAndFiles(nested);
        fileCount += nestedCount.fileCount;
        folderCount += nestedCount.folderCount;
    }

    return { fileCount, folderCount };
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
    if (!existsSync('./folderRequestCache.json')) {
        writeFileSync('./folderRequestCache.json', '', { encoding: 'utf8' });
    }
    const alreadyDownloadedFileIdsSerialized = readFileSync('./filesDownloaded.txt', { encoding: 'utf8' });
    const alreadyDownloadedFileIds = alreadyDownloadedFileIdsSerialized.split('\n').filter((line) => !!line);

    console.log('Fetching root folder.\n');
    const boxFolder = await client.folders.getFolderById(ROOT_FOLDER);
    console.log('Beginning recursive folder scrape.');
    const rootFolder = await getFolder(client, boxFolder.id, alreadyProcessedFolders, boxFolder.name ?? boxFolder.id);
    writeFileSync('./serializedFileSystem.json', JSON.stringify(rootFolder, null, 2), { encoding: 'utf8' });
    console.log('Done fetching folder and file names, paths, and ids! Also done updating the folder request cache.\n');
    clearInterval(FOLDER_REQUEST_CACHE_INTERVAL);

    console.log('Downloading all file IDs not already present in filesDownloaded.txt.');
    const { fileCount, folderCount } = countFoldersAndFiles(rootFolder);
    const multiBar = new cliProgress.MultiBar(
        {
            etaBuffer: 1000,
            fps: 10,
            clearOnComplete: false,
            hideCursor: true,
            format: ' {bar} | {duration_formatted} elapsed, ~{eta_formatted} remaining; {value}/{total} {title} - {percentage}% -- {extra}',
        },
        cliProgress.Presets.shades_classic,
    );
    const fileProgressBar = multiBar.create(fileCount, 0, { title: 'files', percent: 0, extra: '' });
    const folderProgressBar = multiBar.create(folderCount, 0, { title: 'folders', percent: 0, extra: '' });
    const alreadyDownloadedFileIdsSet = new Set(alreadyDownloadedFileIds);

    const getFileDownloadsCompleted = async () => alreadyDownloadedFileIdsSet.size;
    const getFolderDownloadsCompleted = async () => FolderDownloadsCompleted;
    const progressUpdateInterval = setInterval(async () => {
        const fileCountProgress =
            (await getFileDownloadsCompleted()) <= fileCount ? await getFileDownloadsCompleted() : fileCount;
        fileProgressBar.update(fileCountProgress, {
            title: 'files',
            percent: Math.round((fileCountProgress / fileCount) * 10_000) / 100,
            extra: ` | ${FileDownloadSkipCount} skipped, ${FileDownloadErrorCount} errors`,
        });

        getFolderDownloadsCompleted();
        const folderCountProgress =
            (await getFolderDownloadsCompleted()) <= folderCount ? await getFolderDownloadsCompleted() : folderCount;
        folderProgressBar.update(folderCountProgress, {
            title: 'folders',
            percent: Math.round((folderCountProgress / folderCount) * 10_000) / 100,
            extra: '',
        });
    }, 250);

    await downloadFolder(client, rootFolder, './downloads', alreadyDownloadedFileIdsSet);
    console.log('Done downloading!');
    clearInterval(progressUpdateInterval);
};

main();
