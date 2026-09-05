const { google } = require('googleapis');

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

function requireEnv(name) {
    const value = process.env[name];
    if (!value) {
        throw new Error(`필수 시크릿 ${name} 이(가) 설정되지 않았습니다.`);
    }
    return value;
}

const oauth2Client = new google.auth.OAuth2(
    requireEnv('GCP_CLIENT_ID'),
    requireEnv('GCP_CLIENT_SECRET'),
    'https://developers.google.com/oauthplayground'
);

oauth2Client.setCredentials({
    refresh_token: requireEnv('GCP_REFRESH_TOKEN')
});

const drive = google.drive({ version: 'v3', auth: oauth2Client });
const SOURCE_ROOT_ID = requireEnv('GDRIVE_FOLDER_ID');
const TARGET_ROOT_ID = requireEnv('TARGET_GDRIVE_FOLDER_ID');

function getKstDateStrings() {
    const parts = new Intl.DateTimeFormat('en-US', {
        timeZone: 'Asia/Seoul',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    }).formatToParts(new Date());

    const get = type => parts.find(part => part.type === type).value;

    return {
        yearStr: `${get('year')}년`,
        monthStr: `${get('month')}월`,
        dayStr: `${get('day')}일`
    };
}

function escapeQueryValue(value) {
    return String(value)
        .replace(/\\/g, '\\\\')
        .replace(/'/g, "\\'");
}

async function getFolderIdByNameAndParent(folderName, parentId) {
    const safeName = escapeQueryValue(folderName);
    const safeParent = escapeQueryValue(parentId);

    const res = await drive.files.list({
        q: `name = '${safeName}' and '${safeParent}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
        fields: 'files(id, name)',
        pageSize: 10
    });

    return res.data.files[0]?.id || null;
}

async function getOrCreateFolder(folderName, parentId) {
    const existingId = await getFolderIdByNameAndParent(folderName, parentId);
    if (existingId) return existingId;

    const folder = await drive.files.create({
        requestBody: {
            name: folderName,
            mimeType: 'application/vnd.google-apps.folder',
            parents: [parentId]
        },
        fields: 'id'
    });

    if (!folder.data.id) {
        throw new Error(`타겟 폴더 생성 실패: ${folderName}`);
    }

    return folder.data.id;
}

async function listFilesInFolder(folderId) {
    let pageToken = null;
    const files = [];

    do {
        const res = await drive.files.list({
            q: `'${escapeQueryValue(folderId)}' in parents and trashed = false`,
            fields: 'nextPageToken, files(id, name, mimeType)',
            pageToken,
            pageSize: 1000
        });

        files.push(...res.data.files);
        pageToken = res.data.nextPageToken;
    } while (pageToken);

    return files;
}

async function copyFilesForFormat(format, yearStr, monthStr, dayStr) {
    console.log(`\n🔍 [${format.toUpperCase()}] 복사 시작`);

    // 원본: 연도 → 연도_포맷 → 월_포맷 → 일_포맷
    const srcYearId = await getFolderIdByNameAndParent(yearStr, SOURCE_ROOT_ID);
    if (!srcYearId) throw new Error(`원본 연도 폴더 없음: ${yearStr}`);

    const srcFormatName = `${yearStr}_${format}`;
    const srcFormatId = await getFolderIdByNameAndParent(srcFormatName, srcYearId);
    if (!srcFormatId) throw new Error(`원본 포맷 폴더 없음: ${srcFormatName}`);

    const srcMonthName = `${monthStr}_${format}`;
    const srcMonthId = await getFolderIdByNameAndParent(srcMonthName, srcFormatId);
    if (!srcMonthId) throw new Error(`원본 월 폴더 없음: ${srcMonthName}`);

    const srcDayName = `${dayStr}_${format}`;
    const srcDayId = await getFolderIdByNameAndParent(srcDayName, srcMonthId);
    if (!srcDayId) throw new Error(`원본 일 폴더 없음: ${srcDayName}`);

    const sourceFiles = (await listFilesInFolder(srcDayId))
        .filter(file => file.mimeType !== 'application/vnd.google-apps.folder');

    if (sourceFiles.length === 0) {
        throw new Error(
            `[${format.toUpperCase()}] 원본 파일이 0개입니다. 원본 삭제를 막기 위해 중단합니다.`
        );
    }

    // 사본도 기존 4-Depth 구조 유지
    const tgtYearId = await getOrCreateFolder(yearStr, TARGET_ROOT_ID);
    const tgtFormatId = await getOrCreateFolder(srcFormatName, tgtYearId);
    const tgtMonthId = await getOrCreateFolder(srcMonthName, tgtFormatId);
    const tgtDayId = await getOrCreateFolder(srcDayName, tgtMonthId);

    // 재실행 시 같은 이름의 파일은 중복 복사하지 않음
    const targetFiles = (await listFilesInFolder(tgtDayId))
        .filter(file => file.mimeType !== 'application/vnd.google-apps.folder');

    const existingNames = new Set(targetFiles.map(file => file.name));

    let successCount = 0;
    let copiedCount = 0;
    let skippedCount = 0;
    const failedFiles = [];

    console.log(`  -> 📂 원본 ${sourceFiles.length}개 확인`);

    for (const file of sourceFiles) {
        if (existingNames.has(file.name)) {
            successCount++;
            skippedCount++;
            process.stdout.write('S');
            continue;
        }

        let copied = false;

        for (let retry = 1; retry <= 3; retry++) {
            try {
                await drive.files.copy({
                    fileId: file.id,
                    requestBody: {
                        name: file.name,
                        parents: [tgtDayId]
                    }
                });

                copied = true;
                successCount++;
                copiedCount++;
                existingNames.add(file.name);

                process.stdout.write('.');
                await delay(1000);
                break;
            } catch (err) {
                if (retry === 3) {
                    failedFiles.push(`${file.name}: ${err.message}`);
                } else {
                    await delay(2000);
                }
            }
        }

        if (!copied) {
            process.stdout.write('X');
        }
    }

    console.log(
        `\n  -> ✅ [${format.toUpperCase()}] 확인 완료: ${successCount}/${sourceFiles.length}`
    );
    console.log(
        `  -> 신규 복사 ${copiedCount}개 / 기존 파일 스킵 ${skippedCount}개`
    );

    if (failedFiles.length > 0 || successCount !== sourceFiles.length) {
        throw new Error(
            `[${format.toUpperCase()}] 일부 파일 복사 실패. ` +
            `성공 ${successCount}/${sourceFiles.length}\n` +
            failedFiles.join('\n')
        );
    }
}

async function main() {
    console.log('🖨️ [AI 학습용] MD 전용 Drive 복사 엔진 가동');

    const { yearStr, monthStr, dayStr } = getKstDateStrings();

    try {
        // AI 학습용 사본에는 MD만 저장
        await copyFilesForFormat('md', yearStr, monthStr, dayStr);

        console.log('\n🎉 MD 사본 복사가 정상 완료되었습니다.');
    } catch (err) {
        console.error('\n❌ MD 복사 실패:', err.message || err);
        process.exit(1);
    }
}

main();
