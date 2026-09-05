const { google } = require('googleapis');
const delay = ms => new Promise(res => setTimeout(res, ms));

const oauth2Client = new google.auth.OAuth2(
    process.env.GCP_CLIENT_ID,
    process.env.GCP_CLIENT_SECRET,
    'https://developers.google.com/oauthplayground'
);
oauth2Client.setCredentials({ refresh_token: process.env.GCP_REFRESH_TOKEN });
const drive = google.drive({ version: 'v3', auth: oauth2Client });

const SOURCE_ROOT_ID = process.env.GDRIVE_FOLDER_ID; // 원본 최상위 폴더
const TARGET_ROOT_ID = process.env.TARGET_GDRIVE_FOLDER_ID; // 사본 최상위 폴더 (새로 추가한 시크릿)

// 폴더 검색 전용 헬퍼 함수
async function getFolderIdByNameAndParent(folderName, parentId) {
    try {
        const res = await drive.files.list({
            q: `name = '${folderName}' and '${parentId}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
            fields: 'files(id)',
        });
        if (res.data.files.length > 0) return res.data.files[0].id;
        return null;
    } catch (err) { return null; }
}

// 폴더 생성/검색 헬퍼 함수 (타겟용)
async function getOrCreateFolder(folderName, parentId) {
    try {
        let folderId = await getFolderIdByNameAndParent(folderName, parentId);
        if (folderId) return folderId;
        const folder = await drive.files.create({
            resource: { name: folderName, mimeType: 'application/vnd.google-apps.folder', parents: [parentId] },
            fields: 'id',
        });
        return folder.data.id;
    } catch (err) { return parentId; }
}

async function copyFilesForFormat(format, yearStr, monthStr, dayStr) {
    console.log(`\n🔍 [${format.toUpperCase()}] 포맷 복사 프로세스 시작...`);
    
    // 1. 원본 경로 추적
    const srcYearId = await getFolderIdByNameAndParent(yearStr, SOURCE_ROOT_ID);
    if (!srcYearId) { console.log(`  -> ⚠️ 원본 연도 폴더 없음. 스킵.`); return; }
    
    const srcFormatId = await getFolderIdByNameAndParent(`${yearStr}_${format}`, srcYearId);
    if (!srcFormatId) { console.log(`  -> ⚠️ 원본 포맷 폴더 없음. 스킵.`); return; }
    
    const srcMonthId = await getFolderIdByNameAndParent(`${monthStr}_${format}`, srcFormatId);
    if (!srcMonthId) { console.log(`  -> ⚠️ 원본 월 폴더 없음. 스킵.`); return; }
    
    const srcDayId = await getFolderIdByNameAndParent(`${dayStr}_${format}`, srcMonthId);
    if (!srcDayId) { console.log(`  -> ⚠️ 원본 일(${dayStr}_${format}) 폴더 없음. 오늘 생성된 데이터가 없습니다.`); return; }

    // 2. 타겟 경로 구축 (동일한 4-Depth 하이라키 유지)
    const tgtYearId = await getOrCreateFolder(yearStr, TARGET_ROOT_ID);
    const tgtFormatId = await getOrCreateFolder(`${yearStr}_${format}`, tgtYearId);
    const tgtMonthId = await getOrCreateFolder(`${monthStr}_${format}`, tgtFormatId);
    const tgtDayId = await getOrCreateFolder(`${dayStr}_${format}`, tgtMonthId);

    // 3. 원본 파일 목록 가져오기
    let pageToken = null;
    let filesToCopy = [];
    do {
        const res = await drive.files.list({
            q: `'${srcDayId}' in parents and trashed = false`,
            fields: 'nextPageToken, files(id, name)',
            pageToken: pageToken,
            pageSize: 100
        });
        filesToCopy = filesToCopy.concat(res.data.files);
        pageToken = res.data.nextPageToken;
    } while (pageToken);

    console.log(`  -> 📂 총 ${filesToCopy.length}개의 원본 파일을 발견했습니다. 복사를 시작합니다.`);

    // 4. 복사 실행 (★ 업그레이드: 구글 서버 Internal Error 방어용 3회 재시도 로직 장착)
    let copyCount = 0;
    for (const file of filesToCopy) {
        let success = false;
        
        for (let retry = 1; retry <= 3; retry++) {
            try {
                await drive.files.copy({
                    fileId: file.id,
                    requestBody: { name: file.name, parents: [tgtDayId] } // 원본 이름 그대로, 타겟 폴더에 생성
                });
                success = true;
                process.stdout.write(`.`); // 진행 상황 점 찍기
                await delay(1000); // 구글 드라이브 쓰기 API 제한 방어선 (1초)
                break; // 성공 시 재시도 루프 탈출
            } catch (err) {
                if (retry === 3) {
                    console.error(`\n  -> ❌ 복사 최종 실패 [${file.name}]: ${err.message}`);
                } else {
                    // 구글 서버 일시적 에러 시 2초 대기 후 재시도
                    await delay(2000);
                }
            }
        }
        if (success) copyCount++;
    }
    console.log(`\n  -> ✅ [${format.toUpperCase()}] 복사 완료: ${copyCount}/${filesToCopy.length}개 성공`);
}

async function main() {
    console.log("🖨️ 구글 드라이브 일일 데이터 미러링(복사) 엔진 가동");
    const now = new Date();
    now.setHours(now.getHours() + 9); // KST
    const yearStr = String(now.getFullYear()) + "년"; 
    const monthStr = String(now.getMonth() + 1).padStart(2, '0') + "월"; 
    const dayStr = String(now.getDate()).padStart(2, '0') + "일"; 

    if (!TARGET_ROOT_ID) {
        console.error("❌ TARGET_GDRIVE_FOLDER_ID 시크릿이 설정되지 않았습니다.");
        process.exit(1);
    }

    try {
        const formats = ['md'];
        for (const fmt of formats) {
            await copyFilesForFormat(fmt, yearStr, monthStr, dayStr);
        }
        console.log("\n🎉 모든 포맷의 복사 작업이 성공적으로 종료되었습니다.");
    } catch (e) {
        console.error("치명적 복사 에러:", e);
        process.exit(1);
    }
}
main();
