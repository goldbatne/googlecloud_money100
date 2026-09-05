const { google } = require('googleapis');
const delay = ms => new Promise(res => setTimeout(res, ms));

const oauth2Client = new google.auth.OAuth2(
    process.env.GCP_CLIENT_ID,
    process.env.GCP_CLIENT_SECRET,
    'https://developers.google.com/oauthplayground'
);
oauth2Client.setCredentials({ refresh_token: process.env.GCP_REFRESH_TOKEN });
const drive = google.drive({ version: 'v3', auth: oauth2Client });

const SOURCE_ROOT_ID = process.env.GDRIVE_FOLDER_ID; // 원본
const PUBLIC_GDRIVE_FOLDER_ID = process.env.PUBLIC_GDRIVE_FOLDER_ID; // 대외 공유용 쇼룸 (변수명 통일)

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

async function main() {
    console.log("👔 [대외 쇼룸] 완전히 독립된 포맷 분리형(Day_Format) 엔진 가동");
    const now = new Date();
    now.setHours(now.getHours() + 9);
    
    const yearStr = String(now.getFullYear()) + "년"; 
    const monthStr = String(now.getMonth() + 1).padStart(2, '0') + "월"; 
    const dayStr = String(now.getDate()).padStart(2, '0') + "일"; 

    if (!PUBLIC_GDRIVE_FOLDER_ID) {
        console.error("❌ PUBLIC_GDRIVE_FOLDER_ID 시크릿이 없습니다. 엔진을 정지합니다.");
        process.exit(1);
    }

    try {
        // 1. 쇼룸 최상위는 순수한 연/월 2-Depth 유지
        const tgtYearId = await getOrCreateFolder(yearStr, PUBLIC_GDRIVE_FOLDER_ID);
        const tgtMonthId = await getOrCreateFolder(monthStr, tgtYearId);

        // ★ 오직 pdf와 html만 타겟팅 (md 배제)
        const formats = ['html'];
        
        for (const format of formats) {
            console.log(`\n🔍 [${format.toUpperCase()}] 포맷을 분리 전시합니다...`);
            
            // 2. 원본 경로 역추적
            const srcYearId = await getFolderIdByNameAndParent(yearStr, SOURCE_ROOT_ID);
            if (!srcYearId) continue;
            const srcFormatId = await getFolderIdByNameAndParent(`${yearStr}_${format}`, srcYearId);
            if (!srcFormatId) continue;
            const srcMonthId = await getFolderIdByNameAndParent(`${monthStr}_${format}`, srcFormatId);
            if (!srcMonthId) continue;
            const srcDayId = await getFolderIdByNameAndParent(`${dayStr}_${format}`, srcMonthId);
            if (!srcDayId) continue;

            // 3. ★ 핵심 로직: 월(Month) 폴더 바로 아래에 '26일_pdf', '26일_html' 분리 생성
            const tgtDayFormatId = await getOrCreateFolder(`${dayStr}_${format}`, tgtMonthId);

            // 4. 스크래핑 및 복사 실행
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

            let copyCount = 0;
            for (const file of filesToCopy) {
                let success = false;
                for (let retry = 1; retry <= 3; retry++) {
                    try {
                        await drive.files.copy({
                            fileId: file.id,
                            requestBody: { name: file.name, parents: [tgtDayFormatId] }
                        });
                        success = true;
                        process.stdout.write(format === 'pdf' ? 'P' : 'H'); 
                        await delay(1000); 
                        break; 
                    } catch (err) {
                        if (retry < 3) await delay(2000);
                    }
                }
                if (success) copyCount++;
            }
            console.log(`\n  -> ✅ [${format.toUpperCase()}] 쇼룸 전시 완료: ${copyCount}/${filesToCopy.length}개 성공`);
        }
        console.log(`\n🎉 대외 쇼룸 독립 세팅이 성공적으로 종료되었습니다.`);
    } catch (e) {
        console.error("치명적 복사 에러:", e);
        process.exit(1);
    }
}
main();
