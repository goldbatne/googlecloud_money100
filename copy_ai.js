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
const AI_WORKSPACE_ID = process.env.AI_WORKSPACE_FOLDER_ID; // AI 전용 샌드박스 폴더

// 폴더 검색 전용 헬퍼 (타겟 폴더 생성 로직은 삭제됨 - Flat 구조 유지)
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

async function main() {
    console.log("🤖 [AI 워크스페이스] MD 파일 전용 플랫(Flat) 덤프 엔진 가동");
    const now = new Date();
    now.setHours(now.getHours() + 9); // KST
    
    // 타겟팅할 원본 폴더명 조립
    const yearStr = String(now.getFullYear()) + "년"; 
    const monthStr = String(now.getMonth() + 1).padStart(2, '0') + "월"; 
    const dayStr = String(now.getDate()).padStart(2, '0') + "일"; 

    if (!AI_WORKSPACE_ID) {
        console.error("❌ AI_WORKSPACE_FOLDER_ID 시크릿이 설정되지 않았습니다. 엔진을 정지합니다.");
        process.exit(1);
    }

    try {
        // 1. 원본 MD 경로 핀포인트 추적
        const srcYearId = await getFolderIdByNameAndParent(yearStr, SOURCE_ROOT_ID);
        if (!srcYearId) { console.log(`  -> ⚠️ 원본 연도 폴더 없음. 스킵.`); return; }
        
        const srcFormatId = await getFolderIdByNameAndParent(`${yearStr}_md`, srcYearId);
        if (!srcFormatId) { console.log(`  -> ⚠️ 원본 MD 폴더 없음. 스킵.`); return; }
        
        const srcMonthId = await getFolderIdByNameAndParent(`${monthStr}_md`, srcFormatId);
        if (!srcMonthId) { console.log(`  -> ⚠️ 원본 월 폴더 없음. 스킵.`); return; }
        
        const srcDayId = await getFolderIdByNameAndParent(`${dayStr}_md`, srcMonthId);
        if (!srcDayId) { console.log(`  -> ⚠️ 원본 일(${dayStr}_md) 폴더 없음. 오늘 생성된 데이터가 없습니다.`); return; }

        // 2. 해당 폴더의 MD 파일 목록 스크래핑
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

        console.log(`  -> 📂 총 ${filesToCopy.length}개의 MD 파일을 발견했습니다. AI 샌드박스 적재를 시작합니다.`);

        // 3. 복사 실행 (AI_WORKSPACE_ID 최상위에 폴더 없이 그대로 투척)
        let copyCount = 0;
        for (const file of filesToCopy) {
            let success = false;
            for (let retry = 1; retry <= 3; retry++) {
                try {
                    await drive.files.copy({
                        fileId: file.id,
                        requestBody: { name: file.name, parents: [AI_WORKSPACE_ID] }
                    });
                    success = true;
                    process.stdout.write(`*`); // AI 복사 진행률 (별표)
                    await delay(1000); // API 레이트 리밋 방어 (1초)
                    break; 
                } catch (err) {
                    if (retry === 3) {
                        console.error(`\n  -> ❌ AI 복사 최종 실패 [${file.name}]: ${err.message}`);
                    } else {
                        await delay(2000);
                    }
                }
            }
            if (success) copyCount++;
        }
        console.log(`\n🎉 AI 전용 적재 완료: ${copyCount}/${filesToCopy.length}개 성공`);

    } catch (e) {
        console.error("치명적 복사 에러:", e);
        process.exit(1);
    }
}
main();
