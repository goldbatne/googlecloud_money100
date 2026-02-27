const { google } = require('googleapis'); 
const { GoogleGenerativeAI } = require('@google/generative-ai');
const stream = require('stream');
const zlib = require('zlib');
const { mdToPdf } = require('md-to-pdf'); 
const { marked } = require('marked');

const oauth2Client = new google.auth.OAuth2(
    process.env.GCP_CLIENT_ID,
    process.env.GCP_CLIENT_SECRET,
    'https://developers.google.com/oauthplayground'
);
oauth2Client.setCredentials({ refresh_token: process.env.GCP_REFRESH_TOKEN });
const drive = google.drive({ version: 'v3', auth: oauth2Client });

const ROOT_FOLDER_ID = process.env.GDRIVE_FOLDER_ID;

const START_RANK = parseInt(process.env.START_RANK || '1', 10); 
const END_RANK = parseInt(process.env.END_RANK || '50', 10);
const MAX_RETRIES = 3; 

const apiKeys = (process.env.GEMINI_API_KEY || '').split(',').map(k => k.trim()).filter(k => k.length > 0);
const delay = ms => new Promise(res => setTimeout(res, ms));

async function getOrCreateFolder(folderName, parentId) {
    try {
        const res = await drive.files.list({
            q: `name = '${folderName}' and '${parentId}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
            fields: 'files(id)',
        });
        if (res.data.files.length > 0) return res.data.files[0].id;
        const folder = await drive.files.create({
            resource: { name: folderName, mimeType: 'application/vnd.google-apps.folder', parents: [parentId] },
            fields: 'id',
        });
        return folder.data.id;
    } catch (err) { return parentId; }
}

function getKrokiUrl(text) {
    const data = Buffer.from(text, 'utf8');
    const compressed = zlib.deflateSync(data);
    const base64 = compressed.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
    return `https://kroki.io/mermaid/svg/${base64}`;
}

function sanitizeMermaid(rawCode) {
    let fixed = rawCode.replace(/[\u200B-\u200D\uFEFF]/g, ''); 
    fixed = fixed.replace(/\/\/.*$/gm, '').replace(/%%.*$/gm, '').trim();
    fixed = fixed.replace(/["'*#]/g, ''); 
    fixed = fixed.replace(/^\s*(\d+\.|[-*])\s+/gm, ''); 

    if (fixed.match(/^erDiagram/i)) {
        let lines = fixed.split('\n');
        let inEntity = false;
        for(let i=0; i<lines.length; i++) {
            let l = lines[i].trim();
            if(l.includes('{')) inEntity = true;
            else if(l.includes('}')) inEntity = false;
            else if(inEntity && l.length > 0) {
                let words = l.split(/\s+/).filter(w => w);
                if(words.length >= 2) lines[i] = "    " + words[0] + " " + words[1];
                else lines[i] = ""; 
            }
        }
        fixed = lines.join('\n');
        fixed = fixed.replace(/erDiagram\s+(.*)/i, 'erDiagram\n$1');
        fixed = fixed.replace(/\(.*?\)/g, ''); 
        fixed = fixed.replace(/,/g, '\n'); 
        fixed = fixed.replace(/\bENUM\b/gi, ''); 
        fixed = fixed.replace(/\b(PK|FK|UK|Optional)\b/gi, ''); 
        fixed = fixed.replace(/^[a-zA-Z가-힣0-9_]+\s*:\s*(?=[a-zA-Z0-9_]+\s*\|\|--)/gm, '');
        fixed = fixed.replace(/(\|\|--o{|}\|--\|{|}\|--o{|\|\|--\|{|}-o|}-\||-o|-\|)\s*([a-zA-Z0-9_]+)\s*:\s*(.*?)$/gm, '$1 $2 : "has"');
        return fixed;
    }

    fixed = fixed.replace(/\(\[/g, '[').replace(/\]\)/g, ']');
    fixed = fixed.replace(/\[\[/g, '[').replace(/\]\]/g, ']');
    fixed = fixed.replace(/\(\(/g, '(').replace(/\)\)/g, ')');
    fixed = fixed.replace(/--\[/g, '-->[');
    fixed = fixed.replace(/-\[/g, '->[');

    let lines = fixed.split('\n');
    let processedLines = [];
    let autoIdCount = 0;
    let nodeTexts = [];
    let edgeTexts = [];

    for (let l of lines) {
        l = l.trim();
        if (!l) continue;

        if (l.match(/^(graph|flowchart)\s+[a-zA-Z]+/i) || l.toLowerCase() === 'end') {
            processedLines.push(l); continue;
        }
        if (l.match(/^subgraph\s+(.*)/i)) {
            let name = l.replace(/^subgraph\s+/i, '').replace(/["']/g, '');
            processedLines.push(`subgraph "${name}"`); continue;
        }
        if (l.match(/(-->|-\.->|==>|---)\s*([^:]+?)\s*:\s*(.+)$/)) {
            l = l.replace(/(-->|-\.->|==>|---)\s*([^:]+?)\s*:\s*(.+)$/, "$1|$3| $2");
        }
        
        l = l.replace(/\|([^|]+)\|/g, (m, content) => {
            edgeTexts.push(content.replace(/["'\n]/g, ' ').trim()); return `|@@E${edgeTexts.length - 1}@@|`;
        });
        l = l.replace(/--\s*([^>|@]+?)\s*-->/g, (m, content) => {
            edgeTexts.push(content.replace(/["'\n]/g, ' ').trim()); return `-->|@@E${edgeTexts.length - 1}@@|`;
        });

        l = l.replace(/\[([^\]]+)\]/g, (m, content) => {
            nodeTexts.push(`["${content.replace(/["'\n]/g, ' ').trim()}"]`); return `@@N${nodeTexts.length - 1}@@`;
        });
        l = l.replace(/\{([^}]+)\}/g, (m, content) => {
            nodeTexts.push(`{"${content.replace(/["'\n]/g, ' ').trim()}"}`); return `@@N${nodeTexts.length - 1}@@`;
        });
        l = l.replace(/\(([^)]+)\)/g, (m, content) => {
            nodeTexts.push(`("${content.replace(/["'\n]/g, ' ').trim()}")`); return `@@N${nodeTexts.length - 1}@@`;
        });

        l = l.replace(/^(\s*)(@@N\d+@@)/, (m, space, pNode) => `${space}N_AUTO_${autoIdCount++}${pNode}`);
        l = l.replace(/(-->|-\.->|==>|---)\s*(@@N\d+@@)/g, (m, arrow, pNode) => `${arrow} N_AUTO_${autoIdCount++}${pNode}`);

        l = l.replace(/\s+/g, '');
        l = l.replace(/-->/g, ' --> ').replace(/-\.->/g, ' -.-> ').replace(/==>/g, ' ==> ').replace(/---/g, ' --- ').replace(/&/g, ' & '); 

        l = l.replace(/@@E(\d+)@@/g, (m, idx) => edgeTexts[parseInt(idx)]);
        l = l.replace(/@@N(\d+)@@/g, (m, idx) => nodeTexts[parseInt(idx)]);

        processedLines.push(l);
    }
    return processedLines.join('\n');
}

async function main() {
    if (apiKeys.length === 0) {
        console.error("❌ API 키가 등록되지 않았습니다.");
        process.exit(1);
    }

    try {
        const gplayModule = await import('google-play-scraper');
        const gplay = gplayModule.default || gplayModule;
        
        const rawGames = await gplay.list({ collection: gplay.collection.GROSSING, category: gplay.category.GAME, num: 100, country: 'kr', lang: 'ko' });
        const allGames = rawGames.map((game, index) => ({ ...game, actualRank: index + 1 }));

        const now = new Date();
        now.setHours(now.getHours() + 9);
        const dateString = now.toISOString().split('T')[0];
        const yearStr = String(now.getFullYear()) + "년"; 
        const monthStr = String(now.getMonth() + 1).padStart(2, '0') + "월"; 
        const dayStr = String(now.getDate()).padStart(2, '0') + "일"; 

        let successCount = 0;
        let skippedCount = 0;

        if (allGames.length > 0) {
            const mainYearId = await getOrCreateFolder(yearStr, ROOT_FOLDER_ID);
            
            const mdFormatId = await getOrCreateFolder(`${yearStr}_md`, mainYearId);
            const pdfFormatId = await getOrCreateFolder(`${yearStr}_pdf`, mainYearId);
            const htmlFormatId = await getOrCreateFolder(`${yearStr}_html`, mainYearId);

            const mdMonthId = await getOrCreateFolder(`${monthStr}_md`, mdFormatId);
            const pdfMonthId = await getOrCreateFolder(`${monthStr}_pdf`, pdfFormatId);
            const htmlMonthId = await getOrCreateFolder(`${monthStr}_html`, htmlFormatId);

            const mdFolderId = await getOrCreateFolder(`${dayStr}_md`, mdMonthId);
            const pdfFolderId = await getOrCreateFolder(`${dayStr}_pdf`, pdfMonthId);
            const htmlFolderId = await getOrCreateFolder(`${dayStr}_html`, htmlMonthId);

            const targetGames = allGames.slice(START_RANK - 1, END_RANK);
            
            console.log(`\n[${dateString}] 🗄️ 검색 탑재형 코어 병렬 엔진 가동 (${START_RANK}위 ~ ${END_RANK}위)`);
            
            for (let idx = 0; idx < targetGames.length; idx++) {
                const luckyGame = targetGames[idx];
                const luckyRank = luckyGame.actualRank; 
                
                let releaseDate = "정보 없음";
                try {
                    const appDetails = await gplay.app({ appId: luckyGame.appId });
                    releaseDate = appDetails.released || "정보 없음";
                } catch (e) {
                    console.log(`  -> ⚠️ 앱 상세 정보(출시일) 수집 실패. 스킵합니다.`);
                }
                
                const currentKey = apiKeys[idx % apiKeys.length];
                const genAI = new GoogleGenerativeAI(currentKey);
                
                const draftModel = genAI.getGenerativeModel({ 
                    model: "gemini-2.5-flash",
                    tools: [{ googleSearch: {} }],
                    systemInstruction: "당신은 15년 차 수석 게임 시스템 기획자입니다. 빈말이나 과도한 칭찬을 배제하고 사실 기반으로만 작성하십시오. 데이터가 부족한 양산형 게임의 경우 억지로 지어내지 말고 오직 [ABORT_NO_DATA]만 출력하십시오."
                });

                const qaModel = genAI.getGenerativeModel({
                    model: "gemini-2.5-flash",
                    systemInstruction: "당신은 감정이 없는 '엄격한 다이어그램 컴파일러'입니다. 기획적 의도, 설명, 마크다운(```) 기호 없이 오직 완벽하게 동작하는 Mermaid 순수 코드만 반환하십시오."
                });

                const categories = [
                    "핵심 BM (가챠/강화/패스 등 직접적 매출원)",
                    "장기 리텐션 (일일 숙제/업적/마일리지 등 접속 유지 장치)",
                    "소셜 및 경쟁 (길드/PvP/랭킹 등 유저 간 상호작용)",
                    "성장 및 경제 (재화 획득/소모처 및 인플레이션 제어 로직)",
                    "코어 게임플레이 (전투 공식/스테이지 기믹/퍼즐 등 조작의 재미)",
                    "캐릭터 및 전투 클래스 (스킬 메커니즘/시너지/상성 구조)",
                    "수치 및 전투 밸런스 (데미지 공식/스테이터스/성장 체감)",
                    "레벨 디자인 (맵 구조/동선/오브젝트 배치/몬스터 스폰)",
                    "세계관 및 시나리오 (퀘스트 라인/내러티브/NPC 상호작용)",
                    "핵심 콘텐츠 시스템 (레이드/던전/생활형 콘텐츠 등 주요 시스템)",
                    "UI/UX 및 편의성 설계 (HUD 배치/메뉴 뎁스/단축키/조작감)", 
                    "라이브 옵스 및 이벤트 기획 (시즌 이벤트/픽업 로테이션/출석부)",
                    "메타 게임 및 서브 콘텐츠 (도감 수집/하우징/미니게임/꾸미기)",
                    "온보딩 및 튜토리얼 (초반 동선/가이드 로직/이탈 방지 장치)"
                ];
                const randomCategory = categories[Math.floor(Math.random() * categories.length)];

                console.log(`\n[진행률: ${idx + 1}/${targetGames.length}] 매출 ${luckyRank}위: ${luckyGame.title} 처리 중...`);
                console.log(`  -> 🎯 타겟 분석 영역: [${randomCategory}] / 출시일: ${releaseDate}`);

                const prompt = `
# Input
* **타겟 게임:** [${luckyGame.developer}]의 ${luckyGame.title} (구글 매출 ${luckyRank}위)
* **분석 타겟 영역:** ${randomCategory}

# Step 0: 메타데이터 정의 (절대 수정 금지)
최상단에 반드시 다음 4줄을 작성하십시오.
메인장르: (반드시 다음 10개 중 하나만 선택: RPG, MMORPG, 방치형, SLG/전략, 캐주얼/퍼즐, 액션/슈팅, SNG/시뮬레이션, 스포츠/레이싱, 카지노/보드, 기타)
서브장르: (15자 이내 자유 형식)
시스템: (15자 이내 명사형, 파일명에 사용될 핵심 시스템명)
실제개발사: (검색으로 파악한 이 게임의 원작 개발 스튜디오명. 만약 퍼블리셔와 같다면 동일하게 기재)

# Step 1: 실제 게임 내 UI 표기 명칭 타겟팅
1. 타겟 게임에서 **[${randomCategory}]** 영역을 대표하는 시그니처 시스템 1개를 특정하십시오.
2. 유저가 게임 내에서 직접 클릭할 수 있는 **'정확한 UI 텍스트(메뉴명)'**를 기준으로 분석하십시오.

# Step 2: 실무형 분석 문서 작성 (Strict Format)
아래 9단계 구조에 맞춰 마크다운으로 작성하십시오. 04번, 07번 항목은 **표(Table)** 형식으로 정리하십시오.
01. 시스템 정의 및 ROI
02. 콘텐츠 코어 루프 (Mermaid \`graph LR\`)
03. 유저 경험 플로우차트 (Mermaid \`flowchart TD\`)
04. 수치 밸런스 설계 로직 (★ 표 형식 강제)
05. 상세 명세 및 동기 설계
06. 확장형 데이터 테이블 (Mermaid \`erDiagram\`)
07. 엣지 케이스 및 예외 처리 (★ 표 형식 강제)
08. 레퍼런스 기반 다각도 개선 제안
09. **참고 문헌 및 팩트 체크 출처** (★ 필수: 이 분석을 위해 구글 검색에서 참조한 실제 URL 웹 링크를 최소 2개 이상 리스트업 하십시오.)

# ★ [핵심] 개발사-퍼블리셔 교차 검증 및 국가별 맞춤 검색 (Dynamic Grounding)
1. **주체 식별**: 제공된 법인명([${luckyGame.developer}])은 구글플레이에 등록된 '퍼블리셔(Publisher)'입니다. 구글 검색을 통해 이 게임의 **'실제 원작 개발사(Developer)'**가 어디인지 먼저 식별하십시오.
2. **투트랙(Two-Track) 검색**: 
   - 퍼블리셔와 개발사가 다른 경우 (예: 한국 개발사 + 중국 글로벌 퍼블리셔 등), **개발사 본진의 코어 로직 커뮤니티**와 **퍼블리셔가 주도하는 라이브 운영 지표(BM/패치노트)**를 모두 검색하여 교차 검증하십시오.
   - 글로벌 게임은 "{게임명} Reddit", "{게임명} Fandom Wiki", 한국 내수 게임은 "{게임명} 공식 라운지/인벤" 등을 우선 타겟팅하십시오.
3. (중요) 해외 영문, 중문, 일문 데이터를 참고하더라도 최종 출력은 **반드시 전문적인 한국어 게임 기획 용어로 번역 및 정제하여 작성**하십시오.

# Output Constraints (절대 수정 금지)
* [사고 과정 노출 금지]: 내부 검색 과정은 텍스트로 노출하지 마십시오.
* [Mermaid 규칙]: 화살표 텍스트(\`-->|텍스트|\`)는 10자 이내. 대괄호, 중괄호 안에 콜론(:), 따옴표("), 쉼표(,) 절대 금지.
* [노드 ID 규칙]: Mermaid 다이어그램의 노드 ID는 반드시 띄어쓰기 없는 알파벳+숫자 조합(예: A1, B2)으로 작성.
`;
        
                let reportText = "";
                let draftSuccess = false;
                
                for (let initAttempt = 1; initAttempt <= MAX_RETRIES; initAttempt++) {
                    try {
                        await delay(5000); 
                        const draftResult = await draftModel.generateContent(prompt);
                        reportText = draftResult.response.text();
                        draftSuccess = true;
                        break;
                    } catch (apiError) {
                        const errMsg = apiError.message || "";
                        console.log(`  -> 🚨 에러 원인: ${errMsg.substring(0, 120).replace(/\n/g, ' ')}...`); 
                        
                        let waitTime = 15000; 
                        const match = errMsg.match(/retry in (\d+(?:\.\d+)?)s/i);
                        if (match) {
                            waitTime = (Math.ceil(parseFloat(match[1])) + 2) * 1000; 
                            console.log(`  -> ⏱️ 구글 서버 지시 수신: ${waitTime/1000}초 절대 냉각 진입 (${initAttempt}/${MAX_RETRIES})...`);
                        } else {
                            console.log(`  -> ⚠️ 기본 15초 냉각 진입 (${initAttempt}/${MAX_RETRIES})...`);
                        }
                        await delay(waitTime);
                    }
                }

                if (!draftSuccess) {
                  console.error(`  -> ❌ 3회 재시도 실패. 다음 게임으로 넘어갑니다.`);
                  continue; 
                }

                if (reportText.includes('[ABORT_NO_DATA]')) {
                    console.log(`  -> ⏭️ [AUTO-SKIP] 데이터가 부족한 양산형/비주류 게임으로 판단되어 작성을 취소합니다.`);
                    skippedCount++;
                    continue;
                }

                reportText = reportText.replace(/^```(markdown|md)?/i, '').replace(/```$/i, '').trim();

                let metaMatches = [...reportText.matchAll(/메인장르:/g)];
                if (metaMatches.length > 1) {
                    let lastMetaIndex = metaMatches[metaMatches.length - 1].index;
                    reportText = reportText.substring(lastMetaIndex);
                }

                let coreSystemName = "시스템_통합_분석"; 
                const systemMatch = reportText.match(/시스템:\s*([^\n]+)/);
                if (systemMatch) {
                    coreSystemName = systemMatch[1].replace(/\[\/META\]/gi, '').replace(/[/\\?%*:|"<>]/g, '_').trim();
                }

                let realDeveloper = luckyGame.developer; 
                const devMatch = reportText.match(/실제개발사:\s*([^\n]+)/);
                if (devMatch) {
                    realDeveloper = devMatch[1].replace(/\[\/META\]/gi, '').trim();
                }

                reportText = reportText.replace(/메인장르:.*?\n/g, '')
                                       .replace(/서브장르:.*?\n/g, '')
                                       .replace(/시스템:.*?\n/g, '')
                                       .replace(/실제개발사:.*?\n/g, '').trim();

                const cleanHeader = `
# [${luckyRank}위] ${luckyGame.title} 분석 문서
> **분석 타겟:** ${randomCategory}
> **핵심 시스템:** ${coreSystemName}
> **퍼블리셔:** ${luckyGame.developer}
> **실제 개발사:** ${realDeveloper}
> **작성일:** ${dateString}
> **출시일:** ${releaseDate}

---

`;
                reportText = cleanHeader + reportText;

                const mermaidRegex = /```mermaid\s*([\s\S]*?)```/gi;
                let mdText = "";  
                let pdfText = ""; 
                let lastIndex = 0;
                let isMermaidBroken = false; 
                
                for (const match of [...reportText.matchAll(mermaidRegex)]) {
                    const preText = reportText.substring(lastIndex, match.index);
                    mdText += preText;
                    pdfText += preText;

                    let originalMermaid = match[1];
                    let finalFixedMermaid = originalMermaid; 
                    
                    let fastTrackCode = sanitizeMermaid(originalMermaid);
                    const fastUrl = getKrokiUrl(fastTrackCode);
                    
                    try {
                        const fastRes = await fetch(fastUrl);
                        const fastSvg = await fastRes.text();
                        
                        if (fastRes.ok && !fastSvg.includes('Syntax error') && !fastSvg.includes('SyntaxError') && !fastSvg.includes('Error 400')) {
                            console.log(`  -> ⚡ [Fast-Track 성공] 정규식 완벽 교정 완료!`);
                            finalFixedMermaid = fastTrackCode; 
                        } else {
                            console.log(`  -> ⚠️ [Fast-Track 실패] 분리된 QA 에이전트 호출...`);
                            const MAX_QA_RETRIES = 5; 
                            let currentMermaid = originalMermaid;
                            let qaSuccess = false;

                            for (let attempt = 1; attempt <= MAX_QA_RETRIES; attempt++) {
                                const qaPrompt = `
${attempt > 1 ? "\n**[경고] 이전 시도에서 파서 에러가 발생했습니다! 화살표 텍스트에 긴 문장을 쓰지 마십시오. 화살표 텍스트는 10자 이내로 짧게 쓰십시오.**\n" : ""}
1. [ERD 규칙]: \`erDiagram\` 속성에 따옴표나 코멘트를 모두 지우고 '타입 이름'만 남기세요.
2. [Flowchart 규칙]: 모든 \`subgraph\` 이름은 반드시 큰따옴표(\`""\`)로 감쌀 것.
3. [노드 규칙]: 대괄호 \`[]\` 밖의 노드 ID는 반드시 **띄어쓰기 없는 알파벳과 숫자 조합(예: A1, Node2)**으로만 작성하십시오.

[원본 코드]:
${currentMermaid}
`;
                                let qaResultText = "";
                                for(let qaTry=1; qaTry<=3; qaTry++) {
                                    try {
                                        await delay(5000); 
                                        let res = await qaModel.generateContent(qaPrompt);
                                        qaResultText = res.response.text();
                                        break;
                                    } catch(qaErr) {
                                        const errMsg = qaErr.message || "";
                                        let waitTime = 15000;
                                        const match = errMsg.match(/retry in (\d+(?:\.\d+)?)s/i);
                                        if (match) {
                                            waitTime = (Math.ceil(parseFloat(match[1])) + 2) * 1000;
                                            console.log(`  -> ⏱️ [QA] 구글 서버 지시 수신: ${waitTime/1000}초 냉각...`);
                                        } else {
                                            console.log(`  -> ⚠️ [QA] 기본 15초 냉각 진입...`);
                                        }
                                        await delay(waitTime);
                                    }
                                }
                                
                                if(!qaResultText) continue;

                                try {
                                    let aiFixedCode = qaResultText.replace(/```mermaid\s*/ig, '').replace(/```/g, '').trim();
                                    let doubleCheckedCode = sanitizeMermaid(aiFixedCode); 
                                    const testUrl = getKrokiUrl(doubleCheckedCode);
                                    const testResponse = await fetch(testUrl);
                                    const testSvgText = await testResponse.text();

                                    if (testResponse.ok && !testSvgText.includes('Syntax error') && !testSvgText.includes('SyntaxError') && !testSvgText.includes('Error 400')) {
                                        console.log(`  -> [시도 ${attempt}/${MAX_QA_RETRIES}] QA 에이전트 렌더링 복구 성공!`);
                                        finalFixedMermaid = doubleCheckedCode; 
                                        qaSuccess = true;
                                        await delay(15000); 
                                        break; 
                                    } else {
                                        currentMermaid = doubleCheckedCode; 
                                    }
                                } catch(qaError) {}
                                await delay(15000); 
                            }
                            // ★ [안전 스킵]
                            if (!qaSuccess) {
                                console.log(`  -> 🚨 [최후 방어선] 외계어 감지. 해당 게임 분석을 스킵합니다.`);
                                isMermaidBroken = true;
                                break; 
                            }
                        }

                        if (!isMermaidBroken) {
                            mdText += "```mermaid\n" + finalFixedMermaid + "\n```"; 
                            const finalRenderUrl = getKrokiUrl(finalFixedMermaid);
                            pdfText += `\n\n![시스템 다이어그램](${finalRenderUrl})\n\n`; 
                        }

                    } catch (e) {
                        isMermaidBroken = true;
                        break;
                    }
                    lastIndex = match.index + match[0].length;
                }

                // ★ [완벽 스킵 로직] PDF/HTML 저장 프로세스를 타지 않고 바로 건너뜀
                if (isMermaidBroken) {
                    skippedCount++;
                    console.log(`  -> ⏭️ [AUTO-SKIP] 다이어그램 파손으로 인해 PDF/HTML 생성을 생략하고 다음 게임으로 넘어갑니다.`);
                    if (idx < targetGames.length - 1) await delay(30000); 
                    continue; 
                }

                const remainingText = reportText.substring(lastIndex);
                mdText += remainingText;
                pdfText += remainingText;

                const safeTitle = luckyGame.title.replace(/[/\\?%*:|"<>]/g, '_');
                const baseFileName = `[${dateString}]_${String(luckyRank).padStart(3, '0')}위_${safeTitle}_(${coreSystemName})`;

                let mdSaved = false;
                let pdfSaved = false;
                let htmlSaved = false;

                try {
                  if (!mdText || mdText.length < 10) throw new Error("MD 데이터가 비어있습니다.");
                  const mdStream = new stream.PassThrough();
                  mdStream.end(Buffer.from(mdText, 'utf8')); 
                  await drive.files.create({
                    requestBody: { name: `${baseFileName}.md`, parents: [mdFolderId] }, 
                    media: { mimeType: 'text/markdown', body: mdStream }
                  });
                  console.log(`  -> 💾 [MD] 저장 완료`);
                  mdSaved = true;
                } catch (e) { console.error(`  -> ❌ [MD] 저장 실패: ${e.message}`); }

                // ★ [PDF 디자인 원복] 대표님의 오리지널 레퍼런스 적용
                try {
                  console.log(`  -> 📄 [PDF] 변환 시작...`);
                  const pdfData = await mdToPdf({ content: pdfText }, {
                      timeout: 120000, 
                      launch_options: { args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] },
                      css: `
                          body { font-family: 'Noto Sans CJK KR', sans-serif; line-height: 1.7; color: #1F2937; padding: 20px; }
                          h1 { font-size: 2.2em; font-weight: 800; border-bottom: 3px solid #4F46E5; padding-bottom: 12px; margin-bottom: 25px; color: #111827; }
                          h2 { font-size: 1.5em; font-weight: 700; color: #4F46E5; margin-top: 2.2em; border-bottom: 1px solid #E5E7EB; padding-bottom: 8px; }
                          h3 { font-size: 1.25em; font-weight: 600; color: #374151; margin-top: 1.5em; }
                          blockquote { background-color: #EEF2FF; border-left: 5px solid #4F46E5; padding: 15px 20px; border-radius: 0 8px 8px 0; color: #4338CA; margin: 20px 0; font-weight: 500; font-size: 0.95em; }
                          table { width: 100%; border-collapse: collapse; margin: 25px 0; font-size: 0.95em; border-radius: 8px; overflow: hidden; }
                          th, td { border: 1px solid #E5E7EB; padding: 12px 15px; text-align: left; }
                          th { background-color: #F9FAFB; font-weight: 600; color: #111827; }
                          pre { background-color: #F3F4F6; padding: 15px; border-radius: 8px; margin: 15px 0; overflow-x: auto; }
                          code { font-family: monospace; font-size: 0.9em; color: #DB2777; background-color: #F9FAFB; padding: 2px 5px; border-radius: 4px; }
                          pre code { background-color: transparent; color: inherit; padding: 0; }
                          hr { border: 0; height: 1px; background: #E5E7EB; margin: 30px 0; }
                          img { display: block; margin: 30px auto; max-width: 80%; max-height: 400px; width: auto; height: auto; border-radius: 12px; box-shadow: 0 10px 15px -3px rgba(0,0,0,0.1); }
                          @media (max-width: 768px) { body { padding: 15px 10px; } .report-container { padding: 30px 20px; border-radius: 16px; } h1 { font-size: 1.8em; } h2 { font-size: 1.4em; } }
                      `,
                      pdf_options: { format: 'A4', margin: { top: '20mm', right: '20mm', bottom: '20mm', left: '20mm' } }
                  });
                  
                  if (!pdfData || !pdfData.content) throw new Error("PDF 변환 엔진이 빈 데이터를 반환했습니다.");
                  
                  const pdfStream = new stream.PassThrough();
                  pdfStream.end(pdfData.content);
                  await drive.files.create({
                    requestBody: { name: `${baseFileName}.pdf`, parents: [pdfFolderId] }, 
                    media: { mimeType: 'application/pdf', body: pdfStream }
                  });
                  console.log(`  -> 💾 [PDF] 저장 완료`);
                  pdfSaved = true;
                } catch (e) { console.error(`  -> ❌ [PDF] 변환/저장 실패: ${e.message}`); }

                // ★ [HTML 디자인 원복] 대표님의 오리지널 레퍼런스 적용
                try {
                  console.log(`  -> 🌐 [HTML] 변환 시작...`);
                  const parsedHtmlBody = marked.parse(pdfText); 
                  if (!parsedHtmlBody || parsedHtmlBody.trim() === "") throw new Error("HTML 파싱 결과가 비어있습니다.");

                  const fullHtml = `
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${luckyGame.title} 분석 문서</title>
    <style>
        @import url('[https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css](https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css)');
        :root { --primary: #4F46E5; --bg: #F3F4F6; --card-bg: #FFFFFF; --text-main: #1F2937; --border: #E5E7EB; }
        body { font-family: 'Pretendard', -apple-system, sans-serif; background-color: var(--bg); color: var(--text-main); line-height: 1.75; margin: 0; padding: 40px 20px; }
        .report-container { max-width: 900px; margin: 0 auto; background: var(--card-bg); padding: 50px 70px; border-radius: 24px; box-shadow: 0 20px 25px -5px rgba(0,0,0,0.1), 0 10px 10px -5px rgba(0,0,0,0.04); }
        h1 { font-size: 2.4em; font-weight: 800; color: #111827; border-bottom: 4px solid var(--primary); padding-bottom: 15px; margin-bottom: 30px; letter-spacing: -0.02em; }
        h2 { font-size: 1.6em; font-weight: 700; color: var(--primary); margin-top: 2.5em; border-bottom: 1px solid var(--border); padding-bottom: 10px; }
        h3 { font-size: 1.3em; font-weight: 600; color: #374151; margin-top: 1.8em; }
        blockquote { background: #EEF2FF; border-left: 5px solid var(--primary); padding: 20px; margin: 25px 0; border-radius: 0 12px 12px 0; color: #4338CA; font-weight: 500; font-size: 1.05em; }
        table { width: 100%; border-collapse: separate; border-spacing: 0; margin: 30px 0; border-radius: 12px; overflow: hidden; border: 1px solid var(--border); box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05); }
        th { background-color: #F9FAFB; padding: 16px; font-weight: 600; text-align: left; border-bottom: 1px solid var(--border); color: #374151; }
        td { padding: 16px; border-bottom: 1px solid var(--border); }
        tr:last-child td { border-bottom: none; }
        pre { background: #1E293B; color: #F8FAFC; padding: 20px; border-radius: 12px; overflow-x: auto; margin: 20px 0; box-shadow: inset 0 2px 4px 0 rgba(0,0,0,0.06); }
        code { font-family: monospace; font-size: 0.9em; background: #F1F5F9; color: #E11D48; padding: 4px 8px; border-radius: 6px; }
        pre code { background: transparent; color: inherit; padding: 0; }
        img { display: block; margin: 40px auto; max-width: 90%; height: auto; border-radius: 12px; box-shadow: 0 10px 15px -3px rgba(0,0,0,0.1); }
        hr { border: 0; height: 1px; background: var(--border); margin: 40px 0; }
        @media (max-width: 768px) { body { padding: 15px 10px; } .report-container { padding: 30px 20px; border-radius: 16px; } h1 { font-size: 1.8em; } h2 { font-size: 1.4em; } }
    </style>
</head>
<body>
    <div class="report-container">
        ${parsedHtmlBody}
    </div>
</body>
</html>`;
                  
                  const htmlStream = new stream.PassThrough();
                  htmlStream.end(Buffer.from(fullHtml, 'utf8'));
                  await drive.files.create({
                    requestBody: { name: `${baseFileName}.html`, parents: [htmlFolderId] }, 
                    media: { mimeType: 'text/html', body: htmlStream }
                  });
                  console.log(`  -> 💾 [HTML] 저장 완료`);
                  htmlSaved = true;
                } catch (e) { console.error(`  -> ❌ [HTML] 변환/저장 실패: ${e.message}`); }

                if (mdSaved && pdfSaved && htmlSaved) {
                    successCount++;
                } else if (mdSaved || pdfSaved || htmlSaved) {
                    console.log(`  -> ⚠️ 일부 포맷 저장 실패 (MD:${mdSaved}, PDF:${pdfSaved}, HTML:${htmlSaved})`);
                    successCount++; 
                } else {
                    console.error(`  -> ❌ 모든 포맷 저장 실패`);
                }

                if (idx < targetGames.length - 1) await delay(30000); 
            }
            
            console.log(`\n======================================================`);
            console.log(`[${dateString}] 📊 최종 결산 리포트`);
            console.log(`- 목표 처리량: ${targetGames.length}개`);
            console.log(`- 적재 성공량: ${successCount}개`);
            console.log(`- 자동 스킵량 (외계어/비주류): ${skippedCount}개`);
            console.log(`- 완전 에러 폐기량: ${targetGames.length - successCount - skippedCount}개`);
            console.log(`🎉 구글 드라이브 동기화 작업이 모두 종료되었습니다.`);
            console.log(`======================================================\n`);
        }
    } catch (error) { console.error("공정 치명적 에러:", error); process.exit(1); }
}
main();
