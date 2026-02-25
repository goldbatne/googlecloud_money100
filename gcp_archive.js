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

// ★ [NEW] 랜덤 BATCH_SIZE 삭제 -> 환경변수로 시작/종료 순위 통제 (기본값 1~50)
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
  let fixed = rawCode.replace(/\/\/.*$/gm, '').replace(/%%.*$/gm, '').trim();
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

      // ★ [NEW] 랜덤 추출 폐기 -> 할당된 순위(START~END)만큼만 정확히 자르기
      const targetGames = allGames.slice(START_RANK - 1, END_RANK);
      
      console.log(`\n[${dateString}] 🗄️ 멀티 API 코어 병렬 엔진 가동 (${START_RANK}위 ~ ${END_RANK}위)`);
      for (let idx = 0; idx < targetGames.length; idx++) {
        const luckyGame = targetGames[idx];
        const luckyRank = luckyGame.actualRank; 
        
        const currentKey = apiKeys[idx % apiKeys.length];
        const genAI = new GoogleGenerativeAI(currentKey);
        const model = genAI.getGenerativeModel({ model: "gemini-2.5-flash" });

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
        console.log(`  -> 🎯 타겟 분석 영역: [${randomCategory}]`);

        const prompt = `
# Base Persona & Tone
- 당신은 15년 차 수석 게임 시스템 기획자이자 실무 디렉터입니다. 기획은 정답 맞추기가 아니라 '문장으로 회사(자본)를 설득하는 영역'임을 완벽히 이해하고 있습니다.
- 빈말이나 과한 칭찬, 단순 현상 나열을 엄격히 금지합니다. 외부 검색으로 정확한 백엔드 수치를 알 수 없는 경우 합리적으로 역산하되 반드시 **[추정]** 태그를 붙이십시오.
- **[핵심 지시사항]:** 대상 시스템을 분석하는 것에 그치지 말고, 이 시스템을 개선하기 위한 **다각도의 레퍼런스**를 반드시 찾아 제안하십시오. 가능하다면 해당 게임의 공식 설명(가이드/패치노트) 링크를 참고 자료로 제시하고, **"이 시스템을 우리 게임에 도입하거나 개선할 때, 어떤 타 게임의 훌륭한 시스템을 교차 벤치마킹하면 좋을지"** 구체적인 게임명과 시스템을 거론하며 실무적인 개선안을 도출해야 합니다.

# Input
* **타겟 게임:** [${luckyGame.developer}]의 ${luckyGame.title} (구글 매출 ${luckyRank}위)
* **분석 타겟 영역:** ${randomCategory}

# Step 0: 메타데이터 정의 (절대 수정 금지)
본문 작성 전 최상단에 반드시 다음 3줄을 작성하십시오.
메인장르: (반드시 다음 10개 중 하나만 선택: RPG, MMORPG, 방치형, SLG/전략, 캐주얼/퍼즐, 액션/슈팅, SNG/시뮬레이션, 스포츠/레이싱, 카지노/보드, 기타)
서브장르: (15자 이내 자유 형식)
시스템: (15자 이내 명사형, 파일명에 사용될 핵심 시스템명)

# Step 1: 실제 게임 내 UI 표기 명칭 타겟팅 (가장 중요)
1. 2026년 오늘 날짜를 기준으로 검색을 수행하여, 타겟 게임에서 **[${randomCategory}]** 영역을 대표하는 시그니처 시스템 1개를 특정하십시오.
2. '캐릭터 뽑기', '길드전' 같은 제너릭한 일반 명사나 마케팅용 보도자료 용어 사용을 엄격히 금지합니다.
3. 반드시 유저가 게임 접속 후 **'실제 한국 서버 클라이언트 화면(UI)에서 직접 확인하고 클릭할 수 있는 버튼, 메뉴, 탭의 정확한 텍스트'**(예: 원신 - '기원', 승리의 여신: 니케 - '싱크로 디바이스')를 최우선으로 검색하여 분석 대상으로 명시하십시오. 영문 직역은 피하십시오.

# Step 2: 실무형 분석 문서 작성 (Strict Format)
아래 8단계 구조에 맞춰 마크다운 형식으로 작성하십시오.
01. 시스템 정의 및 ROI
02. 콘텐츠 코어 루프 (Mermaid \`graph LR\`)
03. 유저 경험 플로우차트 (Mermaid \`flowchart TD\`)
04. 수치 밸런스 설계 로직
05. 상세 명세 및 동기 설계
06. 확장형 데이터 테이블 (Mermaid \`erDiagram\`)
07. 엣지 케이스 및 예외 처리
08. 레퍼런스 기반 다각도 개선 제안 (공식 설명 자료 참고 및 타 게임의 우수 시스템 교차 벤치마킹 제안 필수)

# Output Constraints (절대 수정 금지)
* [사고 과정 노출 금지]: 파이썬 코드 실행 결과나 내부 검색/분석 과정은 절대로 텍스트로 노출하지 마십시오.
* [페르소나 전환]: 다이어그램(Mermaid) 코드를 작성할 때만큼은 '수석 기획자'가 아니라 '감정과 의도가 거세된 엄격한 컴파일러 기계'로 빙의하십시오.
* [매우 중요] 화살표 텍스트(\`-->|텍스트|\`)는 반드시 **단답형 키워드(10자 이내)**로만 작성하십시오. 문장형 작성은 문법을 파괴하므로 절대 금지합니다.
* 다이어그램 노드 ID(대괄호 앞의 식별자)는 무조건 **알파벳 대문자(A, B, C...)**만 사용. 텍스트 내부에 큰따옴표나 작은따옴표 절대로 사용 금지. 화살표 끝에 콜론(:) 사용 금지.
* ERD 테이블 이름 대괄호/따옴표/띄어쓰기 금지. ERD 속성 작성 시 코멘트를 쓰지 말고 줄바꿈으로 구분하십시오.
`;
        
        let reportText = "";
        let draftSuccess = false;
        
        for (let initAttempt = 1; initAttempt <= MAX_RETRIES; initAttempt++) {
            try {
                await delay(5000); 
                const draftResult = await model.generateContent(prompt);
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

        reportText = reportText.replace(/메인장르:.*?\n/g, '')
                               .replace(/서브장르:.*?\n/g, '')
                               .replace(/시스템:.*?\n/g, '').trim();

        const cleanHeader = `
# [${luckyRank}위] ${luckyGame.title} 분석 문서
> **분석 타겟:** ${randomCategory}
> **핵심 시스템:** ${coreSystemName}
> **개발사:** ${luckyGame.developer}
> **작성일:** ${dateString}

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
                    console.log(`  -> ⚠️ [Fast-Track 실패] AI 딥러닝 교정 루프 진입...`);
                    const MAX_QA_RETRIES = 5; 
                    let currentMermaid = originalMermaid;
                    let qaSuccess = false;

                    for (let attempt = 1; attempt <= MAX_QA_RETRIES; attempt++) {
                        const qaPrompt = `
[페르소나 전환]: 당신은 감정이 없는 '엄격한 다이어그램 컴파일러'입니다. 기획적 의도나 주석은 모두 버리고 오직 완벽한 문법의 코드만 출력하십시오.
${attempt > 1 ? "\n**[경고] 이전 시도에서 파서 에러가 발생했습니다! 화살표 텍스트에 긴 문장을 쓰지 마십시오. 화살표 텍스트는 10자 이내로 짧게 쓰십시오.**\n" : ""}
1. [ERD 규칙]: \`erDiagram\` 속성에 따옴표나 코멘트를 모두 지우고 '타입 이름'만 남기세요.
2. [Flowchart 규칙]: 모든 \`subgraph\` 이름은 반드시 큰따옴표(\`""\`)로 감쌀 것.
3. [노드 규칙]: 대괄호 \`[]\` 밖의 노드 ID는 반드시 알파벳으로 명시하십시오. (예: \`Node1[텍스트]\`)
4. 마크다운(\`\`\`) 없이 순수 코드만 반환하십시오.

[원본 코드]:
${currentMermaid}
`;
                        let qaResultText = "";
                        for(let qaTry=1; qaTry<=3; qaTry++) {
                            try {
                                await delay(5000); 
                                let res = await model.generateContent(qaPrompt);
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
                                console.log(`  -> [시도 ${attempt}/${MAX_QA_RETRIES}] AI 딥러닝 렌더링 성공!`);
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
                    if (!qaSuccess) {
                        console.log(`  -> 🚨 [최후 방어선] 외계어 감지. 스킵합니다.`);
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

        if (isMermaidBroken) {
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
        @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css');
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
        code { font-family: 'JetBrains Mono', monospace; font-size: 0.9em; background: #F1F5F9; color: #E11D48; padding: 4px 8px; border-radius: 6px; }
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
      console.log(`- 적재 성공량 (MD/PDF/HTML 중 1개 이상 생존): ${successCount}개`);
      console.log(`- 완전 폐기량: ${targetGames.length - successCount}개`);
      console.log(`🎉 구글 드라이브 동기화 작업이 모두 종료되었습니다.`);
      console.log(`======================================================\n`);
    }
  } catch (error) { console.error("공정 치명적 에러:", error); process.exit(1); }
}
main();
