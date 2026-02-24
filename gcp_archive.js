const { google } = require('googleapis'); 
const { GoogleGenerativeAI } = require('@google/generative-ai');
const stream = require('stream');
const zlib = require('zlib');
const { mdToPdf } = require('md-to-pdf'); 

const oauth2Client = new google.auth.OAuth2(
  process.env.GCP_CLIENT_ID,
  process.env.GCP_CLIENT_SECRET,
  'https://developers.google.com/oauthplayground'
);
oauth2Client.setCredentials({ refresh_token: process.env.GCP_REFRESH_TOKEN });
const drive = google.drive({ version: 'v3', auth: oauth2Client });

const ROOT_FOLDER_ID = process.env.GDRIVE_FOLDER_ID;
const BATCH_SIZE = 50; 
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
    const yearStr = String(now.getFullYear());
    const monthStr = String(now.getMonth() + 1).padStart(2, '0') + "월";
    
    // ★ [폴더명 분리]
    const dayStr = String(now.getDate()).padStart(2, '0') + "일";
    const mdFolderName = `${dayStr}_md`;
    const pdfFolderName = `${dayStr}_pdf`;

    let successCount = 0;

    if (allGames.length > 0) {
      // ★ [트리 생성: 년 -> 월 -> (md폴더 / pdf폴더)]
      const yearId = await getOrCreateFolder(yearStr, ROOT_FOLDER_ID);
      const monthId = await getOrCreateFolder(monthStr, yearId);
      
      const mdFolderId = await getOrCreateFolder(mdFolderName, monthId);
      const pdfFolderId = await getOrCreateFolder(pdfFolderName, monthId);

      const targetGames = [...allGames].sort(() => 0.5 - Math.random()).slice(0, BATCH_SIZE);
      
      console.log(`\n[${dateString}] 🗄️ 멀티 API 코어 적재 엔진 가동 (가용 키: ${apiKeys.length}개)`);
      console.log(`📂 MD 저장 경로: ${yearStr}/${monthStr}/${mdFolderName}`);
      console.log(`📂 PDF 저장 경로: ${yearStr}/${monthStr}/${pdfFolderName}`);

      for (let idx = 0; idx < targetGames.length; idx++) {
        const luckyGame = targetGames[idx];
        const luckyRank = luckyGame.actualRank; 
        
        const currentKey = apiKeys[idx % apiKeys.length];
        const genAI = new GoogleGenerativeAI(currentKey);
        const model = genAI.getGenerativeModel({ model: "gemini-2.5-flash" });

        console.log(`\n[${idx + 1}/${BATCH_SIZE}] 매출 ${luckyRank}위: ${luckyGame.title} 처리 중 (API Core ${ (idx % apiKeys.length) + 1 } 사용)`);

        const prompt = `
# Role
당신은 15년 차 수석 게임 시스템 기획자이자 실무 디렉터입니다. 모든 기획 요소(캐릭터, 전투, 콘텐츠, 레벨 등)를 철저히 '입력-연산-출력'이 명확한 시스템 로직으로 해체합니다. 당신의 문서는 감성이나 추상적인 재미를 논하지 않으며, 궁극적으로 "이 시스템이 비즈니스적으로 투자 가치(ROI)가 있는가?"에 대한 데이터 기반의 답을 제공해야 합니다.

# Input
* **타겟 게임:** [${luckyGame.title}] (개발사: ${luckyGame.developer})
* **현재 순위:** 한국 구글플레이 매출 ${luckyRank}위
* **분석 대상 자동 선정 명령:** 이 게임에서 현재 매출 순위를 견인하는 가장 핵심적인 BM(Business Model) 또는 핵심 리텐션(Core Loop) 시스템 1가지를 당신이 스스로 판별하여 분석 대상으로 삼으십시오.

# Step 0: 메타데이터 및 가설 정의
본문 작성 전 최상단에 다음 형식의 JSON 블록과 가설을 반드시 선언하십시오.

\`\`\`json
{
  "analysis_date": "${dateString}",
  "store_rank": ${luckyRank},
  "game_title": "${luckyGame.title}",
  "developer": "${luckyGame.developer}",
  "core_system_analyzed": "당신이 선정한 타겟 시스템 명사형 (예: 재화 소각 및 스탯 증폭 시스템)"
}
\`\`\`
* 장르: (10자 이내)
* 분석 기준 가설: [추정 DAU / 핵심 객단가 / 목표 리텐션 중 택 1하여 수치 선언]

# Step 1: 핵심 로직 강제 치환 및 정보 교차 검증
1. 당신이 선정한 '분석 대상'이 타겟 게임의 매출과 리텐션에 어떻게 기여하는지 시스템 관점에서 1문장으로 정의하십시오.
2. 검색 시 ${dateString}을 기준으로 최신 트렌드를 교차 검증하고, 1년 이상 지난 정보는 "오래된 정보입니다"라고 명시하십시오. 불확실한 수치는 절대 지어내지 말고 "정보 부족"이라 선언하십시오.

# Step 2: 범용 입체 역기획서 작성 (6-Step Dissection)
아래 구조에 맞춰 마크다운 형식으로 작성하십시오. 감성적 서술을 엄격히 금지합니다.

## [출력 목차]
* **01. 시스템 정의 및 ROI (The 'Why')**
* **02. 자원/경험 변환 루프 (The 'Process')**
    * 유저의 입력(비용/시간/컨트롤)이 어떤 연산(확률/물리/기믹)을 거쳐 출력(보상/경험)되는지 시각화 (Mermaid flowchart TD).
* **03. 핵심 수치 및 밸런스 로직 (The 'Numeric')**
* **04. 데이터 스키마 설계 (The 'Data')**
    * 이 기획을 클라이언트-서버 간 구현하기 위한 기획자 주도형 데이터 구조 (Mermaid erDiagram). 정적/동적 데이터 분리.
* **05. 치명적 예외 및 방어 기제 (The 'Defense')**
* **06. MVP 구현 통찰 (The 'Action')**

# Output Constraints
* [사고 과정 노출 금지]: 파이썬 코드 실행 결과나 내부 검색/분석 과정은 절대로 텍스트로 노출하지 마십시오. 처음부터 끝까지 생략 없이 단 한 번만 출력하십시오.
* [페르소나 전환]: 다이어그램(Mermaid) 코드를 작성할 때만큼은 '수석 기획자'가 아니라 '감정과 의도가 거세된 엄격한 컴파일러 기계'로 빙의하십시오.
* [매우 중요] 화살표 텍스트(\`-->|텍스트|\`)는 반드시 **단답형 키워드(10자 이내)**로만 작성하십시오. 문장형 작성은 문법을 파괴하므로 절대 금지합니다.
* 다이어그램 노드 ID(대괄호 앞의 식별자)는 무조건 **알파벳 대문자(A, B, C...)**만 사용. 텍스트 내부에 큰따옴표나 작은따옴표 절대로 사용 금지. 화살표 끝에 콜론(:) 사용 금지.
* ERD 테이블 이름 대괄호/따옴표/띄어쓰기 금지. ERD 속성 작성 시 코멘트를 쓰지 말고 줄바꿈으로 구분하십시오.
`;
        
        let reportText = "";
        let draftSuccess = false;
        
        for (let initAttempt = 1; initAttempt <= MAX_RETRIES; initAttempt++) {
            try {
                const draftResult = await model.generateContent(prompt);
                reportText = draftResult.response.text();
                draftSuccess = true;
                break;
            } catch (apiError) {
                console.log(`  -> ⚠️ 구글 API 서버 503 과부하 감지. 15초 냉각 후 재시도 (${initAttempt}/${MAX_RETRIES})...`);
                await delay(15000);
            }
        }

        if (!draftSuccess) {
          console.error(`  -> ❌ ${MAX_RETRIES}회 재시도 실패: 구글 서버 완전 다운. 다음 게임으로 넘어갑니다.`);
          continue; 
        }

        let jsonMatches = [...reportText.matchAll(/```json/g)];
        if (jsonMatches.length > 1) {
            let lastMetaIndex = jsonMatches[jsonMatches.length - 1].index;
            reportText = reportText.substring(lastMetaIndex);
        }

        const mermaidRegex = /```mermaid\s*([\s\S]*?)```/gi;
        let newReportText = "";
        let lastIndex = 0;
        let isMermaidBroken = false; 
        
        for (const match of [...reportText.matchAll(mermaidRegex)]) {
            newReportText += reportText.substring(lastIndex, match.index);
            let fastTrackCode = sanitizeMermaid(match[1]);
            
            const fastUrl = getKrokiUrl(fastTrackCode);
            try {
                const fastRes = await fetch(fastUrl);
                const fastSvg = await fastRes.text();
                if (fastRes.ok && !fastSvg.includes('Syntax error') && !fastSvg.includes('SyntaxError')) {
                    console.log(`  -> ⚡ [Fast-Track 성공] 다이어그램을 이미지로 치환 완료!`);
                    newReportText += `\n\n![시스템 다이어그램](${fastUrl})\n\n`;
                } else {
                    console.log(`  -> 🚨 [품질 미달] 수술 불가능한 외계어 다이어그램 감지.`);
                    isMermaidBroken = true; 
                    break; 
                }
            } catch (e) {
                isMermaidBroken = true;
                break;
            }
            lastIndex = match.index + match[0].length;
        }

        if (isMermaidBroken) {
            console.log(`  -> ⏭️ 해당 게임의 기획서를 구글 드라이브에 저장하지 않고 건너뜁니다 (Skip).`);
            if (idx < targetGames.length - 1) await delay(30000); 
            continue; 
        }

        reportText = newReportText + reportText.substring(lastIndex);

        let coreSystemName = "시스템_통합_분석"; 
        try {
          const jsonMatch = reportText.match(/```json\n([\s\S]*?)\n```/);
          if (jsonMatch && jsonMatch[1]) {
            const parsedData = JSON.parse(jsonMatch[1]);
            if (parsedData.core_system_analyzed) {
              coreSystemName = parsedData.core_system_analyzed.replace(/[/\\?%*:|"<>]/g, '_').trim();
            }
          }
        } catch (e) {
          console.log(`  -> ⚠️ 메타데이터 파싱 에러: 기본 시스템 명칭으로 대체합니다.`);
        }

        const safeTitle = luckyGame.title.replace(/[/\\?%*:|"<>]/g, '_');
        const baseFileName = `[${dateString}]_${String(luckyRank).padStart(3, '0')}위_${safeTitle}_(${coreSystemName})`;

        try {
          // ★ [1] 마크다운(.md) 파일 저장 -> mdFolderId로 전송
          const mdStream = new stream.PassThrough();
          mdStream.end(Buffer.from(reportText, 'utf8'));
          await drive.files.create({
            requestBody: { name: `${baseFileName}.md`, parents: [mdFolderId] },
            media: { mimeType: 'text/markdown', body: mdStream }
          });
          console.log(`  -> 💾 [MD] 저장 완료: ${mdFolderName}/${baseFileName}.md`);

          // ★ [2] PDF(.pdf) 파일 변환 및 저장 -> pdfFolderId로 전송
          console.log(`  -> 📄 [PDF] 변환 시작... (약 5초 소요)`);
          const pdfData = await mdToPdf({ content: reportText }, {
              launch_options: { args: ['--no-sandbox', '--disable-setuid-sandbox'] },
              css: `
                  @import url('[https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@400;700&display=swap](https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@400;700&display=swap)');
                  body { font-family: 'Noto Sans KR', sans-serif; line-height: 1.6; color: #333; }
                  h1, h2, h3 { color: #111; margin-top: 24px; border-bottom: 1px solid #eaeaea; padding-bottom: 8px;}
                  img { max-width: 100%; height: auto; display: block; margin: 20px auto; }
                  table { border-collapse: collapse; width: 100%; margin: 20px 0; font-size: 0.9em; }
                  th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
                  th { background-color: #f4f4f4; color: #333; font-weight: bold; }
                  code { background-color: #f4f4f4; padding: 2px 4px; border-radius: 4px; font-size: 0.9em; }
              `,
              pdf_options: { format: 'A4', margin: { top: '20mm', right: '20mm', bottom: '20mm', left: '20mm' } }
          });

          const pdfStream = new stream.PassThrough();
          pdfStream.end(pdfData.content);
          await drive.files.create({
            requestBody: { name: `${baseFileName}.pdf`, parents: [pdfFolderId] },
            media: { mimeType: 'application/pdf', body: pdfStream }
          });
          console.log(`  -> 💾 [PDF] 저장 완료: ${pdfFolderName}/${baseFileName}.pdf`);

          successCount++;
          
        } catch (e) { 
            console.error(`  -> ❌ 파일 저장 중 에러 발생: ${e.message}`); 
        }

        if (idx < targetGames.length - 1) await delay(30000); 
      }
      
      console.log(`\n======================================================`);
      console.log(`[${dateString}] 📊 최종 결산 리포트`);
      console.log(`- 목표 처리량: ${targetGames.length}개`);
      console.log(`- 적재 성공량 (MD+PDF 세트): ${successCount}개`);
      console.log(`- 불량 폐기량: ${targetGames.length - successCount}개`);
      console.log(`🎉 구글 드라이브 동기화 작업이 모두 종료되었습니다.`);
      console.log(`======================================================\n`);
    }
  } catch (error) { console.error("공정 치명적 에러:", error); process.exit(1); }
}
main();
