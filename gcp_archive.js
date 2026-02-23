const { google } = require('googleapis'); 
const { GoogleGenerativeAI } = require('@google/generative-ai');
const stream = require('stream');
const zlib = require('zlib');

const oauth2Client = new google.auth.OAuth2(
  process.env.GCP_CLIENT_ID,
  process.env.GCP_CLIENT_SECRET,
  'https://developers.google.com/oauthplayground'
);
oauth2Client.setCredentials({ refresh_token: process.env.GCP_REFRESH_TOKEN });
const drive = google.drive({ version: 'v3', auth: oauth2Client });

const ROOT_FOLDER_ID = process.env.GDRIVE_FOLDER_ID;
const BATCH_SIZE = 30; // 30개 가동
const MAX_RETRIES = 5;

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

async function isMermaidValid(code) {
  try {
    const data = Buffer.from(code, 'utf8');
    const compressed = zlib.deflateSync(data);
    const base64 = compressed.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
    const response = await fetch(`https://kroki.io/mermaid/svg/${base64}`);
    return response.ok && !(await response.text()).includes('Syntax error');
  } catch(e) { return false; }
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

    if (allGames.length > 0) {
      const yearId = await getOrCreateFolder(yearStr, ROOT_FOLDER_ID);
      const targetFolderId = await getOrCreateFolder(monthStr, yearId);

      const targetGames = [...allGames].sort(() => 0.5 - Math.random()).slice(0, BATCH_SIZE);
      
      console.log(`\n[${dateString}] 🗄️ 멀티 API 코어 적재 엔진 가동 (가용 키: ${apiKeys.length}개)`);

      for (let idx = 0; idx < targetGames.length; idx++) {
        const luckyGame = targetGames[idx];
        const luckyRank = luckyGame.actualRank; 
        
        const currentKey = apiKeys[idx % apiKeys.length];
        const genAI = new GoogleGenerativeAI(currentKey);
        const model = genAI.getGenerativeModel({ model: "gemini-2.5-flash" });

        console.log(`\n[${idx + 1}/${BATCH_SIZE}] 매출 ${luckyRank}위: ${luckyGame.title} 처리 중 (API Core ${ (idx % apiKeys.length) + 1 } 사용)`);

        let reportText = "";
        let attempt = 1;

        while (attempt <= MAX_RETRIES) {
          // ★ [재설계] 대표님이 고안하신 '15년 차 수석 기획자' 프롬프트 장착 완료
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
    * 해당 분야가 비즈니스적으로 존재하는 이유와 예상 KPI(ARPU, 이탈 방어율 등) 기여도.
* **02. 자원/경험 변환 루프 (The 'Process')**
    * 유저의 입력(비용/시간/컨트롤)이 어떤 연산(확률/물리/기믹)을 거쳐 출력(보상/경험)되는지 시각화 (Mermaid flowchart TD).
* **03. 핵심 수치 및 밸런스 로직 (The 'Numeric')**
    * 해당 분야를 지탱하는 핵심 변수 3가지와 상관관계 수식(추정). Step 0의 가설에 기반할 것. 표(Table) 활용.
* **04. 데이터 스키마 설계 (The 'Data')**
    * 이 기획을 클라이언트-서버 간 구현하기 위한 기획자 주도형 데이터 구조 (Mermaid erDiagram). 정적/동적 데이터 분리.
* **05. 치명적 예외 및 방어 기제 (The 'Defense')**
    * 어뷰징, 밸런스 붕괴 등 발생 가능한 치명적 예외 상황 3가지와 시스템적 방어책. 리스트 활용.
* **06. MVP 구현 통찰 (The 'Action')**
    * 이 구성을 3인 팀(기획/클라/서버) 규모에서 1개월 내 MVP로 구현하기 위해 과감히 쳐내야 할 핵심 기능 1가지.

# Output Constraints
* 빈말, 과한 칭찬, "타격감이 좋다" 같은 주관적 표현을 절대 금지합니다.
* 내부 점검: 출력 전 논리적 결함이나 지시 누락이 없는지 스스로 점검하십시오.
* Mermaid 노드명과 속성값에 괄호, 따옴표 등 특수문자 사용을 엄격히 금지. ERD에 데이터 타입(INT, VARCHAR 등) 명시.
${attempt > 1 ? "주의: 이전 시도에서 Mermaid 문법 에러가 났습니다. Syntax를 엄격히 교정하십시오." : ""}
`;
          
          try {
            const result = await model.generateContent(prompt);
            reportText = result.response.text();

            const mermaidMatches = reportText.match(/```mermaid\n([\s\S]*?)\n```/g) || [];
            let allValid = true;
            for (const m of mermaidMatches) {
              const code = m.replace(/```mermaid\n/, '').replace(/\n```/, '');
              if (!(await isMermaidValid(code))) { allValid = false; break; }
            }
            
            if (allValid) {
              break; 
            } else { 
              console.log(`  -> [시도 ${attempt}/5] ⚠️ Mermaid 문법 에러. 재교정 요청 중...`);
              attempt++; 
              await delay(10000); 
            }
          } catch (apiError) {
            const shortErrorMsg = apiError.message ? apiError.message.split('\n')[0] : "Unknown Error";
            console.log(`  -> [시도 ${attempt}/5] 🚨 구글 API 통신 에러 발생 (${shortErrorMsg}). 15초 냉각 후 재시도...`);
            attempt++;
            await delay(15000);
          }
        }

        if (!reportText) {
          console.error(`  -> ❌ 5회 재시도 실패: 구글 서버 응답 없음. 다음 게임으로 넘어갑니다.`);
          continue; 
        }

        const fileName = `[${dateString}]_${String(luckyRank).padStart(3, '0')}위_${luckyGame.title.replace(/[/\\?%*:|"<>]/g, '_')}.md`;
        const bufferStream = new stream.PassThrough();
        bufferStream.end(Buffer.from(reportText, 'utf8'));

        try {
          await drive.files.create({
            requestBody: { name: fileName, parents: [targetFolderId] },
            media: { mimeType: 'text/markdown', body: bufferStream }
          });
          console.log(`  -> 💾 적재 완료: ${yearStr}/${monthStr}/${fileName}`);
        } catch (e) { console.error(`  -> ❌ 드라이브 업로드 실패: ${e.message}`); }

        if (idx < targetGames.length - 1) await delay(20000); 
      }
    }
  } catch (error) { console.error("공정 치명적 에러:", error); process.exit(1); }
}
main();
