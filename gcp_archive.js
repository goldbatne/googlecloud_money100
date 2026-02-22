const { google } = require('googleapis');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const stream = require('stream');

// ★ [수정] 대표님의 모델 리스트 중 가장 적합한 2.0-flash를 사용합니다.
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

const auth = new google.auth.JWT(
  process.env.GCP_CLIENT_EMAIL,
  null,
  (process.env.GCP_PRIVATE_KEY || '').replace(/\\n/g, '\n'),
  ['https://www.googleapis.com/auth/drive.file']
);
const drive = google.drive({ version: 'v3', auth });

const FOLDER_ID = process.env.GDRIVE_FOLDER_ID; // 반드시 '공유 드라이브' 내 폴더 ID여야 함
const BATCH_SIZE = 10; 

const delay = ms => new Promise(res => setTimeout(res, ms));

// 크로키 서버를 이용한 문법 검증기
async function isMermaidValid(code) {
  try {
    const zlib = require('zlib');
    const data = Buffer.from(code, 'utf8');
    const compressed = zlib.deflateSync(data);
    const base64 = compressed.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
    const testUrl = `https://kroki.io/mermaid/svg/${base64}`;
    const response = await fetch(testUrl);
    const svgText = await response.text();
    return response.ok && !svgText.includes('Syntax error') && !svgText.includes('SyntaxError');
  } catch(e) { return false; }
}

async function main() {
  try {
    const gplayModule = await import('google-play-scraper');
    const gplay = gplayModule.default || gplayModule;
    const games = await gplay.list({ collection: gplay.collection.GROSSING, category: gplay.category.GAME, num: 100, country: 'kr', lang: 'ko' });
    const today = new Date();
    today.setHours(today.getHours() + 9);
    const dateString = today.toISOString().split('T')[0];

    if (games.length > 0) {
      const targetGames = games.sort(() => 0.5 - Math.random()).slice(0, BATCH_SIZE);
      console.log(`\n[${dateString}] 🗄️ ${BATCH_SIZE}개 개별 파일 적재 및 자가 치유 루프 가동`);

      for (let idx = 0; idx < targetGames.length; idx++) {
        const luckyGame = targetGames[idx];
        const luckyRank = games.findIndex(g => g.appId === luckyGame.appId) + 1;
        
        console.log(`\n[${idx + 1}/${BATCH_SIZE}] 매출 ${luckyRank}위: ${luckyGame.title} 처리 중...`);

        const model = genAI.getGenerativeModel({ model: "gemini-2.0-flash" }); // 2.0 모델 적용
        
        let reportText = "";
        let attempt = 1;
        const MAX_RETRIES = 5;

        // ★ [5중 자가 치유 루프] 문법이 맞을 때까지 5번 시도합니다.
        while (attempt <= MAX_RETRIES) {
          const prompt = `당신은 수석 게임 기획자입니다. [${luckyGame.title}]의 핵심 시스템 1개를 역기획서로 작성하십시오. 
          01.시스템정의 02.코어루프(Mermaid graph LR) 03.레벨디자인(Mermaid flowchart TD) 04.데이터스키마(Mermaid erDiagram) 05.BM구조.
          ${attempt > 1 ? "주의: 이전 시도에서 Mermaid 문법 에러가 났습니다. 따옴표와 괄호를 엄격히 체크하십시오." : ""}`;
          
          const result = await model.generateContent(prompt);
          reportText = result.response.text();

          const mermaidMatches = reportText.match(/```mermaid\n([\s\S]*?)\n```/g) || [];
          let allValid = true;
          for (const m of mermaidMatches) {
            const code = m.replace(/```mermaid\n/, '').replace(/\n```/, '');
            if (!(await isMermaidValid(code))) { allValid = false; break; }
          }

          if (allValid) {
            console.log(`  -> [시도 ${attempt}/5] 문법 검증 통과.`);
            break;
          } else {
            console.log(`  -> [시도 ${attempt}/5] 문법 에러 감지. 다시 요청합니다...`);
            attempt++;
            await delay(10000);
          }
        }

        const fileName = `[${dateString}]_${String(luckyRank).padStart(3, '0')}위_${luckyGame.title.replace(/[/\\?%*:|"<>]/g, '_')}.md`;
        const bufferStream = new stream.PassThrough();
        bufferStream.end(Buffer.from(reportText, 'utf8'));

        try {
          await drive.files.create({
            requestBody: { name: fileName, parents: [FOLDER_ID] },
            media: { mimeType: 'text/markdown', body: bufferStream },
            fields: 'id',
            supportsAllDrives: true // 공유 드라이브 지원 옵션
          });
          console.log(`  -> 💾 업로드 완료: ${fileName}`);
        } catch (e) { console.error(`  -> ❌ 업로드 실패: ${e.message}`); }

        if (idx < targetGames.length - 1) await delay(30000); 
      }
    }
  } catch (error) { console.error("실행 중 에러 발생:", error); process.exit(1); }
}
main();
