const { google } = require('googleapis');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const stream = require('stream');

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

// ★ [재설계] 유령 직원 대신 대표님 본인 인증(OAuth2)을 사용합니다.
const oauth2Client = new google.auth.OAuth2(
  process.env.GCP_CLIENT_ID,
  process.env.GCP_CLIENT_SECRET,
  'https://developers.google.com/oauthplayground'
);
oauth2Client.setCredentials({ refresh_token: process.env.GCP_REFRESH_TOKEN });
const drive = google.drive({ version: 'v3', auth: oauth2Client });

const FOLDER_ID = process.env.GDRIVE_FOLDER_ID;
const BATCH_SIZE = 10;
const MAX_RETRIES = 5;

const delay = ms => new Promise(res => setTimeout(res, ms));

async function isMermaidValid(code) {
  try {
    const zlib = require('zlib');
    const data = Buffer.from(code, 'utf8');
    const compressed = zlib.deflateSync(data);
    const base64 = compressed.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
    const response = await fetch(`https://kroki.io/mermaid/svg/${base64}`);
    const svgText = await response.text();
    return response.ok && !svgText.includes('Syntax error');
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
      console.log(`\n[${dateString}] 🗄️ 10개 파일 적재 및 5중 자가 치유 엔진 가동`);

      for (let idx = 0; idx < targetGames.length; idx++) {
        const luckyGame = targetGames[idx];
        const luckyRank = games.findIndex(g => g.appId === luckyGame.appId) + 1;
        console.log(`\n[${idx + 1}/${BATCH_SIZE}] ${luckyRank}위: ${luckyGame.title} 분석 중...`);

        const model = genAI.getGenerativeModel({ model: "gemini-2.5-flash" });
        let reportText = "";
        let attempt = 1;

        while (attempt <= MAX_RETRIES) {
          const prompt = `당신은 수석 기획자입니다. [${luckyGame.title}]의 핵심 시스템 역기획서를 01.정의 02.코어루프(Mermaid graph LR) 03.레벨디자인(Mermaid flowchart TD) 04.ERD(Mermaid erDiagram) 순으로 작성하십시오. ${attempt > 1 ? "주의: 이전 시도에서 문법 에러가 났으니 괄호와 따옴표를 다시 확인하십시오." : ""}`;
          const result = await model.generateContent(prompt);
          reportText = result.response.text();

          const mermaidMatches = reportText.match(/```mermaid\n([\s\S]*?)\n```/g) || [];
          let allValid = true;
          for (const m of mermaidMatches) {
            const code = m.replace(/```mermaid\n/, '').replace(/\n```/, '');
            if (!(await isMermaidValid(code))) { allValid = false; break; }
          }
          if (allValid) { console.log(`  -> [시도 ${attempt}/5] 문법 통과.`); break; }
          else { console.log(`  -> [시도 ${attempt}/5] 에러 감지. 다시 요청...`); attempt++; await delay(10000); }
        }

        const fileName = `[${dateString}]_${String(luckyRank).padStart(3, '0')}위_${luckyGame.title.replace(/[/\\?%*:|"<>]/g, '_')}.md`;
        const bufferStream = new stream.PassThrough();
        bufferStream.end(Buffer.from(reportText, 'utf8'));

        try {
          await drive.files.create({
            requestBody: { name: fileName, parents: [FOLDER_ID] },
            media: { mimeType: 'text/markdown', body: bufferStream }
          });
          console.log(`  -> 💾 업로드 완료: ${fileName}`);
        } catch (e) { console.error(`  -> ❌ 실패: ${e.message}`); }

        if (idx < targetGames.length - 1) await delay(45000); 
      }
    }
  } catch (error) { console.error("에러:", error); process.exit(1); }
}
main();
