'use strict';

// =============================================================================
//
//  📦 Google Play 게임 역기획서 자동 생성 파이프라인
//
//  흐름 요약:
//    1. Google Play 매출 순위 스크래핑
//    2. Scout  — 공식 가이드 기준 시스템명·재화명 수집 (최대 3회)
//    3. Writer — Gemini API 역기획서 초안 생성 (딥서치)
//    4. Mermaid 다이어그램 검증 및 자동 복구 (Fast-Track → QA Agent)
//    5. MD / PDF / HTML 3포맷 변환 후 Google Drive 날짜별 폴더에 저장
//
//  환경 변수 (필수):
//    GCP_CLIENT_ID       - Google OAuth2 클라이언트 ID
//    GCP_CLIENT_SECRET   - Google OAuth2 클라이언트 시크릿
//    GCP_REFRESH_TOKEN   - Google OAuth2 리프레시 토큰
//    GDRIVE_FOLDER_ID    - 저장 대상 루트 폴더 ID
//    GEMINI_API_KEY      - Gemini API 키 (쉼표로 복수 등록 가능 → Round-Robin 순환)
//
//  환경 변수 (선택):
//    START_RANK          - 처리 시작 순위 (기본값: 1)
//    END_RANK            - 처리 종료 순위 (기본값: 50)
//
// =============================================================================

const { google }             = require('googleapis');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const stream                 = require('stream');
const zlib                   = require('zlib');
const { mdToPdf }            = require('md-to-pdf');
const { marked }             = require('marked');


// =============================================================================
//  ⚙️  설정값
// =============================================================================

const ROOT_FOLDER_ID = process.env.GDRIVE_FOLDER_ID;
const START_RANK     = parseInt(process.env.START_RANK || '1',  10);
const END_RANK       = parseInt(process.env.END_RANK   || '50', 10);

const MAX_DRAFT_RETRIES = 3; // Writer API 오류(rate limit) 시 재시도
const MAX_QA_RETRIES    = 5; // Mermaid 다이어그램 QA Agent 재시도

const ANALYSIS_CATEGORIES = [
    '핵심 BM (가챠/강화/패스 등 직접적 매출원)',
    '장기 리텐션 (일일 숙제/업적/마일리지 등 접속 유지 장치)',
    '소셜 및 경쟁 (길드/PvP/랭킹 등 유저 간 상호작용)',
    '성장 및 경제 (재화 획득/소모처 및 인플레이션 제어 로직)',
    '코어 게임플레이 (전투 공식/스테이지 기믹/퍼즐 등 조작의 재미)',
    '캐릭터 및 전투 클래스 (스킬 메커니즘/시너지/상성 구조)',
    '수치 및 전투 밸런스 (데미지 공식/스테이터스/성장 체감)',
    '레벨 디자인 (맵 구조/동선/오브젝트 배치/몬스터 스폰)',
    '세계관 및 시나리오 (퀘스트 라인/내러티브/NPC 상호작용)',
    '핵심 콘텐츠 시스템 (레이드/던전/생활형 콘텐츠 등 주요 시스템)',
    'UI/UX 및 편의성 설계 (HUD 배치/메뉴 뎁스/단축키/조작감)',
    '라이브 옵스 및 이벤트 기획 (시즌 이벤트/픽업 로테이션/출석부)',
    '메타 게임 및 서브 콘텐츠 (도감 수집/하우징/미니게임/꾸미기)',
    '온보딩 및 튜토리얼 (초반 동선/가이드 로직/이탈 방지 장치)',
];

// PDF 변환 옵션 (md-to-pdf / Puppeteer 기반)
// --max-old-space-size: 50회 루프 시 Puppeteer OOM 방지를 위해 JS 힙 상한 제한
const PDF_OPTIONS = {
    timeout: 120000,
    launch_options: {
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--js-flags=--max-old-space-size=512',
        ],
    },
    css: `
        body        { font-family: 'Noto Sans CJK KR', sans-serif; line-height: 1.6; color: #1F2937; padding: 0; margin: 0; }
        h1          { font-size: 2.0em; font-weight: 800; border-bottom: 2px solid #4F46E5; padding-bottom: 10px; margin-bottom: 20px; color: #111827; page-break-after: avoid; }
        h2          { font-size: 1.4em; font-weight: 700; color: #4F46E5; margin-top: 1.5em; border-bottom: 1px solid #E5E7EB; padding-bottom: 6px; page-break-after: avoid; }
        h3          { font-size: 1.2em; font-weight: 600; color: #374151; margin-top: 1.2em; page-break-after: avoid; }
        blockquote  { background-color: #EEF2FF; border-left: 5px solid #4F46E5; padding: 12px 15px; color: #4338CA; margin: 15px 0; font-weight: 500; font-size: 0.95em; page-break-inside: avoid; }
        table       { width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 0.85em; table-layout: auto; }
        th, td      { border: 1px solid #E5E7EB; padding: 8px 10px; text-align: left; word-break: keep-all; overflow-wrap: anywhere; }
        tr          { page-break-inside: avoid; }
        pre         { background-color: #F3F4F6; padding: 15px; border-radius: 8px; margin: 15px 0; white-space: pre-wrap; word-break: break-all; page-break-inside: avoid; }
        code        { font-family: monospace; font-size: 0.9em; color: #DB2777; background-color: #F9FAFB; padding: 2px 4px; border-radius: 4px; word-break: break-all; }
        pre code    { background-color: transparent; color: inherit; padding: 0; word-break: break-all; }
        div         { page-break-inside: avoid; break-inside: avoid; }
        img         { display: block; margin: 20px auto; max-width: 100% !important; max-height: 220mm !important; height: auto !important; object-fit: contain; page-break-inside: avoid; }
    `,
    pdf_options: {
        format: 'A4',
        margin: { top: '20mm', right: '20mm', bottom: '20mm', left: '20mm' },
        printBackground: true,
    },
};


// =============================================================================
//  🔐  Google API 클라이언트 초기화
// =============================================================================

const oauth2Client = new google.auth.OAuth2(
    process.env.GCP_CLIENT_ID,
    process.env.GCP_CLIENT_SECRET,
    'https://developers.google.com/oauthplayground'
);
oauth2Client.setCredentials({ refresh_token: process.env.GCP_REFRESH_TOKEN });

const drive = google.drive({ version: 'v3', auth: oauth2Client });


// =============================================================================
//  🔑  API 키 Round-Robin Queue
// =============================================================================

class ApiKeyQueue {
    constructor(keys) {
        if (!keys || keys.length === 0) {
            console.error('❌ GEMINI_API_KEY가 등록되지 않았습니다.');
            process.exit(1);
        }
        this._keys = [...keys];
    }

    next() {
        const key = this._keys.shift();
        this._keys.push(key);
        return key;
    }
}

const apiKeyQueue = new ApiKeyQueue(
    (process.env.GEMINI_API_KEY || '').split(',').map(k => k.trim()).filter(Boolean)
);


// =============================================================================
//  🛠️  유틸리티
// =============================================================================

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

/** KST 기준 날짜 분해 */
function getKSTDateParts() {
    const now   = new Date();
    const fmt   = opts => new Intl.DateTimeFormat('ko-KR', { timeZone: 'Asia/Seoul', ...opts }).format(now);
    const year  = fmt({ year:  'numeric' }).replace(/\D/g, '');
    const month = fmt({ month: '2-digit' }).replace(/\D/g, '').padStart(2, '0');
    const day   = fmt({ day:   '2-digit' }).replace(/\D/g, '').padStart(2, '0');
    return {
        dateString: `${year}-${month}-${day}`,
        yearStr:    `${year}년`,
        monthStr:   `${month}월`,
        dayStr:     `${day}일`,
    };
}

/**
 * Gemini API 호출 공통 래퍼 — rate-limit 에러 시 동적 대기 후 재시도
 * @param {GenerativeModel} model
 * @param {string}          prompt
 * @param {number}          maxRetries
 * @returns {Promise<string>} 응답 텍스트. maxRetries 초과 시 빈 문자열 반환.
 */
async function callGeminiWithRetry(model, prompt, maxRetries = 3) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            await delay(5000);
            const result = await model.generateContent(prompt);
            return result.response.text();
        } catch (err) {
            const msg      = err.message || '';
            const matched  = msg.match(/retry in (\d+(?:\.\d+)?)s/i);
            const waitTime = matched ? (Math.ceil(parseFloat(matched[1])) + 2) * 1000 : 15000;
            console.log(`  -> ⏱️  ${waitTime / 1000}초 냉각 후 재시도 (${attempt}/${maxRetries})... [${msg.substring(0, 80)}]`);
            await delay(waitTime);
        }
    }
    return '';
}

/** Mermaid 코드 → Kroki.io SVG URL (deflate + URL-safe Base64) */
function buildKrokiUrl(mermaidCode) {
    const compressed = zlib.deflateSync(Buffer.from(mermaidCode, 'utf8'));
    const encoded    = compressed
        .toString('base64')
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=+$/g, '');
    return `https://kroki.io/mermaid/svg/${encoded}`;
}

/**
 * Kroki 응답 정상 SVG 여부 검증
 * Kroki는 파싱 오류 시에도 HTTP 200을 반환하며 SVG 내에 에러 문자열을 포함함
 */
function isValidKrokiSvg(response, svgText) {
    return (
        response.ok &&
        !svgText.includes('Syntax error') &&
        !svgText.includes('SyntaxError') &&
        !svgText.includes('Error 400')
    );
}


// =============================================================================
//  📁  Google Drive 유틸리티
// =============================================================================

/** 폴더 존재하면 ID 반환, 없으면 생성 후 반환. 실패 시 throw. */
async function getOrCreateFolder(folderName, parentId) {
    const res = await drive.files.list({
        q:      `name = '${folderName}' and '${parentId}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
        fields: 'files(id)',
    });
    if (res.data.files.length > 0) return res.data.files[0].id;

    const folder = await drive.files.create({
        resource: { name: folderName, mimeType: 'application/vnd.google-apps.folder', parents: [parentId] },
        fields:   'id',
    });
    return folder.data.id;
}

/**
 * 날짜 기반 Drive 폴더 구조 생성
 * 구조: 루트 / 연도 / { 연도_md, 연도_pdf, 연도_html } / 월_포맷 / 일_포맷
 * @returns {Promise<{ mdFolderId, pdfFolderId, htmlFolderId }>}
 */
async function createDriveFolders({ yearStr, monthStr, dayStr }) {
    const yearId      = await getOrCreateFolder(yearStr, ROOT_FOLDER_ID);
    const [mdYearId, pdfYearId, htmlYearId] = await Promise.all([
        getOrCreateFolder(`${yearStr}_md`,   yearId),
        getOrCreateFolder(`${yearStr}_pdf`,  yearId),
        getOrCreateFolder(`${yearStr}_html`, yearId),
    ]);
    const [mdMonthId, pdfMonthId, htmlMonthId] = await Promise.all([
        getOrCreateFolder(`${monthStr}_md`,   mdYearId),
        getOrCreateFolder(`${monthStr}_pdf`,  pdfYearId),
        getOrCreateFolder(`${monthStr}_html`, htmlYearId),
    ]);
    const [mdFolderId, pdfFolderId, htmlFolderId] = await Promise.all([
        getOrCreateFolder(`${dayStr}_md`,   mdMonthId),
        getOrCreateFolder(`${dayStr}_pdf`,  pdfMonthId),
        getOrCreateFolder(`${dayStr}_html`, htmlMonthId),
    ]);
    return { mdFolderId, pdfFolderId, htmlFolderId };
}

/** 동일 파일명 존재 확인 (멱등성 보장). 확인 실패 시 false 반환해 덮어쓰기 허용. */
async function fileExistsInDrive(fileName, folderId) {
    try {
        const res = await drive.files.list({
            q:      `name = '${fileName}' and '${folderId}' in parents and trashed = false`,
            fields: 'files(id)',
        });
        return res.data.files.length > 0;
    } catch {
        return false;
    }
}

/** Drive 파일 업로드. 중복이면 스킵 후 false 반환. */
async function uploadToDrive({ fileName, folderId, mimeType, content }) {
    if (await fileExistsInDrive(fileName, folderId)) {
        console.log(`  -> ⏭️  [SKIP] 이미 존재: ${fileName}`);
        return false;
    }
    const body = new stream.PassThrough();
    body.end(Buffer.isBuffer(content) ? content : Buffer.from(content, 'utf8'));
    await drive.files.create({
        requestBody: { name: fileName, parents: [folderId] },
        media:       { mimeType, body },
    });
    return true;
}


// =============================================================================
//  🤖  Gemini 모델 초기화
// =============================================================================

/**
 * 게임별 Gemini 모델 3종 초기화
 * - scoutModel:  명칭 수집 전담 (Google Search 활성화, 짧은 목록 출력만)
 * - draftModel:  역기획서 작성 (Google Search 활성화, 딥서치)
 * - qaModel:     Mermaid 복구 전담 (Search 불필요, 순수 코드 출력만)
 */
function initModels(genAI, gameTitle, appId) {
    const scoutModel = genAI.getGenerativeModel({
        model: 'gemini-2.5-flash',
        tools: [{ googleSearch: {} }],
        systemInstruction:
            `당신은 게임 데이터 수집 전문 크롤러입니다. ` +
            `분석·설명·추측은 절대 금지. ` +
            `오직 실제 게임 UI에 표시되는 명칭만 수집해 지정된 형식으로 출력하십시오. ` +
            `검색 대상은 반드시 "${gameTitle}" (앱ID: ${appId}) 단 하나입니다.`,
    });

    const draftModel = genAI.getGenerativeModel({
        model: 'gemini-2.5-flash',
        tools: [{ googleSearch: {} }],
        systemInstruction:
            '당신은 인간의 심리를 꿰뚫어 보는 15년 차 수석 게임 시스템 기획자이자 디렉터입니다. ' +
            `이번 세션의 분석 대상은 오직 "${gameTitle}" (앱ID: ${appId}) 단 하나입니다. ` +
            '같은 IP를 공유하더라도 이름이 다른 게임(예: "메이플스토리M"과 "메이플 키우기"는 별개)의 데이터를 절대 혼용하지 마십시오. ' +
            '검색 결과가 타겟 게임과 다른 게임이면 즉시 검색어를 바꾸십시오. ' +
            'UX와 BM을 바탕으로 한 합리적 역기획(Educated Guess)은 허용하나, 시스템 뼈대나 핵심 명칭을 지어내는 것은 금지합니다. ' +
            '1차 검색에서 정보가 부족하면 검색 키워드를 바꿔 심층 사이트를 끝까지 추적하는 딥 서치(Deep Search)를 수행하십시오. ' +
            '시스템의 흔적조차 없으면 [ABORT_NO_DATA], 타겟 외 게임 데이터가 섞였다고 판단되면 [IP_CONFUSED]를 출력하십시오.',
    });

    const qaModel = genAI.getGenerativeModel({
        model: 'gemini-2.5-flash',
        systemInstruction:
            '당신은 감정이 없는 엄격한 다이어그램 컴파일러입니다. ' +
            '기획적 의도, 설명, 마크다운(```) 기호 없이 오직 완벽하게 동작하는 Mermaid 순수 코드만 반환하십시오.',
    });

    return { scoutModel, draftModel, qaModel };
}


// =============================================================================
//  🧹  Mermaid 코드 정제 (sanitizeMermaid)
//
//  Gemini가 생성하는 Mermaid 코드에는 각종 파싱 오류가 빈번하게 발생함.
//  Kroki에 보내기 전 정규식으로 선제적으로 정리하는 Fast-Track 처리기.
//
//  처리 순서:
//    1. 공통 전처리 (유니코드 제로폭 문자, 주석, 특수문자 제거)
//    2. erDiagram 전용 처리 (속성 정리, 관계 표현식 정규화)
//    3. flowchart/graph 전용 처리 (괄호 변환, 엣지/노드 텍스트 토큰화, ID 자동 부여)
// =============================================================================

function sanitizeMermaid(rawCode) {

    // ── 공통 전처리 ──────────────────────────────────────────────────────────
    let code = rawCode
        .replace(/[\u200B-\u200D\uFEFF]/g, '') // 유니코드 제로폭 문자 제거
        .replace(/\/\/.*$/gm, '')              // // 주석 제거
        .replace(/%%.*$/gm, '')                // %% Mermaid 주석 제거
        .trim();

    code = code
        .replace(/["'*#]/g, '')                // 파싱 오류 유발 특수문자 제거
        .replace(/^\s*(\d+\.|[-*])\s+/gm, ''); // 목록 기호(1. / - / *) 제거

    // ── erDiagram 전용 처리 ─────────────────────────────────────────────────
    if (code.match(/^erDiagram/i)) {
        // 엔티티 블록 내 속성을 '타입 이름' 두 단어만 남기고 나머지 제거
        const lines  = code.split('\n');
        let inEntity = false;
        for (let i = 0; i < lines.length; i++) {
            const l = lines[i].trim();
            if      (l.includes('{'))        inEntity = true;
            else if (l.includes('}'))        inEntity = false;
            else if (inEntity && l.length > 0) {
                const words = l.split(/\s+/).filter(Boolean);
                lines[i]    = words.length >= 2 ? `    ${words[0]} ${words[1]}` : '';
            }
        }
        code = lines.join('\n');

        code = code
            .replace(/erDiagram\s+(.*)/i, 'erDiagram\n$1')
            .replace(/\(.*?\)/g,  '')
            .replace(/,/g,        '\n')
            .replace(/\bENUM\b/gi, '')
            .replace(/\b(PK|FK|UK|Optional)\b/gi, '')
            .replace(/^[a-zA-Z가-힣0-9_]+\s*:\s*(?=[a-zA-Z0-9_]+\s*\|\|--)/gm, '')
            .replace(/(\|\|--o{|}\|--\|{|}\|--o{|\|\|--\|{|}-o|}-\||-o|-\|)\s*([a-zA-Z0-9_]+)\s*:\s*(.*?)$/gm, '$1 $2 : "has"');

        return code;
    }

    // ── flowchart / graph 전용 처리 ─────────────────────────────────────────

    // 비표준 괄호 조합 → 표준 괄호
    code = code
        .replace(/\(\[/g, '[').replace(/\]\)/g, ']')
        .replace(/\[\[/g, '[').replace(/\]\]/g, ']')
        .replace(/\(\(/g, '(').replace(/\)\)/g, ')')
        .replace(/--\[/g, '-->[')
        .replace(/-\[/g,  '->[');

    const lines         = code.split('\n');
    const processedLines = [];
    let   autoIdCount   = 0;
    const nodeTexts     = []; // @@N{n}@@ 임시 토큰 → 노드 라벨
    const edgeTexts     = []; // @@E{n}@@ 임시 토큰 → 엣지 라벨

    for (let line of lines) {
        line = line.trim();
        if (!line) continue;

        // 선언 줄 및 end 키워드는 그대로 통과
        if (line.match(/^(graph|flowchart)\s+[a-zA-Z]+/i) || line.toLowerCase() === 'end') {
            processedLines.push(line);
            continue;
        }

        // subgraph 이름에 따옴표 강제 (없으면 한글 파싱 오류)
        if (line.match(/^subgraph\s+(.*)/i)) {
            const name = line.replace(/^subgraph\s+/i, '').replace(/["']/g, '');
            processedLines.push(`subgraph "${name}"`);
            continue;
        }

        // 콜론 레이블(A --> B : 설명) → 파이프 레이블(A -->|설명| B)
        if (line.match(/(-->|-\.->|==>|---)\s*([^:]+?)\s*:\s*(.+)$/)) {
            line = line.replace(/(-->|-\.->|==>|---)\s*([^:]+?)\s*:\s*(.+)$/, '$1|$3| $2');
        }

        // 엣지 라벨 토큰화 (공백 제거 단계에서 내용 손상 방지)
        line = line.replace(/\|([^|]+)\|/g, (_, c) => {
            edgeTexts.push(c.replace(/["'\n]/g, ' ').trim());
            return `|@@E${edgeTexts.length - 1}@@|`;
        });
        line = line.replace(/--\s*([^>|@]+?)\s*-->/g, (_, c) => {
            edgeTexts.push(c.replace(/["'\n]/g, ' ').trim());
            return `-->|@@E${edgeTexts.length - 1}@@|`;
        });

        // 노드 라벨 토큰화 (복원 시 따옴표 부여)
        line = line.replace(/\[([^\]]+)\]/g, (_, c) => { nodeTexts.push(`["${c.replace(/["'\n]/g, ' ').trim()}"]`); return `@@N${nodeTexts.length - 1}@@`; });
        line = line.replace(/\{([^}]+)\}/g,  (_, c) => { nodeTexts.push(`{"${c.replace(/["'\n]/g, ' ').trim()}"}`); return `@@N${nodeTexts.length - 1}@@`; });
        line = line.replace(/\(([^)]+)\)/g,  (_, c) => { nodeTexts.push(`("${c.replace(/["'\n]/g, ' ').trim()}")`); return `@@N${nodeTexts.length - 1}@@`; });

        // 한글 노드 ID 방지: ID 없는 노드에 영문 자동 ID 부여
        line = line.replace(/^(\s*)(@@N\d+@@)/,                  (_, sp, n) => `${sp}N_AUTO_${autoIdCount++}${n}`);
        line = line.replace(/(-->|-\.->|==>|---)\s*(@@N\d+@@)/g, (_, a, n)  => `${a} N_AUTO_${autoIdCount++}${n}`);

        // 공백 정리 후 화살표/연산자 주변에만 공백 복원
        line = line.replace(/\s+/g, '');
        line = line
            .replace(/-->/g,   ' --> ')
            .replace(/-\.->/g, ' -.-> ')
            .replace(/==>/g,   ' ==> ')
            .replace(/---/g,   ' --- ')
            .replace(/&/g,     ' & ');

        // 토큰 복원
        line = line.replace(/@@E(\d+)@@/g, (_, i) => edgeTexts[parseInt(i)]);
        line = line.replace(/@@N(\d+)@@/g, (_, i) => nodeTexts[parseInt(i)]);

        processedLines.push(line);
    }

    return processedLines.join('\n');
}


// =============================================================================
//  🔄  Mermaid 블록 처리 (processMermaidBlocks)
//
//  리포트 내 모든 ```mermaid 블록을 2단계 복구 전략으로 처리:
//    1단계 Fast-Track: sanitizeMermaid 정규식 정제 → Kroki 검증
//    2단계 QA Agent:   실패 시 Gemini 재작성 요청 → 최대 5회
//  최종 실패 블록은 ⚠️ 플레이스홀더로 대체하고 나머지 리포트는 정상 저장
// =============================================================================

async function processMermaidBlocks(reportText, qaModel) {
    const mermaidBlockRegex = /```mermaid\s*([\s\S]*?)```/gi;
    let mdText      = '';
    let pdfText     = '';
    let lastIndex   = 0;
    let brokenCount = 0;

    for (const match of [...reportText.matchAll(mermaidBlockRegex)]) {
        const preText = reportText.substring(lastIndex, match.index);
        mdText  += preText;
        pdfText += preText;

        const originalMermaid = match[1];
        let   fixedMermaid    = null;

        // ── 1단계: Fast-Track ────────────────────────────────────────────
        try {
            const cleaned = sanitizeMermaid(originalMermaid);
            const res     = await fetch(buildKrokiUrl(cleaned));
            const svg     = await res.text();
            if (isValidKrokiSvg(res, svg)) {
                console.log(`  -> ⚡ [Fast-Track 성공]`);
                fixedMermaid = cleaned;
            }
        } catch { /* fetch 실패 → 2단계로 */ }

        // ── 2단계: QA Agent ──────────────────────────────────────────────
        if (!fixedMermaid) {
            console.log(`  -> ⚠️  [Fast-Track 실패] QA 에이전트 호출...`);
            let currentMermaid = originalMermaid;

            for (let attempt = 1; attempt <= MAX_QA_RETRIES; attempt++) {
                const warning = attempt > 1
                    ? '**[경고] 이전 시도에서 파서 에러가 발생했습니다! 화살표 텍스트는 10자 이내로 짧게 쓰십시오.**\n'
                    : '';

                const qaPrompt = `${warning}
1. [ERD 규칙]:       \`erDiagram\` 속성에 따옴표나 코멘트를 모두 지우고 '타입 이름'만 남기세요.
2. [Flowchart 규칙]: 모든 \`subgraph\` 이름은 반드시 큰따옴표(\`""\`)로 감쌀 것.
3. [노드 ID 규칙]:   노드 ID는 반드시 띄어쓰기 없는 영문+숫자 조합(예: A1, Node2)으로만 작성. 한글 노드 ID 절대 금지.

[원본 코드]:
${currentMermaid}`;

                const qaResultText = await callGeminiWithRetry(qaModel, qaPrompt, 3);

                if (!qaResultText) { await delay(15000); continue; }

                try {
                    const cleaned = sanitizeMermaid(
                        qaResultText.replace(/```mermaid\s*/ig, '').replace(/```/g, '').trim()
                    );
                    const res = await fetch(buildKrokiUrl(cleaned));
                    const svg = await res.text();

                    if (isValidKrokiSvg(res, svg)) {
                        console.log(`  -> ✅ [시도 ${attempt}/${MAX_QA_RETRIES}] QA 복구 성공!`);
                        fixedMermaid = cleaned;
                        await delay(15000);
                        break;
                    } else {
                        currentMermaid = cleaned;
                    }
                } catch { /* Kroki fetch 실패 → 다음 시도 */ }

                await delay(15000);
            }
        }

        // ── 결과 반영 ────────────────────────────────────────────────────
        if (fixedMermaid) {
            mdText  += `\`\`\`mermaid\n${fixedMermaid}\n\`\`\``;
            pdfText += `\n\n<div style="page-break-inside:avoid;break-inside:avoid;text-align:center;width:100%;">` +
                       `<img src="${buildKrokiUrl(fixedMermaid)}" alt="시스템 다이어그램" ` +
                       `style="max-width:100%;max-height:220mm;height:auto;object-fit:contain;margin:0 auto;display:block;" />` +
                       `</div>\n\n`;
        } else {
            brokenCount++;
            console.log(`  -> 🚨 [다이어그램 복구 실패] 플레이스홀더로 대체. (누적 ${brokenCount}개)`);
            const placeholder = `\n\n> ⚠️ **[다이어그램 렌더링 실패]** Mermaid 파싱 오류로 인해 이 다이어그램을 표시할 수 없습니다.\n\n`;
            mdText  += placeholder;
            pdfText += placeholder;
        }

        lastIndex = match.index + match[0].length;
    }

    const remaining = reportText.substring(lastIndex);
    mdText  += remaining;
    pdfText += remaining;

    return { mdText, pdfText, brokenCount };
}


// =============================================================================
//  🌐  HTML 리포트 템플릿
// =============================================================================

function buildHtmlReport(gameTitle, bodyHtml) {
    return `<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${gameTitle} 역기획서</title>
    <style>
        @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css');

        :root {
            --primary:   #4F46E5;
            --bg:        #F3F4F6;
            --card-bg:   #FFFFFF;
            --text-main: #1F2937;
            --border:    #E5E7EB;
        }

        body             { font-family: 'Pretendard', -apple-system, sans-serif; background-color: var(--bg); color: var(--text-main); line-height: 1.75; margin: 0; padding: 40px 20px; }
        .report-container{ max-width: 900px; margin: 0 auto; background: var(--card-bg); padding: 50px 70px; border-radius: 24px; box-shadow: 0 20px 25px -5px rgba(0,0,0,.1), 0 10px 10px -5px rgba(0,0,0,.04); }

        h1          { font-size: 2.4em; font-weight: 800; color: #111827; border-bottom: 4px solid var(--primary); padding-bottom: 15px; margin-bottom: 30px; letter-spacing: -0.02em; }
        h2          { font-size: 1.6em; font-weight: 700; color: var(--primary); margin-top: 2.5em; border-bottom: 1px solid var(--border); padding-bottom: 10px; }
        h3          { font-size: 1.3em; font-weight: 600; color: #374151; margin-top: 1.8em; }
        blockquote  { background: #EEF2FF; border-left: 5px solid var(--primary); padding: 20px; margin: 25px 0; border-radius: 0 12px 12px 0; color: #4338CA; font-weight: 500; font-size: 1.05em; }

        table       { width: 100%; border-collapse: separate; border-spacing: 0; margin: 30px 0; border-radius: 12px; border: 1px solid var(--border); box-shadow: 0 4px 6px -1px rgba(0,0,0,.05); table-layout: auto; }
        th          { background-color: #F9FAFB; padding: 16px; font-weight: 600; text-align: left; border-bottom: 1px solid var(--border); color: #374151; word-break: keep-all; }
        td          { padding: 16px; border-bottom: 1px solid var(--border); word-break: keep-all; overflow-wrap: anywhere; }
        tr:last-child td { border-bottom: none; }

        pre         { background: #1E293B; color: #F8FAFC; padding: 20px; border-radius: 12px; overflow-x: auto; margin: 20px 0; white-space: pre-wrap; word-break: break-all; }
        code        { font-family: monospace; font-size: 0.9em; background: #F1F5F9; color: #E11D48; padding: 4px 8px; border-radius: 6px; word-break: break-all; }
        pre code    { background: transparent; color: inherit; padding: 0; }

        img         { display: block; margin: 40px auto; max-width: 100% !important; height: auto !important; object-fit: contain; border-radius: 12px; box-shadow: 0 10px 15px -3px rgba(0,0,0,.1); }
        hr          { border: 0; height: 1px; background: var(--border); margin: 40px 0; }

        @media (max-width: 768px) {
            body              { padding: 15px 10px; }
            .report-container { padding: 30px 20px; border-radius: 16px; }
            h1                { font-size: 1.8em; }
            h2                { font-size: 1.4em; }
        }
    </style>
</head>
<body>
    <div class="report-container">
        ${bodyHtml}
    </div>
</body>
</html>`;
}


// =============================================================================
//  🔭  buildScoutPrompt — 공식 가이드 기준 시스템명 수집 (최대 3회, 실패 시 ABORT)
//  1회: 공식 가이드·공홈·카페  2회: 나무위키  3회: 커뮤니티 교차확인
// =============================================================================

function buildScoutPrompt(game, attempt = 1) {
    const storeUrl = `https://play.google.com/store/apps/details?id=${game.appId}`;

    const strategies = {
        1: {
            label: '공식 가이드·공홈·카페 (1차 출처)',
            queries: [
                `${game.title} 공식 가이드 시스템 소개`,
                `${game.title} ${game.developer} 공식 홈페이지 게임 소개 시스템`,
                `${game.title} 네이버 공식카페 공략 시스템 재화`,
            ],
            instruction: `
## 이번 회차 핵심 지시
개발사(${game.developer})가 공식적으로 배포한 가이드, 공식 홈페이지, 공식 카페에서만 명칭을 수집하십시오.
공식 출처에서 확인된 명칭만 [시스템명]·[재화명]에 기재하십시오.
공식 출처 URL이 없으면 이번 회차는 실패로 처리됩니다.`,
        },
        2: {
            label: '나무위키 전용 (2차 정리 출처)',
            queries: [
                `나무위키 ${game.title} 콘텐츠 시스템`,
                `나무위키 ${game.title} 재화 종류`,
            ],
            instruction: `
## 이번 회차 핵심 지시
namu.wiki 에서 ${game.title} 문서를 찾아 시스템명·재화명을 수집하십시오.
나무위키 문서 URL이 없으면 이번 회차는 실패로 처리됩니다.`,
        },
        3: {
            label: '커뮤니티 교차 검증 (3차 확인)',
            queries: [
                `${game.title} 인벤 OR 아카라이브 시스템 재화 공략`,
                `${game.title} 초보 가이드 시스템 목록`,
            ],
            instruction: `
## 이번 회차 핵심 지시
인벤(inven.co.kr) 또는 아카라이브(arca.live)에서 ${game.title} 공략 문서를 찾아
시스템명·재화명을 수집하십시오.
복수 출처에서 동일한 명칭이 확인될수록 신뢰도가 높습니다.`,
        },
    };

    const s = strategies[attempt] || strategies[3];

    return `
# [팩트 수집 ${attempt}회차] ${game.title} — 공식 명칭 교차검증

## ⚠️ 타겟 게임 고정 (절대 변경 금지)
- 게임명:    ${game.title}
- 앱 ID:     ${game.appId}
- 스토어URL: ${storeUrl}
- 개발사:    ${game.developer}

---

## 검색 전략: ${s.label}

### 지정 검색어 (순서대로 실행)
${s.queries.map((q, i) => (i + 1) + '. ' + q).join('\n')}

${s.instruction}

---

## 수집 항목

**[시스템명]** 게임 UI에 실제 표시되는 콘텐츠/기능 이름
- 반드시 출처 문서에 실제로 적힌 명칭 그대로
- 최대 12개, 쉼표 구분
- ❌ 금지: 다른 게임 명칭 유추, 일반적인 단어로 대체

**[재화명]** 게임 내 화폐·포인트·재료 명칭 (UI 표기 그대로)
- 최대 12개, 쉼표 구분
- ❌ 금지: "골드", "다이아" 같은 일반 명칭으로 추측

**[메뉴명]** 메인 화면 하단 탭 또는 주요 진입 버튼 명칭
- 최대 8개, 쉼표 구분

**[출처URL]** 위 명칭을 직접 확인한 페이지 URL (필수 1개 이상)

---

## 출력 형식 (이 형식 외 텍스트 절대 금지)
[시스템명]  (쉼표 구분)
[재화명]    (쉼표 구분)
[메뉴명]    (쉼표 구분)
[출처URL]   (URL1, URL2, ...)
[출처신뢰도] 높음 / 보통 / 낮음

## 신뢰도 기준
- 높음: 공식 가이드·공홈·카페에서 직접 확인
- 보통: 나무위키·인벤 등 신뢰 커뮤니티에서 확인
- 낮음: 단일 비공식 출처에서만 확인

## 중단 조건
- ${game.title}이 아닌 다른 게임 명칭이 섞이면: [IP_CONFUSED]
- 지정 검색어 실행 후 관련 데이터가 전혀 없으면: [ABORT_NO_DATA]
`;
}


// =============================================================================
//  📝  buildAnalysisPrompt — 역기획서 생성 프롬프트
// =============================================================================

function buildAnalysisPrompt(game, rank, category, factSheet = '') {
    const storeUrl = `https://play.google.com/store/apps/details?id=${game.appId}`;

    return `
# ⚠️ [최우선 준수 사항] 타겟 게임 식별 — 위반 시 전체 출력 무효
아래 정보가 이번 분석의 유일한 타겟입니다. 분석 시작 전 반드시 숙지하십시오.

* **정확한 게임명:** ${game.title}
* **개발사:**        ${game.developer}
* **앱 ID:**         ${game.appId}
* **스토어 URL:**    ${storeUrl}
* **구글 매출 순위:** ${rank}위
* **분석 타겟 영역:** ${category}

### 혼동 방지 체크리스트 (분석 전 자가 점검)
- [ ] 검색 시 반드시 "${game.title}" 정확한 명칭만 사용했는가?
- [ ] 같은 IP의 다른 플랫폼/다른 게임(예: "메이플스토리M" ≠ "메이플 키우기") 데이터를 혼용하지 않았는가?
- [ ] 검색 결과의 앱 ID가 "${game.appId}"와 일치하는 게임의 정보인가?
- [ ] 위 3가지 중 하나라도 불확실하면 해당 데이터는 반드시 **"데이터 비공개 (검색 불가)"** 로 대체했는가?

---

# Step 0: 메타데이터 정의 (절대 수정 금지)
최상단에 반드시 다음 3줄을 작성하십시오.
메인장르: (RPG / MMORPG / 방치형 / SLG/전략 / 캐주얼/퍼즐 / 액션/슈팅 / SNG/시뮬레이션 / 스포츠/레이싱 / 카지노/보드 / 기타 중 하나)
서브장르: (15자 이내 자유 형식)
시스템:   (15자 이내 명사형, 파일명에 사용될 핵심 시스템명)

# [고정 어휘 사전] — 아래 명칭 외 시스템명·재화명을 임의로 생성하지 마십시오
${factSheet ? `${factSheet}

## 준수 규칙
1. [시스템명] 목록의 이름 → 반드시 그대로 사용 (동의어·축약·번역 금지)
2. [재화명] 목록의 이름   → 반드시 그대로 사용
3. 목록에 없는 명칭       → "데이터 비공개 (검색 불가)"로 표기
4. [출처신뢰도]가 낮음    → 해당 명칭 사용 시 *(출처 미검증)* 주석 추가
` : '⚠️ 팩트 사전 없음 — 딥 서치 직접 확인. 불가 명칭은 데이터 비공개 표기.'}
# Step 1: 실제 게임 내 UI 표기 명칭 타겟팅
1. [${category}] 영역을 대표하는 시그니처 시스템 1개를 특정하십시오.
   **반드시 위 [고정 어휘 사전]의 [시스템명] 중에서 선택하십시오.**
2. 유저가 게임 내에서 직접 클릭할 수 있는 **정확한 UI 텍스트(메뉴명)** 기준으로 분석하십시오.

# Step 2: 실무형 시스템 역기획서 작성 (Endfield Reference Format)
기획팀과 개발팀이 타사 시스템을 해부하기 위한 '역기획 및 심리 분석 명세서'입니다.
아래 9단계 구조에 맞춰 마크다운으로 작성하십시오. (05·07·08번 표 형식 강제)

01. **정의 및 기획 의도** 시스템 개요 + 도입 의도(수익화/리텐션/트래픽 유도 등)
02. **시스템 구조도** 핵심 서브시스템 연결 관계 (★ Mermaid \`graph LR\` 강제)
03. **이용 플로우차트** 유저 핵심 이용 흐름 (★ Mermaid \`flowchart TD\` 강제)
04. **상세 명세 및 심리 설계** 인터랙션/상태 전이 명세 + 심리 트리거(FOMO/손실회피 등) 해부
05. **데이터 테이블 및 수치 밸런스** 재화 Source/Sink 구조 (★ 표 형식 강제)
06. **확장형 DB ERD** 백엔드 DB 테이블 설계 (★ Mermaid \`erDiagram\` 강제)
07. **예외 처리 명세** 엣지 케이스 / 어뷰징 방지 / 한계 도달 처리 (★ 표 형식 강제)
08. **비교 분석 및 인사이트** 유사 장르 탑 티어 게임과의 비교 매트릭스 + 개선 제안 (★ 표 형식 강제)
09. **참고 문헌 및 팩트 체크 출처** (★ 필수: 실제 URL 최소 2개. 반드시 ${game.title} 관련 URL만 사용)

# ★ 딥 서치 및 환각 방지 철칙
1. **[최우선] 게임 식별 고정**: 모든 검색은 "${game.title}" + 앱ID "${game.appId}" 기준으로만 수행.
   같은 IP를 공유하는 다른 게임이 검색되면 즉시 검색어를 바꾸십시오.
2. **심층 검색망 가동**: 1차 검색 부족 시 나무위키·Game8·NGA·유튜브 패치노트 요약글까지 파헤치십시오.
3. **데이터 검증**: 최신 라이브 서버 기준, 복수 출처 교차 검증. 찾을 수 없으면 **"데이터 비공개 (검색 불가)"** 명시.

# Output Constraints
* [사고 과정 노출 금지]  내부 검색/검증 과정은 텍스트로 노출하지 마십시오.
* [Mermaid 규칙]         화살표 텍스트(\`-->|텍스트|\`)는 10자 이내. 대괄호/중괄호 안에 콜론·따옴표·쉼표 절대 금지.
* [노드 ID 규칙]         노드 ID는 반드시 띄어쓰기 없는 영문+숫자 조합(예: A1, NodeB2). 한글 노드 ID 절대 금지.
* 데이터가 전혀 없는 비주류 게임이면 [ABORT_NO_DATA] 한 줄만 출력하고 종료.
* 타겟 게임이 아닌 다른 게임의 데이터가 섞였다고 판단되면 [IP_CONFUSED] 한 줄만 출력하고 종료.
`;
}


// =============================================================================
//  🏃  메인 파이프라인
// =============================================================================

async function main() {

    // ── 0. 환경 변수 사전 검증 ──────────────────────────────────────────────
    const REQUIRED_ENV = ['GCP_CLIENT_ID', 'GCP_CLIENT_SECRET', 'GCP_REFRESH_TOKEN', 'GDRIVE_FOLDER_ID', 'GEMINI_API_KEY'];
    const missingEnv   = REQUIRED_ENV.filter(k => !process.env[k]);
    if (missingEnv.length > 0) {
        console.error(`❌ 필수 환경 변수 누락: ${missingEnv.join(', ')}`);
        process.exit(1);
    }

    const errorLog = []; // 에러 누적 (GitHub Actions 콘솔에서 확인)

    try {

        // ── 1. Google Play 순위 스크래핑 ────────────────────────────────────
        const gplayModule = await import('google-play-scraper');
        const gplay       = gplayModule.default || gplayModule;

        const rawGames = await gplay.list({
            collection: gplay.collection.GROSSING,
            category:   gplay.category.GAME,
            num:        100,
            country:    'kr',
            lang:       'ko',
        });
        const allGames = rawGames.map((game, index) => ({ ...game, actualRank: index + 1 }));

        const dateParts   = getKSTDateParts();
        const dateString  = dateParts.dateString;

        // ── 2. Drive 폴더 구조 생성 ──────────────────────────────────────────
        let mdFolderId, pdfFolderId, htmlFolderId;
        try {
            ({ mdFolderId, pdfFolderId, htmlFolderId } = await createDriveFolders(dateParts));
        } catch (folderErr) {
            console.error(`❌ Drive 폴더 구조 생성 실패: ${folderErr.message}`);
            process.exit(1);
        }

        // ── 3. 처리 대상 게임 목록 슬라이싱 ────────────────────────────────
        const targetGames = allGames.slice(START_RANK - 1, END_RANK);
        console.log(`\n[${dateString}] 🗄️  파이프라인 가동 (${START_RANK}위 ~ ${END_RANK}위, 총 ${targetGames.length}개)`);

        const stats = { full: 0, partial: 0, skipped: 0, diagram: 0 };

        // ── 4. 게임별 역기획서 생성 루프 ────────────────────────────────────
        for (let idx = 0; idx < targetGames.length; idx++) {
            const game     = targetGames[idx];
            const rank     = game.actualRank;
            const progress = `[${idx + 1}/${targetGames.length}]`;

            // 4-1. 앱 출시일 수집
            let releaseDate = '정보 없음';
            try {
                const detail = await gplay.app({ appId: game.appId });
                releaseDate  = detail.released || '정보 없음';
            } catch {
                console.log(`  -> ⚠️  출시일 수집 실패`);
            }

            // 4-2. Gemini 모델 초기화 (Round-Robin 키 순환)
            const genAI                          = new GoogleGenerativeAI(apiKeyQueue.next());
            const { scoutModel, draftModel, qaModel } = initModels(genAI, game.title, game.appId);

            const category = ANALYSIS_CATEGORIES[Math.floor(Math.random() * ANALYSIS_CATEGORIES.length)];
            console.log(`\n${progress} 매출 ${rank}위: ${game.title}`);
            console.log(`  -> 🎯 분석 영역: [${category}] / 출시일: ${releaseDate}`);

            // 4-3. Scout — 공식 가이드 기준 시스템명 수집
            // minInstalls 기준으로 최대 시도 횟수 결정: 100만+→3회 / 10만+→2회 / 미만→1회
            const gameInstalls      = game.minInstalls || 0;
            const MAX_SCOUT_RETRIES = gameInstalls >= 1_000_000 ? 3
                                    : gameInstalls >= 100_000   ? 2
                                    : 1;
            if (MAX_SCOUT_RETRIES < 3) {
                console.log(`  -> ℹ️  [SCOUT-LIMIT] 설치수 ${gameInstalls.toLocaleString()}. scout 최대 ${MAX_SCOUT_RETRIES}회.`);
            }

            let factSheet    = '';
            let scoutAborted = false;
            const scoutLabels = ['공식 가이드', '나무위키', '커뮤니티'];

            for (let sAttempt = 1; sAttempt <= MAX_SCOUT_RETRIES; sAttempt++) {
                try {
                    await delay(3000);
                    console.log(`  -> 🔭 [SCOUT ${sAttempt}/${MAX_SCOUT_RETRIES}] ${scoutLabels[sAttempt - 1]} 탐색...`);
                    const scoutText = await callGeminiWithRetry(scoutModel, buildScoutPrompt(game, sAttempt), 2);

                    if (!scoutText) { continue; }

                    if (scoutText.includes('[ABORT_NO_DATA]')) {
                        console.log(`  -> ⏭️  [SCOUT-ABORT] 데이터 없음. ABORT.`);
                        scoutAborted = true;
                        break;
                    }
                    if (scoutText.includes('[IP_CONFUSED]')) {
                        console.log(`  -> ⚠️  [SCOUT-IP ${sAttempt}회] IP 혼동. 다음 회차로.`);
                        continue;
                    }

                    const hasUrl = scoutText.includes('[출처URL]') && /https?:\/\//.test(scoutText);
                    if (!hasUrl) {
                        console.log(`  -> ⚠️  [SCOUT-NO-URL ${sAttempt}회] URL 없음. 다음 회차로.`);
                        continue;
                    }

                    const trustMatch = scoutText.match(/\[출처신뢰도\]\s*(높음|보통|낮음)/);
                    const trust      = trustMatch ? trustMatch[1] : '낮음';

                    if (trust === '낮음' && sAttempt < MAX_SCOUT_RETRIES) {
                        console.log(`  -> ⚠️  [SCOUT-LOW ${sAttempt}회] 신뢰도 낮음. 다음 회차로.`);
                        continue;
                    }

                    factSheet = scoutText;
                    console.log(`  -> ✅ [SCOUT-OK] 완료 (신뢰도: ${trust}, ${sAttempt}회차)`);
                    break;

                } catch (scoutErr) {
                    console.log(`  -> ⚠️  [SCOUT-ERR ${sAttempt}회] ${scoutErr.message?.substring(0, 60)}`);
                }
            }

            if (scoutAborted || !factSheet) {
                const reason = scoutAborted ? 'ABORT_NO_DATA' : `${MAX_SCOUT_RETRIES}회 전부 URL/신뢰도 미달`;
                const errMsg = `[${rank}위] ${game.title} — Scout 실패 (${reason})`;
                console.error(`  -> ❌ [SCOUT-ABORT] ${errMsg}`);
                errorLog.push(errMsg);
                stats.skipped++;
                continue;
            }

            // 4-4. 역기획서 초안 생성
            const reportRaw = await callGeminiWithRetry(draftModel, buildAnalysisPrompt(game, rank, category, factSheet), MAX_DRAFT_RETRIES);

            if (!reportRaw) {
                const errMsg = `[${rank}위] ${game.title} — Draft 생성 ${MAX_DRAFT_RETRIES}회 실패`;
                console.error(`  -> ❌ ${errMsg}`);
                errorLog.push(errMsg);
                stats.skipped++;
                continue;
            }

            // 4-5. 리포트 텍스트 정제
            let reportText = reportRaw
                .replace(/^```(markdown|md)?/i, '')
                .replace(/```$/i, '')
                .trim();

            // AI가 메타데이터를 중복 출력한 경우 마지막 것만 사용
            const metaMatches = [...reportText.matchAll(/메인장르:/g)];
            if (metaMatches.length > 1) {
                reportText = reportText.substring(metaMatches[metaMatches.length - 1].index);
            }

            // 파일명용 핵심 시스템명 추출
            const systemMatch  = reportText.match(/시스템:\s*([^\n]+)/);
            const coreSystemName = systemMatch
                ? systemMatch[1].replace(/\[\/META\]/gi, '').replace(/[/\\?%*:|"<>]/g, '_').trim()
                : '시스템_통합_분석';

            // 메타데이터 줄 제거 후 헤더 붙이기
            reportText = reportText
                .replace(/메인장르:.*?\n/g, '')
                .replace(/서브장르:.*?\n/g, '')
                .replace(/시스템:.*?\n/g,   '')
                .trim();

            reportText = [
                `# [${rank}위] ${game.title} 역기획서`,
                `> **분석 타겟:** ${category}`,
                `> **핵심 시스템:** ${coreSystemName}`,
                `> **개발사:** ${game.developer}`,
                `> **작성일:** ${dateString}`,
                `> **출시일:** ${releaseDate}`,
                '',
                '---',
                '',
                reportText,
            ].join('\n');

            // 4-6. Mermaid 블록 처리
            const { mdText, pdfText, brokenCount } = await processMermaidBlocks(reportText, qaModel);
            if (brokenCount > 0) stats.diagram++;

            // 4-7. 파일명 생성
            const safeTitle    = game.title.replace(/[/\\?%*:|"<>]/g, '_');
            const baseFileName = `[${dateString}]_${String(rank).padStart(3, '0')}위_${safeTitle}_(${coreSystemName})`;

            // 4-8. MD / PDF / HTML 저장
            const uploads = [
                { tag: 'MD',   ext: '.md',   folderId: mdFolderId,   mimeType: 'text/markdown',   content: mdText,                        validate: () => mdText.length >= 10 },
                { tag: 'PDF',  ext: '.pdf',  folderId: pdfFolderId,  mimeType: 'application/pdf', content: null,                          validate: null },
                { tag: 'HTML', ext: '.html', folderId: htmlFolderId, mimeType: 'text/html',        content: null,                          validate: null },
            ];

            let savedCount = 0;

            for (const u of uploads) {
                try {
                    let content = u.content;
                    if (u.tag === 'PDF') {
                        console.log(`  -> 📄 [PDF]  변환 시작...`);
                        const pdfData = await mdToPdf({ content: pdfText }, PDF_OPTIONS);
                        if (!pdfData?.content) throw new Error('PDF 엔진이 빈 데이터를 반환했습니다.');
                        content = pdfData.content;
                    } else if (u.tag === 'HTML') {
                        console.log(`  -> 🌐 [HTML] 변환 시작...`);
                        const parsedBody = marked.parse(pdfText);
                        if (!parsedBody?.trim()) throw new Error('HTML 파싱 결과가 비어있습니다.');
                        content = buildHtmlReport(game.title, parsedBody);
                    } else if (u.validate && !u.validate()) {
                        throw new Error('MD 데이터가 비어있습니다.');
                    }

                    const saved = await uploadToDrive({ fileName: `${baseFileName}${u.ext}`, folderId: u.folderId, mimeType: u.mimeType, content });
                    if (saved) { console.log(`  -> 💾 [${u.tag.padEnd(4)}] 저장 완료`); savedCount++; }
                    else       { savedCount++; } // SKIP도 성공으로 집계
                } catch (e) {
                    console.error(`  -> ❌ [${u.tag.padEnd(4)}] 저장 실패: ${e.message}`);
                    errorLog.push(`[${u.tag}] ${baseFileName}: ${e.message}`);
                }
            }

            // 4-9. 결과 집계
            if      (savedCount === 3) { stats.full++; }
            else if (savedCount >= 1)  { stats.partial++; console.log(`  -> ⚠️  일부 포맷 저장 실패 (${savedCount}/3)`); }
            else                       { console.error(`  -> ❌ 모든 포맷 저장 실패`); }

            if (idx < targetGames.length - 1) await delay(30000);
        }

        // ── 5. 최종 결산 ─────────────────────────────────────────────────────
        const failedCount = targetGames.length - stats.full - stats.partial - stats.skipped;
        console.log(`\n${'='.repeat(56)}`);
        console.log(`[${dateString}] 📊 최종 결산`);
        console.log(`  목표 처리량              ${targetGames.length}개`);
        console.log(`  완전 성공 (3포맷 모두)   ${stats.full}개`);
        console.log(`  부분 성공 (1~2포맷)      ${stats.partial}개`);
        console.log(`  다이어그램 일부 깨짐      ${stats.diagram}개  ← 리포트는 저장됨`);
        console.log(`  자동 스킵 (데이터 부족)  ${stats.skipped}개`);
        console.log(`  완전 실패                ${failedCount}개`);
        console.log(`${'='.repeat(56)}`);
        console.log(`🎉 Google Drive 동기화 완료`);
        console.log(`${'='.repeat(56)}\n`);

    } catch (fatalError) {
        console.error('💀 치명적 에러 발생:', fatalError);
        process.exit(1);
    }
}

main();
