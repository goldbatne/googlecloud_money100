'use strict';

// =============================================================================
//
//  📦 Google Play 게임 게임 분석 문서 자동 생성 파이프라인
//
//  흐름 요약:
//    1. Google Play 매출 순위 스크래핑
//    2. Scout  — 공식 가이드 기준 시스템명·재화명 수집 (최대 3회)
//    3. Writer — Gemini API 분석 문서 초안 생성 (딥서치)
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

// Fisher-Yates 셔플 후 순환 — 14게임마다 모든 카테고리 균등 커버
// Math.random() 방식 대비 최대-최소 편차를 1 이내로 유지
const _categoryQueue = (() => {
    const arr = [...ANALYSIS_CATEGORIES];
    for (let i = arr.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [arr[i], arr[j]] = [arr[j], arr[i]];
    }
    return arr;
})();
let _categoryIdx = 0;
function pickCategory() {
    if (_categoryIdx >= _categoryQueue.length) {
        // 한 바퀴 완료 → 재셔플
        for (let i = _categoryQueue.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [_categoryQueue[i], _categoryQueue[j]] = [_categoryQueue[j], _categoryQueue[i]];
        }
        _categoryIdx = 0;
    }
    return _categoryQueue[_categoryIdx++];
}

// PDF 변환 옵션 (md-to-pdf / Puppeteer 기반)
// --max-old-space-size: 50회 루프 시 Puppeteer OOM 방지
// --font-render-hinting=none: GitHub Actions 환경 한글 폰트 렌더링 안정화
const PDF_OPTIONS = {
    timeout: 120000,
    launch_options: {
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--font-render-hinting=none',
            '--js-flags=--max-old-space-size=512',
        ],
    },
    // Noto Sans KR: Google Fonts CDN → Puppeteer가 웹폰트 로드 가능
    // (시스템 폰트 의존 제거. waitForNetworkIdle 없이도 @font-face가 먼저 평가됨)
    css: `
        @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@400;500;700;800&display=swap');

        * { box-sizing: border-box; }

        body {
            font-family: 'Noto Sans KR', sans-serif;
            font-size: 10pt;
            line-height: 1.7;
            color: #1F2937;
            margin: 0;
            padding: 6mm 0;
            word-break: break-all;
            overflow-wrap: anywhere;
        }

        h1 {
            font-size: 18pt; font-weight: 800; color: #111827;
            border-bottom: 3px solid #4F46E5;
            padding-bottom: 8px; margin: 0 0 18px;
            page-break-after: avoid;
        }
        h2 {
            font-size: 13pt; font-weight: 700; color: #4F46E5;
            border-bottom: 1px solid #E5E7EB;
            padding-bottom: 5px; margin: 24px 0 10px;
            page-break-after: avoid;
        }
        h3 {
            font-size: 11pt; font-weight: 700; color: #374151;
            margin: 18px 0 8px;
            page-break-after: avoid;
        }

        blockquote {
            background: #EEF2FF;
            border-left: 4px solid #4F46E5;
            padding: 10px 14px;
            margin: 14px 0;
            color: #4338CA;
            font-weight: 500;
            font-size: 9.5pt;
            page-break-inside: avoid;
        }

        /* 표: 고정 레이아웃 + 셀 넘침 방지 */
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 16px 0;
            font-size: 8.5pt;
            table-layout: fixed;
            page-break-inside: auto;
        }
        th, td {
            border: 1px solid #D1D5DB;
            padding: 6px 8px;
            text-align: left;
            vertical-align: top;
            word-break: break-all;
            overflow-wrap: anywhere;
            hyphens: auto;
        }
        th {
            background-color: #F3F4F6;
            font-weight: 700;
            color: #111827;
        }
        tr { page-break-inside: avoid; }

        pre {
            background: #F3F4F6;
            padding: 12px;
            border-radius: 6px;
            margin: 12px 0;
            white-space: pre-wrap;
            word-break: break-all;
            font-size: 8pt;
            page-break-inside: avoid;
        }
        code {
            font-family: 'Courier New', monospace;
            font-size: 8.5pt;
            color: #DB2777;
            background: #FDF2F8;
            padding: 1px 4px;
            border-radius: 3px;
            word-break: break-all;
        }
        pre code { background: transparent; color: inherit; padding: 0; }

        /* 다이어그램: Base64 인라인 SVG — 외부 URL fetch 불필요 */
        .diagram-wrap {
            text-align: center;
            margin: 16px 0;
            page-break-inside: avoid;
        }
        .diagram-wrap img {
            max-width: 100%;
            max-height: 200mm;
            height: auto;
            display: block;
            margin: 0 auto;
        }

        p  { margin: 8px 0; }
        hr { border: 0; border-top: 1px solid #E5E7EB; margin: 20px 0; }
        ul, ol { padding-left: 20px; margin: 8px 0; }
        li { margin: 3px 0; }
    `,
    pdf_options: {
        format: 'A4',
        margin: { top: '18mm', right: '18mm', bottom: '18mm', left: '18mm' },
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

/**
 * PDF 변환 래퍼 — EADDRINUSE 포트 충돌 시 대기 후 재시도
 * md-to-pdf 내부 서버가 이전 실패 후 포트를 점유한 채 남을 수 있음.
 * 최대 3회 재시도, 실패 간 5초 대기로 포트 해제 여유를 줌.
 */
async function convertToPdf(mdText, options, maxRetries = 3) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            const pdfData = await mdToPdf({ content: mdText }, options);
            if (!pdfData?.content) throw new Error('PDF 엔진이 빈 데이터를 반환했습니다.');
            return pdfData.content;
        } catch (err) {
            const isPortConflict = err.code === 'EADDRINUSE' || err.message?.includes('EADDRINUSE');
            if (isPortConflict && attempt < maxRetries) {
                console.log(`  -> ⏱️  [PDF] 포트 충돌(EADDRINUSE) — ${attempt}회 실패. 5초 대기 후 재시도...`);
                await delay(5000);
                continue;
            }
            throw err;
        }
    }
}

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
            if (attempt < maxRetries) await delay(waitTime); // 마지막 실패 시 대기 불필요
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
    const safeName = folderName.replace(/'/g, "\\'"); // Drive 쿼리 인젝션 방어
    const res = await drive.files.list({
        q:      `name = '${safeName}' and '${parentId}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
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
 * - draftModel:  분석 문서 작성 (Google Search 활성화, 딥서치)
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
            `당신은 검색 기반 팩트 추출 엔진입니다. ` +
            `분석 대상은 오직 "${gameTitle}" (앱ID: ${appId}) 단 하나입니다. ` +
            '같은 IP를 공유하더라도 이름이 다른 게임의 데이터를 절대 혼용하지 마십시오. ' +
            '검색 결과가 타겟 게임과 다른 게임이면 즉시 검색어를 바꾸십시오. ' +
            '모든 항목은 반드시 검색으로 확인된 사실만 작성하십시오. 확인되지 않은 내용은 추측 없이 "데이터 비공개 (검색 불가)"로 표기하십시오. ' +
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
//    1. 공통 전처리 (유니코드 제로폭 문자, 주석 제거) — 따옴표 제거는 아직 하지 않음
//    2. 다이어그램 타입 감지 후 경로 분기 (erDiagram / flowchart·graph)
//       - erDiagram: 따옴표가 관계 레이블에 필수이므로 독립 경로에서 처리
//       - flowchart:  공통 특수문자 제거 후 토큰화 처리
//    ※ 이전 구조는 공통 전처리에서 " 를 먼저 제거한 뒤 erDiagram에서 "has"를
//       다시 붙이는 순서 의존성이 있었음 → 분기 먼저 하는 구조로 수정
// =============================================================================

function sanitizeMermaid(rawCode) {

    // ── 1단계: 공통 전처리 (따옴표 제거 제외) ────────────────────────────────
    let code = rawCode
        .replace(/[\u200B-\u200D\uFEFF]/g, '') // 유니코드 제로폭 문자 제거
        .replace(/\/\/.*$/gm, '')              // // 주석 제거
        .replace(/%%.*$/gm, '')                // %% Mermaid 주석 제거
        .replace(/^\s*(\d+\.|[-*])\s+/gm, '') // 목록 기호(1. / - / *) 제거
        .trim();

    // ── 2단계: erDiagram 전용 경로 ──────────────────────────────────────────
    // 따옴표 제거 전에 감지 → erDiagram은 관계 레이블에 "has" 큰따옴표 필수
    if (/^erDiagram/i.test(code)) {

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

        // erDiagram 전용 정제 (따옴표는 "has" 레이블용으로 마지막에 직접 부여)
        code = code
            .replace(/['*#]/g,    '')                    // 작은따옴표·* · # 만 제거 (큰따옴표는 유지)
            .replace(/erDiagram\s+(.*)/i, 'erDiagram\n$1')
            .replace(/\(.*?\)/g,  '')
            .replace(/,/g,        '\n')
            .replace(/\bENUM\b/gi, '')
            .replace(/\b(PK|FK|UK|Optional)\b/gi, '')
            .replace(/^[a-zA-Z가-힣0-9_]+\s*:\s*(?=[a-zA-Z0-9_]+\s*\|\|--)/gm, '')
            // 관계 레이블 → "has" (큰따옴표 직접 삽입 — 공통 전처리가 이미 지우지 않음)
            .replace(/(\|\|--o{|}\|--\|{|}\|--o{|\|\|--\|{|}-o|}-\||-o|-\|)\s*([a-zA-Z0-9_]+)\s*:\s*(.*?)$/gm, '$1 $2 : "has"');

        return code;
    }

    // ── 3단계: flowchart / graph 전용 경로 ──────────────────────────────────
    // erDiagram이 아닌 경우에만 따옴표 포함 특수문자 전체 제거
    code = code.replace(/["'*#]/g, '');

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
//
//  mode 파라미터:
//    'pdf'  → Base64 인라인 SVG img 태그 (PDF 렌더링용, 외부 fetch 없음)
//    'html' → Kroki URL img 태그 (HTML 경량화, 파일 크기 절감)
// =============================================================================

async function processMermaidBlocks(reportText, qaModel, mode = 'pdf') {
    const mermaidBlockRegex = /```mermaid\s*([\s\S]*?)```/gi;
    let mdText      = '';
    let lastIndex   = 0;
    let brokenCount = 0;

    for (const match of [...reportText.matchAll(mermaidBlockRegex)]) {
        mdText += reportText.substring(lastIndex, match.index);

        const originalMermaid = match[1];
        let   fixedMermaid    = null;

        // ── 1단계: Fast-Track ────────────────────────────────────────────
        let cachedSvg = null; // 검증에 사용한 SVG 재사용 (이중 fetch 방지)
        try {
            const cleaned = sanitizeMermaid(originalMermaid);
            const res     = await fetch(buildKrokiUrl(cleaned));
            const svg     = await res.text();
            if (isValidKrokiSvg(res, svg)) {
                console.log(`  -> ⚡ [Fast-Track 성공]`);
                fixedMermaid = cleaned;
                cachedSvg    = svg;
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
            if (mode === 'html') {
                // HTML: Kroki URL 방식 — 외부 CDN 참조, 파일 크기 절감
                mdText += `\n\n<div class="diagram-wrap">` +
                          `<img src="${buildKrokiUrl(fixedMermaid)}" alt="시스템 다이어그램" />` +
                          `</div>\n\n`;
            } else {
                // PDF(기본): Base64 인라인 SVG — 외부 URL fetch 불필요, 오프라인 렌더링
                // cachedSvg: Fast-Track 검증 시 이미 받은 SVG → 재사용해 Kroki 요청 절약
                try {
                    const svgStr = cachedSvg ?? await fetch(buildKrokiUrl(fixedMermaid)).then(r => r.text());
                    const b64    = Buffer.from(svgStr).toString('base64');
                    mdText += `\n\n<div class="diagram-wrap">` +
                              `<img src="data:image/svg+xml;base64,${b64}" alt="시스템 다이어그램" />` +
                              `</div>\n\n`;
                } catch {
                    // Base64 fetch 실패 시 URL 폴백
                    mdText += `\n\n<div class="diagram-wrap">` +
                              `<img src="${buildKrokiUrl(fixedMermaid)}" alt="시스템 다이어그램" />` +
                              `</div>\n\n`;
                }
            }
        } else {
            brokenCount++;
            console.log(`  -> 🚨 [다이어그램 복구 실패] 플레이스홀더로 대체. (누적 ${brokenCount}개)`);
            mdText += `\n\n> ⚠️ **[다이어그램 렌더링 실패]** Mermaid 파싱 오류로 인해 이 다이어그램을 표시할 수 없습니다.\n\n`;
        }

        lastIndex = match.index + match[0].length;
    }

    mdText += reportText.substring(lastIndex);

    return { mdText, brokenCount };
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
    <title>${gameTitle} 분석 문서</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@400;500;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

        :root {
            --primary:    #4F46E5;
            --primary-lt: #EEF2FF;
            --primary-dk: #3730A3;
            --accent:     #06B6D4;
            --bg:         #F8FAFC;
            --card:       #FFFFFF;
            --text:       #1E293B;
            --text-muted: #64748B;
            --border:     #E2E8F0;
            --radius:     12px;
            --shadow:     0 1px 3px rgba(0,0,0,.08), 0 8px 24px rgba(0,0,0,.06);
        }

        body {
            font-family: 'Noto Sans KR', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg);
            color: var(--text);
            line-height: 1.8;
            padding: 32px 16px 64px;
            font-size: 15px;
            word-break: keep-all;
            overflow-wrap: break-word;
        }

        /* ── 레이아웃 ─────────────────────────────────────────────────── */
        .report-container {
            max-width: 860px;
            margin: 0 auto;
            background: var(--card);
            border-radius: 20px;
            box-shadow: var(--shadow);
            overflow: hidden;
        }

        .report-header {
            background: linear-gradient(135deg, var(--primary) 0%, var(--primary-dk) 100%);
            padding: 40px 48px 36px;
            color: #fff;
        }
        .report-header h1 {
            font-size: 1.8em;
            font-weight: 800;
            color: #fff;
            border: none;
            margin: 0 0 16px;
            line-height: 1.3;
            letter-spacing: -0.02em;
        }
        .report-header blockquote {
            background: rgba(255,255,255,.15);
            border-left: 3px solid rgba(255,255,255,.6);
            color: rgba(255,255,255,.9);
            font-size: 0.875em;
            padding: 10px 16px;
            margin: 6px 0;
            border-radius: 0 8px 8px 0;
        }
        /* 헤더 구역 이후 본문 */
        .report-body { padding: 40px 48px 48px; }

        /* ── 제목 ─────────────────────────────────────────────────────── */
        h1 {
            font-size: 1.6em; font-weight: 800; color: #0F172A;
            border-bottom: 3px solid var(--primary);
            padding-bottom: 10px; margin: 36px 0 16px;
            letter-spacing: -0.02em;
        }
        h2 {
            font-size: 1.2em; font-weight: 700; color: var(--primary);
            border-bottom: 1px solid var(--border);
            padding-bottom: 6px; margin: 28px 0 12px;
        }
        h3 {
            font-size: 1.05em; font-weight: 700; color: #334155;
            margin: 20px 0 8px;
        }
        h4 { font-size: 0.95em; font-weight: 600; color: #475569; margin: 14px 0 6px; }

        /* ── 본문 요소 ────────────────────────────────────────────────── */
        p  { margin: 10px 0; }
        hr { border: 0; border-top: 1px solid var(--border); margin: 28px 0; }

        ul, ol { padding-left: 24px; margin: 10px 0; }
        li { margin: 4px 0; }
        li > ul, li > ol { margin: 4px 0; }

        blockquote {
            background: var(--primary-lt);
            border-left: 4px solid var(--primary);
            padding: 14px 18px;
            margin: 18px 0;
            border-radius: 0 var(--radius) var(--radius) 0;
            color: var(--primary-dk);
            font-weight: 500;
            font-size: 0.93em;
        }

        strong { color: #0F172A; }
        em     { color: var(--text-muted); font-style: italic; }

        /* ── 코드 ─────────────────────────────────────────────────────── */
        code {
            font-family: 'JetBrains Mono', 'Courier New', monospace;
            font-size: 0.84em;
            background: #FDF4FF;
            color: #BE185D;
            padding: 2px 6px;
            border-radius: 5px;
            border: 1px solid #FBCFE8;
            word-break: break-all;
        }
        pre {
            background: #0F172A;
            color: #E2E8F0;
            padding: 20px;
            border-radius: var(--radius);
            overflow-x: auto;
            margin: 16px 0;
            font-size: 0.82em;
            line-height: 1.6;
            -webkit-overflow-scrolling: touch;
        }
        pre code {
            background: transparent;
            color: inherit;
            padding: 0;
            border: none;
            font-size: 1em;
            word-break: normal;
        }

        /* ── 표 ───────────────────────────────────────────────────────── */
        .table-wrap {
            overflow-x: auto;
            margin: 20px 0;
            border-radius: var(--radius);
            border: 1px solid var(--border);
            box-shadow: 0 2px 8px rgba(0,0,0,.04);
            -webkit-overflow-scrolling: touch;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.875em;
            min-width: 400px;
        }
        th {
            background: #F8FAFC;
            font-weight: 700;
            color: #0F172A;
            padding: 12px 16px;
            text-align: left;
            border-bottom: 2px solid var(--border);
            white-space: nowrap;
        }
        td {
            padding: 11px 16px;
            border-bottom: 1px solid var(--border);
            vertical-align: top;
            word-break: break-word;
            overflow-wrap: anywhere;
        }
        tr:last-child td { border-bottom: none; }
        tbody tr:hover   { background: #F8FAFC; }

        /* ── 다이어그램 ───────────────────────────────────────────────── */
        .diagram-wrap {
            text-align: center;
            margin: 24px 0;
            padding: 20px;
            background: #FAFAFA;
            border: 1px solid var(--border);
            border-radius: var(--radius);
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
        }
        .diagram-wrap img {
            max-width: 100%;
            height: auto;
            display: inline-block;
        }

        /* ── 이미지 ───────────────────────────────────────────────────── */
        img:not(.diagram-wrap img) {
            max-width: 100%;
            height: auto;
            display: block;
            margin: 20px auto;
            border-radius: var(--radius);
        }

        /* ── 모바일 ───────────────────────────────────────────────────── */
        @media (max-width: 640px) {
            body { padding: 0 0 48px; font-size: 14px; }
            .report-header  { padding: 28px 20px 24px; }
            .report-body    { padding: 24px 20px 32px; }
            .report-header h1 { font-size: 1.4em; }
            h1 { font-size: 1.3em; }
            h2 { font-size: 1.1em; }
            pre { padding: 14px; font-size: 0.78em; }
            th, td { padding: 9px 12px; }
        }

        @media (max-width: 400px) {
            .report-header h1 { font-size: 1.2em; }
            body { font-size: 13px; }
        }
    </style>
</head>
<body>
    <div class="report-container">
        <div id="report-content">
            ${bodyHtml}
        </div>
    </div>
    <script>
        // 첫 번째 h1 + 그 뒤 blockquote들을 헤더 영역으로 분리
        (function () {
            const container = document.getElementById('report-content');
            const firstH1   = container.querySelector('h1');
            if (!firstH1) return;

            const header = document.createElement('div');
            header.className = 'report-header';
            const body   = document.createElement('div');
            body.className   = 'report-body';

            // 노드를 직접 이동 (중복 DOM 방지, 메모리 효율)
            header.appendChild(firstH1);
            let node = container.firstElementChild;
            while (node) {
                const next = node.nextElementSibling;
                if (node.tagName === 'BLOCKQUOTE') {
                    header.appendChild(node);
                } else if (node.tagName !== 'HR') {
                    body.appendChild(node);
                } else {
                    node.remove(); // HR은 헤더 구분선 역할이 끝났으므로 제거
                }
                node = next;
            }

            // 표를 .table-wrap으로 감싸기
            body.querySelectorAll('table').forEach(tbl => {
                const wrap = document.createElement('div');
                wrap.className = 'table-wrap';
                tbl.parentNode.insertBefore(wrap, tbl);
                wrap.appendChild(tbl);
            });

            container.appendChild(header);
            container.appendChild(body);
        })();
    </script>
</body>
</html>`;
}


// =============================================================================
//  🔭  buildScoutPrompt — 공식 가이드 기준 시스템명 수집 (최대 3회, 실패 시 ABORT)
//  1회: 공식 가이드·공홈·카페  2회: 전문 분석 글(인벤·Game8 등)  3회: 나무위키·커뮤니티
// =============================================================================

// 영문 표기 한국 개발사 화이트리스트 (한글 감지 불가 케이스 보완)
const KR_DEVELOPERS = new Set([
    'Netmarble', 'NCSoft', 'Nexon', 'Krafton', 'Com2uS', 'Smilegate',
    'Kakao Games', 'Pearl Abyss', 'Gravity', 'WeMade', 'Shift Up',
    'HYBE IM', 'Devsisters', 'Joycity', 'Gamevil', 'Neowiz',
    'Line Games', 'Ngelgames', '4:33 Creative Lab', 'Sundaytoz',
]);

function buildScoutPrompt(game, attempt = 1) {
    const storeUrl  = `https://play.google.com/store/apps/details?id=${game.appId}`;
    const isKorean  = /[가-힣]/.test(game.developer) ||
                      KR_DEVELOPERS.has(game.developer.trim());

    const strategies = {
        1: (() => {
            return isKorean ? {
                label: '공식 가이드·공홈·카페 — 국내 (1순위 출처)',
                queries: [
                    `${game.title} 공식 홈페이지`,
                    `${game.title} 공식 가이드 시스템 소개`,
                    `${game.title} ${game.developer} 공식 홈페이지 게임 소개 시스템`,
                    `${game.title} 네이버 공식카페 공략 시스템 재화`,
                ],
                instruction: `
## 이번 회차 핵심 지시
개발사(${game.developer})가 공식적으로 배포한 공식 홈페이지, 가이드, 네이버 공식 카페에서만 명칭을 수집하십시오.
공식 홈페이지를 최우선으로 확인하고, 시스템명·재화명이 UI 또는 공지에 표기된 그대로 수집하십시오.
공식 출처에서 확인된 명칭만 [시스템명]·[재화명]에 기재하십시오.
공식 출처 URL이 없으면 이번 회차는 실패로 처리됩니다.`,
            } : {
                label: '공식 가이드·공홈·커뮤니티 — 글로벌 (1순위 출처)',
                queries: [
                    `${game.title} official website`,
                    `${game.title} official guide system introduction`,
                    `${game.title} ${game.developer} official site game system`,
                    `${game.title} official discord OR forum system guide`,
                ],
                instruction: `
## 이번 회차 핵심 지시
개발사(${game.developer})의 공식 홈페이지를 최우선으로 확인하고, 공식 가이드, 공식 Discord·포럼에서 시스템명·재화명을 수집하십시오.
공식 홈페이지에서 UI 또는 공지에 표기된 명칭을 그대로 수집하십시오.
공식 출처에서 확인된 명칭만 [시스템명]·[재화명]에 기재하십시오.
공식 출처 URL이 없으면 이번 회차는 실패로 처리됩니다.`,
            };
        })(),
        2: (() => {
            return isKorean ? {
                label: '전문 분석 글 — 국내 (2순위 출처)',
                queries: [
                    `${game.title} 인벤 공략 시스템 분석`,
                    `${game.title} 치지직 게임 라운지 시스템 재화`,
                    `${game.title} 유튜브 심층 리뷰 시스템 재화`,
                    `${game.title} AppGamer OR "Pocket Gamer" guide system`,
                ],
                instruction: `
## 이번 회차 핵심 지시
인벤(inven.co.kr), 치지직(chzzk.naver.com) 게임 라운지, 유튜브 심층 공략·리뷰에서 ${game.title}의 시스템명·재화명을 수집하십시오.
글로벌 서비스 게임의 경우 AppGamer·Pocket Gamer에도 영문 분석 글이 있을 수 있으므로 함께 확인하십시오.
치지직은 실제 플레이 영상·클립에서 UI에 표시되는 명칭을 직접 확인하십시오.
누구나 편집할 수 있는 위키류(나무위키 등)는 이번 회차에서 제외합니다.
전문 분석 글 또는 영상 URL이 없으면 이번 회차는 실패로 처리됩니다.`,
            } : {
                label: '전문 분석 글 — 글로벌 (2순위 출처)',
                queries: [
                    `${game.title} Game8 guide system`,
                    `${game.title} NGA 攻略 system`,
                    `${game.title} reddit guide system mechanics`,
                    `${game.title} AppGamer OR "Pocket Gamer" guide system`,
                    `${game.title} site:store.steampowered.com/app OR site:steamcommunity.com guide`,
                    `${game.title} twitch clips system guide`,
                    `${game.title} youtube in-depth review system`,
                ],
                instruction: `
## 이번 회차 핵심 지시
Game8, NGA, Reddit, AppGamer, Pocket Gamer, Steam 커뮤니티 허브(steamcommunity.com), Twitch 클립, 유튜브 심층 공략·리뷰에서 ${game.title}의 시스템명·재화명을 수집하십시오.
AppGamer·Pocket Gamer는 모바일 게임 전문 분석 사이트이므로 Steam이 없는 모바일 전용 게임에서 우선 확인하십시오.
Steam 커뮤니티 허브는 공식에 준하는 플레이어 가이드가 있으므로 PC·콘솔 게임에서 우선 확인하십시오.
Twitch·유튜브는 실제 플레이 클립·리뷰에서 UI에 표시되는 명칭을 직접 확인하십시오.
누구나 편집할 수 있는 위키류는 이번 회차에서 제외합니다.
전문 분석 글 또는 영상 URL이 없으면 이번 회차는 실패로 처리됩니다.`,
            };
        })(),
        3: (() => {
            return isKorean ? {
                label: '위키·커뮤니티 교차 검증 — 국내 (3순위 출처)',
                queries: [
                    `나무위키 ${game.title} 콘텐츠 시스템`,
                    `${game.title} 아카라이브 시스템 재화 공략`,
                ],
                instruction: `
## 이번 회차 핵심 지시
나무위키(namu.wiki) 또는 아카라이브(arca.live)에서 ${game.title} 문서를 찾아 시스템명·재화명을 수집하십시오.
이 출처는 누구나 편집 가능하므로 신뢰도 낮음으로 처리됩니다.
복수 출처에서 동일한 명칭이 확인될수록 신뢰도가 높아집니다.`,
            } : {
                label: '위키·커뮤니티 교차 검증 — 글로벌 (3순위 출처)',
                queries: [
                    `${game.title} fandom wiki system`,
                    `${game.title} gamepedia wiki system`,
                    `${game.title} wiki.gg guide system`,
                ],
                instruction: `
## 이번 회차 핵심 지시
Fandom(fandom.com), Gamepedia, Wiki.gg에서 ${game.title} 위키 문서를 찾아 시스템명·재화명을 수집하십시오.
이 출처는 누구나 편집 가능하므로 신뢰도 낮음으로 처리됩니다.
복수 위키에서 동일한 명칭이 확인될수록 신뢰도가 높아집니다.`,
            };
        })(),
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

## 출처 신뢰도 기준
- 높음: 공식 가이드·공홈·카페에서 직접 확인
- 보통: 인벤·Game8·NGA·유튜브 심층 공략 등 전문 분석 글에서 확인
- 낮음: 나무위키·아카라이브 등 누구나 편집 가능한 출처에서만 확인

## 중단 조건
- ${game.title}이 아닌 다른 게임 명칭이 섞이면: [IP_CONFUSED]
- 지정 검색어 실행 후 관련 데이터가 전혀 없으면: [ABORT_NO_DATA]
`;
}


// =============================================================================
//  📝  buildAnalysisPrompt — 분석 문서 생성 프롬프트
// =============================================================================

function buildAnalysisPrompt(game, rank, category, factSheet = '') {
    const storeUrl = `https://play.google.com/store/apps/details?id=${game.appId}`;

    return `
# ⚠️ [최우선] 타겟 게임 고정 — 위반 시 전체 출력 무효
* **게임명:** ${game.title}
* **개발사:** ${game.developer}
* **앱 ID:**  ${game.appId}
* **URL:**    ${storeUrl}
* **매출 순위:** ${rank}위
* **분석 영역:** ${category}

### 혼동 방지 체크리스트
- [ ] 모든 검색에 "${game.title}" + 앱ID "${game.appId}" 를 함께 사용했는가?
- [ ] 같은 IP의 다른 게임(예: "메이플스토리M" ≠ "메이플 키우기") 데이터를 혼용하지 않았는가?
- [ ] 확인 불가 데이터는 추측 없이 **"데이터 비공개 (검색 불가)"** 로 표기했는가?

---

# Step 0: 메타데이터 (절대 수정 금지)
메인장르: (RPG / MMORPG / 방치형 / SLG/전략 / 캐주얼/퍼즐 / 액션/슈팅 / SNG/시뮬레이션 / 스포츠/레이싱 / 카지노/보드 / 기타 중 하나)
서브장르: (15자 이내)
시스템:   (15자 이내 명사형, 파일명에 사용)

# [고정 어휘 사전] — 이 목록 외 명칭 임의 생성 금지
${factSheet ? `${factSheet}

## 준수 규칙
1. [시스템명] 목록의 이름 → 반드시 그대로 사용 (동의어·축약·번역 금지)
2. [재화명] 목록의 이름   → 반드시 그대로 사용
3. 목록에 없는 명칭       → "데이터 비공개 (검색 불가)"로 표기
4. [출처신뢰도]가 낮음    → 해당 명칭 사용 시 *(출처 미검증)* 주석 추가
` : '⚠️ 팩트 사전 없음 — 딥 서치 직접 확인. 확인 불가 명칭은 데이터 비공개 표기.'}

# Step 1: 분석 시스템 특정
1. [${category}] 영역의 시그니처 시스템 1개를 [고정 어휘 사전]의 [시스템명]에서 선택하십시오.
2. 유저가 게임 내에서 직접 클릭하는 **정확한 UI 텍스트(메뉴명)** 기준으로 분석하십시오.

# Step 2: 분석 문서 작성 (8개 항목)

## 작성 원칙
- **수집 항목 (01·02·03·04·06)**: 검색으로 확인된 사실만 기술. 확인 불가는 "데이터 비공개 (검색 불가)" 표기. 항목 말미에 \`> 출처: URL\` 필수.
- **분석 항목 (05·07)**: 위 수집 항목에서 확인된 팩트에 한해서만 추론. 팩트 없는 항목은 분석하지 말 것. 단일 출처 기반 추론은 *(단일 출처)* 주석.

---

01. **시스템 개요**
    검색으로 확인된 시스템 정의·구성 요소 요약. 공식 설명 우선.

02. **시스템 구조도**
    확인된 서브시스템 간 연결 관계 (★ Mermaid \`graph LR\` 강제)

03. **이용 플로우차트**
    확인된 유저 핵심 이용 흐름 (★ Mermaid \`flowchart TD\` 강제)

04. **UX 명세**
    검색·영상·스크린샷으로 확인된 인터랙션·상태 전이 명세. 확인 불가 항목은 데이터 비공개 표기.

05. **기획 의도 분석** ← 분석 항목
    위 01~04에서 수집된 팩트를 근거로 아래 두 가지를 분석하십시오.
    - **설계 의도**: 이 시스템이 왜 이렇게 설계됐는가. 수익화·리텐션·트래픽 유도 관점에서 의도를 추론.
    - **심리 설계**: 수집된 팩트(수치·흐름·구조)에서 역으로 읽히는 심리 트리거(FOMO·손실 회피·보상 스케줄 등)를 구체적 근거와 함께 기술. 근거 없는 트리거 명칭 나열 금지.

06. **수치 및 데이터 테이블**
    공식·커뮤니티에서 확인된 재화 Source/Sink·수치 밸런스 (★ 표 형식 강제. 미확인 수치는 셀에 "비공개" 표기)

07. **문제점 및 개선 제안** ← 분석 항목
    위 수집된 팩트 기반으로 이 시스템의 구조적 문제점을 짚고, 구체적인 개선 방향을 제안하십시오. (★ 표 형식 강제)
    - **문제점**: 유저 경험·수익 구조·밸런스 관점에서 실제로 확인되는 문제. 추측 기반 문제 제기 금지.
    - **개선 제안**: 동일 장르 타 게임의 검색 가능한 사례를 근거로 구체적 대안 제시. 근거 없는 제안 금지.

08. **참고 문헌**
    위 항목들의 근거 URL 목록 (★ 필수. 반드시 ${game.title} 관련 URL만. 최소 2개)

---

# ★ 딥 서치 철칙
1. 모든 검색은 "${game.title}" + 앱ID "${game.appId}" 기준. 다른 게임이 검색되면 즉시 키워드 변경.
2. 출처 우선순위를 반드시 준수하십시오:
   - **1순위**: 공식 사이트·공식 카페·개발사 공지 (가장 신뢰)
   - **2순위**: 인벤 공략·Game8·NGA·유튜브 심층 리뷰 등 전문 분석 글
   - **3순위**: 나무위키·아카라이브 등 누구나 편집 가능한 출처 (최후 보완용)
   - 출처 표기 순서도 이 위계를 따를 것. 나무위키가 있더라도 공식·전문 출처가 있으면 나중에 표기.
3. 1·2순위 검색 부족 시에만 3순위로 보완. 3순위만 남은 항목은 *(나무위키 단독)* 주석 추가.
4. 복수 출처 교차 검증. 단일 출처만 있으면 해당 데이터에 *(단일 출처)* 주석.

# Output Constraints
* [Mermaid 규칙]  화살표 텍스트(\`-->|텍스트|\`)는 10자 이내. 대괄호/중괄호 안에 콜론·따옴표·쉼표 절대 금지.
* [노드 ID 규칙]  노드 ID는 반드시 띄어쓰기 없는 영문+숫자 조합(예: A1, NodeB2). 한글 노드 ID 절대 금지.
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

        const stats = {
            full: 0, partial: 0, skipped: 0, diagram: 0,
            scoutOk: 0, scoutFallback: 0, scoutCross: 0, scoutAbort: 0,
        };

        // ── 4. 게임별 분석 문서 생성 루프 ────────────────────────────────────
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

            const category = pickCategory();
            console.log(`\n${progress} 매출 ${rank}위: ${game.title}`);
            console.log(`  -> 🎯 분석 영역: [${category}] / 출시일: ${releaseDate}`);

            // 4-3. Scout — 공식 가이드 기준 시스템명 수집 (3회 고정)
            const MAX_SCOUT_RETRIES = 3;

            let factSheet    = '';
            let scoutAborted = false;
            let scoutFormatFallback = false; // 모든 회차가 형식 실패 → Writer 자체 딥서치로 진행

            for (let sAttempt = 1; sAttempt <= MAX_SCOUT_RETRIES; sAttempt++) {
                try {
                    await delay(3000);
                    const scoutPrompt = buildScoutPrompt(game, sAttempt);
                    // 실제 전략 라벨을 프롬프트에서 추출 (첫 줄 ## 검색 전략: 이후)
                    const labelMatch  = scoutPrompt.match(/## 검색 전략: (.+)/);
                    const scoutLabel  = labelMatch ? labelMatch[1] : `${sAttempt}회차`;
                    console.log(`  -> 🔭 [SCOUT ${sAttempt}/${MAX_SCOUT_RETRIES}] ${scoutLabel} 탐색...`);
                    const scoutText = await callGeminiWithRetry(scoutModel, scoutPrompt, 2);

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

                    // ── Q1: 형식 검증 ─────────────────────────────────────────────
                    // [시스템명] 태그 없으면 Writer가 팩트 사전으로 쓸 수 없음.
                    // 다음 회차가 남아 있으면 계속 시도. 마지막 회차이면 폴백 플래그 세우고 루프 종료.
                    if (!scoutText.includes('[시스템명]')) {
                        if (sAttempt < MAX_SCOUT_RETRIES) {
                            console.log(`  -> ⚠️  [SCOUT-FORMAT ${sAttempt}회] 형식 불일치 ([시스템명] 누락). 다음 회차로.`);
                            continue;
                        } else {
                            console.log(`  -> ⚠️  [SCOUT-FORMAT ${sAttempt}회] 형식 불일치. 전 회차 실패 → Writer 자체 딥서치로 진행.`);
                            scoutFormatFallback = true;
                            break;
                        }
                    }

                    // ── Q2: Scout-Writer 사이 appId 교차 확인 ────────────────────
                    // factSheet 안에 타겟 게임 title 또는 appId가 있어야 정상.
                    // 없다는 건 완전히 다른 게임 정보가 섞인 것 → 다음 회차로.
                    const titleInSheet = scoutText.includes(game.title);
                    const appIdInSheet = scoutText.includes(game.appId);
                    if (!titleInSheet && !appIdInSheet) {
                        console.log(`  -> ⚠️  [SCOUT-CROSS ${sAttempt}회] factSheet에 타겟 게임 식별자 없음 (IP 오염 의심). 다음 회차로.`);
                        stats.scoutCross++;
                        continue;
                    }

                    factSheet = scoutText;
                    console.log(`  -> ✅ [SCOUT-OK] 완료 (신뢰도: ${trust}, ${sAttempt}회차)`);
                    break;

                } catch (scoutErr) {
                    console.log(`  -> ⚠️  [SCOUT-ERR ${sAttempt}회] ${scoutErr.message?.substring(0, 60)}`);
                }
            }

            // ── Scout 결과 판단 ────────────────────────────────────────────────
            if (scoutAborted) {
                const errMsg = `[${rank}위] ${game.title} — Scout ABORT_NO_DATA`;
                console.error(`  -> ❌ [SCOUT-ABORT] ${errMsg}`);
                errorLog.push(errMsg);
                stats.scoutAbort++;
                stats.skipped++;
                continue;
            }
            if (!factSheet) {
                const reason = scoutFormatFallback ? '형식 불일치 전 회차 실패' : `${MAX_SCOUT_RETRIES}회 URL/신뢰도/교차확인 미달`;
                console.log(`  -> ℹ️  [SCOUT-FALLBACK] ${reason} → Writer 자체 딥서치로 진행`);
                stats.scoutFallback++;
            } else {
                stats.scoutOk++;
            }

            // 4-4. 분석 문서 초안 생성
            const reportRaw = await callGeminiWithRetry(draftModel, buildAnalysisPrompt(game, rank, category, factSheet), MAX_DRAFT_RETRIES);

            if (!reportRaw) {
                const errMsg = `[${rank}위] ${game.title} — Draft 생성 ${MAX_DRAFT_RETRIES}회 실패`;
                console.error(`  -> ❌ ${errMsg}`);
                errorLog.push(errMsg);
                stats.skipped++;
                continue;
            }

            // Writer가 직접 판단한 중단 신호 체크
            if (reportRaw.includes('[ABORT_NO_DATA]')) {
                const errMsg = `[${rank}위] ${game.title} — Writer ABORT_NO_DATA`;
                console.log(`  -> ⏭️  ${errMsg}`);
                errorLog.push(errMsg);
                stats.skipped++;
                continue;
            }
            if (reportRaw.includes('[IP_CONFUSED]')) {
                // factSheet 주입 상태였다면 제거 후 순수 딥서치로 1회 재시도
                if (factSheet) {
                    console.log(`  -> ⚠️  [WRITER-IP] IP_CONFUSED — factSheet 제거 후 딥서치 재시도...`);
                    const retryRaw = await callGeminiWithRetry(draftModel, buildAnalysisPrompt(game, rank, category, ''), MAX_DRAFT_RETRIES);
                    if (retryRaw && !retryRaw.includes('[IP_CONFUSED]') && !retryRaw.includes('[ABORT_NO_DATA]')) {
                        console.log(`  -> ✅ [WRITER-IP] 딥서치 재시도 성공`);
                        var finalReportRaw = retryRaw;
                    } else {
                        const errMsg = `[${rank}위] ${game.title} — Writer IP_CONFUSED (재시도 후에도 실패)`;
                        console.log(`  -> ⏭️  ${errMsg}`);
                        errorLog.push(errMsg);
                        stats.skipped++;
                        continue;
                    }
                } else {
                    const errMsg = `[${rank}위] ${game.title} — Writer IP_CONFUSED`;
                    console.log(`  -> ⏭️  ${errMsg}`);
                    errorLog.push(errMsg);
                    stats.skipped++;
                    continue;
                }
            }

            // 4-5. 리포트 텍스트 정제
            const effectiveReport = (typeof finalReportRaw !== 'undefined') ? finalReportRaw : reportRaw;
            let reportText = effectiveReport
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

            // 카테고리 축약 코드 (파일명 + frontmatter용)
            const CATEGORY_CODE = {
                '핵심 BM (가챠/강화/패스 등 직접적 매출원)':                   'BM',
                '장기 리텐션 (일일 숙제/업적/마일리지 등 접속 유지 장치)':     'RET',
                '소셜 및 경쟁 (길드/PvP/랭킹 등 유저 간 상호작용)':           'SOC',
                '성장 및 경제 (재화 획득/소모처 및 인플레이션 제어 로직)':     'ECO',
                '코어 게임플레이 (전투 공식/스테이지 기믹/퍼즐 등 조작의 재미)': 'CORE',
                '캐릭터 및 전투 클래스 (스킬 메커니즘/시너지/상성 구조)':     'CHAR',
                '수치 및 전투 밸런스 (데미지 공식/스테이터스/성장 체감)':     'BAL',
                '레벨 디자인 (맵 구조/동선/오브젝트 배치/몬스터 스폰)':       'LVL',
                '세계관 및 시나리오 (퀘스트 라인/내러티브/NPC 상호작용)':     'STORY',
                '핵심 콘텐츠 시스템 (레이드/던전/생활형 콘텐츠 등 주요 시스템)': 'CONT',
                'UI/UX 및 편의성 설계 (HUD 배치/메뉴 뎁스/단축키/조작감)':   'UX',
                '라이브 옵스 및 이벤트 기획 (시즌 이벤트/픽업 로테이션/출석부)': 'LIVE',
                '메타 게임 및 서브 콘텐츠 (도감 수집/하우징/미니게임/꾸미기)': 'META',
                '온보딩 및 튜토리얼 (초반 동선/가이드 로직/이탈 방지 장치)':  'ONBD',
            };
            const categoryCode = CATEGORY_CODE[category] || 'ETC';

            // 메타데이터 줄 제거 (파일명용 시스템명은 이미 추출 완료)
            reportText = reportText
                .replace(/메인장르:.*?\n/g, '')
                .replace(/서브장르:.*?\n/g, '')
                .replace(/시스템:.*?\n/g,   '')
                .trim();

            // ── MD 전용: YAML frontmatter + 순수 텍스트 본문 ────────────────
            // LLM 학습용. SVG/Base64 없음. Mermaid 코드블록 원본 유지.
            const mdLlmText = [
                '---',
                `title: "${game.title} 분석 문서"`,
                `date: "${dateString}"`,
                `rank: ${rank}`,
                `app_id: "${game.appId}"`,
                `developer: "${game.developer}"`,
                `release_date: "${releaseDate}"`,
                `category: "${category}"`,
                `category_code: "${categoryCode}"`,
                `core_system: "${coreSystemName}"`,
                '---',
                '',
                reportText,
            ].join('\n');

            // ── PDF/HTML 전용: blockquote 헤더 + Mermaid → SVG 치환 ─────────
            // 사람이 읽는 시각화 문서용.
            const reportTextForVisual = [
                `# [${rank}위] ${game.title} 분석 문서`,
                `> **분석 카테고리:** ${category}`,
                `> **핵심 시스템:** ${coreSystemName}`,
                `> **개발사:** ${game.developer}`,
                `> **매출 순위:** ${rank}위 (${dateString} 기준)`,
                `> **출시일:** ${releaseDate}`,
                '',
                '---',
                '',
                reportText,
            ].join('\n');

            // 4-6. Mermaid 블록 처리 — PDF/HTML용 소스에만 적용 (mode별 분리)
            // mdLlmText는 Mermaid 코드블록 원본 유지 (LLM 학습 노이즈 방지)
            // PDF: Base64 인라인 SVG (오프라인 렌더링, GitHub Actions sandbox 대응)
            // HTML: Kroki URL (경량화, 외부 공유 최적화)
            const { mdText: pdfMdText, brokenCount }  = await processMermaidBlocks(reportTextForVisual, qaModel, 'pdf');
            const { mdText: htmlMdText }               = await processMermaidBlocks(reportTextForVisual, qaModel, 'html');
            if (brokenCount > 0) stats.diagram++;

            // 4-7. 파일명 생성 (카테고리 코드 포함)
            const safeTitle    = game.title.replace(/[/\\?%*:|"<>]/g, '_');
            const baseFileName = `[${dateString}]_${String(rank).padStart(3, '0')}위_${safeTitle}_(${coreSystemName})_[${categoryCode}]`;

            // 4-8. MD / PDF / HTML 저장
            // MD: mdLlmText (순수 텍스트 + YAML frontmatter, LLM 학습용)
            // PDF/HTML: visualMdText (SVG 치환 완료, 사람이 읽는 시각화 문서)
            const uploads = [
                { tag: 'MD',   ext: '.md',   folderId: mdFolderId,   mimeType: 'text/markdown',   content: mdLlmText,    validate: () => mdLlmText.length >= 10 },
                { tag: 'PDF',  ext: '.pdf',  folderId: pdfFolderId,  mimeType: 'application/pdf', content: null,          validate: null },
                { tag: 'HTML', ext: '.html', folderId: htmlFolderId, mimeType: 'text/html',        content: null,          validate: null },
            ];

            let savedCount = 0;

            for (const u of uploads) {
                try {
                    let content = u.content;
                    if (u.tag === 'PDF') {
                        console.log(`  -> 📄 [PDF]  변환 시작...`);
                        content = await convertToPdf(pdfMdText, PDF_OPTIONS);
                    } else if (u.tag === 'HTML') {
                        console.log(`  -> 🌐 [HTML] 변환 시작...`);
                        const parsedBody = marked.parse(htmlMdText);
                        if (!parsedBody?.trim()) throw new Error('HTML 파싱 결과가 비어있습니다.');
                        content = buildHtmlReport(game.title, parsedBody);
                    } else if (u.validate && !u.validate()) {
                        throw new Error('MD 데이터가 비어있습니다.');
                    }

                    const saved = await uploadToDrive({ fileName: `${baseFileName}${u.ext}`, folderId: u.folderId, mimeType: u.mimeType, content });
                    if (saved) {
                        console.log(`  -> 💾 [${u.tag.padEnd(4)}] 저장 완료`);
                    } else {
                        console.log(`  -> ⏭️  [${u.tag.padEnd(4)}] 이미 존재 (SKIP)`);
                    }
                    savedCount++; // 신규 저장·SKIP 모두 성공으로 집계 (멱등성 보장)
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
        console.log(`──────────────────────────────────────────────────────`);
        console.log(`  Scout 성공 (factSheet)   ${stats.scoutOk}개`);
        console.log(`  Scout FALLBACK (딥서치)  ${stats.scoutFallback}개`);
        console.log(`  Scout CROSS (IP 오염)    ${stats.scoutCross}개`);
        console.log(`  Scout ABORT (데이터 없음)${stats.scoutAbort}개`);
        console.log(`${'='.repeat(56)}`);
        console.log(`🎉 Google Drive 동기화 완료`);
        console.log(`${'='.repeat(56)}\n`);

        // ── 6. Drive 실패 로그 파일 저장 ─────────────────────────────────────
        if (errorLog.length > 0) {
            try {
                const logFileName  = `[${dateString}]_ERROR_LOG_rank${process.env.START_RANK || 1}-${process.env.END_RANK || 50}.txt`;
                const logContent   = [
                    `[${dateString}] 실패 로그 (rank ${process.env.START_RANK || 1}~${process.env.END_RANK || 50})`,
                    `총 ${errorLog.length}건`,
                    '='.repeat(56),
                    ...errorLog,
                ].join('\n');
                const logDriveId = await uploadToDrive({
                    fileName: logFileName,
                    folderId: ROOT_FOLDER_ID,
                    mimeType: 'text/plain',
                    content:  Buffer.from(logContent, 'utf8'),
                });
                console.log(`📋 실패 로그 Drive 저장 완료: ${logFileName} (${logDriveId})`);
            } catch (logErr) {
                console.error(`⚠️  실패 로그 Drive 저장 실패: ${logErr.message}`);
                // 로컬 출력으로라도 남김
                console.error('=== ERROR LOG ===');
                errorLog.forEach(e => console.error(e));
            }
        }

    } catch (fatalError) {
        console.error('💀 치명적 에러 발생:', fatalError);
        process.exit(1);
    }
}

main();
