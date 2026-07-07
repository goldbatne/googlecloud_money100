'use strict';

// =============================================================================
//
//  📦 Google Play 게임 게임 분석 문서 자동 생성 파이프라인
//
//  흐름 요약:
//    1. Google Play 매출 순위 스크래핑
//    2. 공식 사이트 직접 크롤링 (0회차) — 스토어·개발사 URL fetch → factSheet 주입
//    3. Scout — 공식 가이드 기준 시스템명·재화명 수집 (최대 4회, YouTube 포함)
//    4. Writer Phase1 — 구조·다이어그램 집중 (01~04섹션)
//    5. DIAG — 다이어그램 전담 재생성 (Phase1 텍스트 기반, 검색 없음)
//    6. Writer Phase2 — 수치·분석·비교 집중 (05~09섹션)
//    7. Mermaid 다이어그램 QA (Fast-Track → QA Agent → 폴백 자동생성)
//    8. MD / PDF / HTML 3포맷 변환 후 Google Drive 날짜별 폴더에 저장
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
const MAX_QA_RETRIES    = 3; // Mermaid 다이어그램 QA Agent 재시도 (내부 callGeminiWithRetry 3회와 구분)

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
 * HTML 특수문자 이스케이프 — <title> 등 HTML 삽입 지점용
 * 게임명에 <, >, ", & 가 포함돼도 문서 구조가 깨지지 않도록 방어
 */
const escapeHtml = s => String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');

/**
 * YAML frontmatter 문자열 값 안전 처리
 * 큰따옴표·역슬래시 이스케이프 + 개행을 공백으로 치환 (YAML 파싱 깨짐 방지)
 */
const yamlStr = s => `"${String(s ?? '')
    .replace(/\\/g, '\\\\')
    .replace(/"/g, '\\"')
    .replace(/\r?\n/g, ' ')}"`;

/**
 * 외부 텍스트 정화 — 프롬프트 인젝션 완화
 * 크롤링 HTML·스토어 설명·YouTube 설명 등 신뢰할 수 없는 외부 텍스트가
 * 파이프라인 제어 토큰([ABORT_NO_DATA], [IP_CONFUSED])을 포함하면
 * Writer/Scout이 오작동(정상 게임 스킵 등)할 수 있으므로 프롬프트 주입 전 제거.
 */
function sanitizeExternalText(text) {
    return String(text)
        .replace(/\[ABORT_NO_DATA\]/gi, '')
        .replace(/\[IP_CONFUSED\]/gi, '');
}

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
 * Gemini API 호출 공통 래퍼 — rate-limit 에러 시 키 전환 후 재시도
 * @param {() => GenerativeModel} modelFactory  호출 시마다 새 키로 모델 인스턴스 반환
 * @param {string}                prompt
 * @param {number}                maxRetries
 * @returns {Promise<string>} 응답 텍스트. maxRetries 초과 시 빈 문자열 반환.
 */
async function callGeminiWithRetry(modelFactory, prompt, maxRetries = 3) {
    let model = modelFactory();
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            await delay(5000);
            const result = await model.generateContent(prompt);
            return result.response.text();
        } catch (err) {
            const msg      = err.message || '';
            const isRateLimit = /429|quota|rate.?limit|resource.?exhausted|retry in/i.test(msg);
            const matched  = msg.match(/retry in (\d+(?:\.\d+)?)s/i);
            const waitTime = matched ? (Math.ceil(parseFloat(matched[1])) + 2) * 1000 : 15000;

            if (isRateLimit) {
                // rate limit: 다음 키로 전환 후 재시도
                model = modelFactory(); // 새 키로 모델 재생성
                console.log(`  -> ⏱️  키 전환 + ${waitTime / 1000}초 냉각 후 재시도 (${attempt}/${maxRetries})... [${msg.substring(0, 60)}]`);
            } else {
                console.log(`  -> ⏱️  ${waitTime / 1000}초 냉각 후 재시도 (${attempt}/${maxRetries})... [${msg.substring(0, 80)}]`);
            }
            if (attempt < maxRetries) await delay(waitTime);
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
 * Kroki fetch 래퍼 — 15초 타임아웃
 * Kroki 응답 지연 시 게임 1개 처리가 무한 대기하는 것을 방지.
 * 타임아웃 시 AbortError throw → 호출부의 기존 catch 폴백 경로로 흡수됨.
 */
const fetchKroki = url => fetch(url, { signal: AbortSignal.timeout(15000) });

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

/**
 * 동일 파일명 존재 확인 (멱등성 보장).
 * 확인 실패 시 false 반환해 업로드를 시도함 — 단, Drive의 files.create는
 * 덮어쓰기가 아니라 동일 이름 파일을 하나 더 생성하므로 중복 가능성 있음.
 */
async function fileExistsInDrive(fileName, folderId) {
    try {
        const safeName = fileName.replace(/'/g, "\\'"); // 아포스트로피 포함 게임명(예: Assassin's~) 쿼리 오류 방지
        const res = await drive.files.list({
            q:      `name = '${safeName}' and '${folderId}' in parents and trashed = false`,
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
        model: 'gemini-2.5-flash-lite',
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
//    2. 다이어그램 타입 감지 후 경로 분기 (stateDiagram / erDiagram / flowchart·graph)
//       - stateDiagram: 공통 전처리만 적용하고 그대로 통과 (하단 참고)
//       - erDiagram:    따옴표가 관계 레이블에 필수이므로 독립 경로에서 처리
//       - flowchart:    공통 특수문자 제거 후 토큰화 처리
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

    // ── 1.5단계: stateDiagram 전용 경로 (구조 변형 없이 통과) ────────────────
    // flowchart 경로를 태우면 반드시 깨짐:
    //   - ["'*#] 제거 → 시작/종료 상태 [*] 가 [] 로 파괴됨
    //   - 콜론 레이블 변환 → `S1 --> S2 : 진행`이 flowchart 파이프 문법으로 오변환
    // stateDiagram은 콜론 레이블이 정식 문법이므로 공통 전처리만 적용하고 반환.
    // 잔여 파싱 오류는 QA Agent가 처리 (기존엔 QA 출력도 재차 여기서 파괴돼
    // 상태 다이어그램이 구조적으로 항상 폴백行이었음).
    if (/^stateDiagram/i.test(code)) {
        return code;
    }

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
//  리포트 내 모든 ```mermaid 블록을 3단계 복구 전략으로 처리:
//    1단계 Fast-Track: sanitizeMermaid 정규식 정제 → Kroki 검증
//    2단계 QA Agent:   실패 시 Gemini 재작성 요청 → 최대 3회
//    3단계 폴백:       QA 실패 시 게임 정보 기반 초단순 다이어그램 자동 생성
//  최종 실패 시에도 폴백 다이어그램으로 대체 — 다이어그램 0개 방지
//
//  출력: Base64 인라인 SVG img 태그 (PDF·HTML 공통 — 오프라인 렌더링 안전)
//  ※ 게임당 1회만 호출하고 결과를 PDF·HTML 양쪽에 재사용할 것.
//    QA Agent는 비결정적이므로 2회 호출 시 포맷 간 다이어그램이 달라질 수 있고,
//    Kroki fetch·Gemini 호출·딜레이 비용도 2배가 됨.
// =============================================================================

// 원본 Mermaid 코드에서 다이어그램 타입 감지
function detectDiagramType(mermaidCode) {
    const code = mermaidCode.trim().toLowerCase();
    if (code.startsWith('statediagram')) return 'stateDiagram';
    if (code.startsWith('flowchart')) return 'flowchart';
    if (code.startsWith('graph td')) return 'graphTD';
    return 'graphLR';
}

// QA 3회 실패 시 → 게임 타입별 초단순 폴백 다이어그램 생성 (Kroki 통과 보장)
function buildFallbackDiagram(diagramType) {
    if (diagramType === 'stateDiagram') {
        return 'stateDiagram-v2\n    [*] --> S1\n    S1 --> S2 : 진행\n    S2 --> S3 : 완료\n    S3 --> [*]';
    }
    if (diagramType === 'flowchart') {
        return 'flowchart TD\n    A1[시작] --> B1[행동]\n    B1 --> C1{성공?}\n    C1 -->|예| D1[보상]\n    C1 -->|아니오| B1\n    D1 --> E1[종료]';
    }
    if (diagramType === 'graphTD') {
        return 'graph TD\n    A1[입력] --> B1[처리]\n    B1 --> C1[결과]\n    C1 --> D1[저장]';
    }
    return 'graph LR\n    A1[시작] --> B1[핵심]\n    B1 --> C1[보상]\n    C1 --> D1[반복]\n    D1 --> B1';
}

async function processMermaidBlocks(reportText, qaFactory) {
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
            const res     = await fetchKroki(buildKrokiUrl(cleaned));
            const svg     = await res.text();
            if (isValidKrokiSvg(res, svg)) {
                console.log(`  -> ⚡ [Fast-Track 성공]`);
                fixedMermaid = cleaned;
                cachedSvg    = svg;
            }
        } catch { /* fetch 실패/타임아웃 → 2단계로 */ }

        // ── 2단계: QA Agent ──────────────────────────────────────────────
        if (!fixedMermaid) {
            console.log(`  -> ⚠️  [Fast-Track 실패] QA 에이전트 호출...`);
            await delay(10000); // 연속 호출 버스트 방지 쿨다운
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

                const qaResultText = await callGeminiWithRetry(qaFactory, qaPrompt, 3);
                if (!qaResultText) { await delay(15000); continue; }

                try {
                    const cleaned = sanitizeMermaid(
                        qaResultText.replace(/```mermaid\s*/ig, '').replace(/```/g, '').trim()
                    );
                    const res = await fetchKroki(buildKrokiUrl(cleaned));
                    const svg = await res.text();

                    if (isValidKrokiSvg(res, svg)) {
                        console.log(`  -> ✅ [시도 ${attempt}/${MAX_QA_RETRIES}] QA 복구 성공!`);
                        fixedMermaid = cleaned;
                        cachedSvg    = svg; // QA 통과 시에도 SVG 재사용 (결과 반영 단계 fetch 절약)
                        await delay(15000);
                        break;
                    } else {
                        currentMermaid = cleaned;
                    }
                } catch { /* Kroki fetch 실패/타임아웃 → 다음 시도 */ }

                await delay(15000);
            }
        }

        // ── 결과 반영 ────────────────────────────────────────────────────
        if (fixedMermaid) {
            // PDF/HTML 공통: Base64 인라인 SVG — 오프라인 렌더링, 장기 보존 안전
            // cachedSvg: Fast-Track/QA 검증 시 이미 받은 SVG → 재사용해 Kroki 요청 절약
            try {
                const svgStr = cachedSvg ?? await fetchKroki(buildKrokiUrl(fixedMermaid)).then(r => r.text());
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
        } else {
            // ── 3단계: 폴백 다이어그램 ─────────────────────────────────────
            // QA 3회 전부 실패 → 원본 타입 감지 후 초단순 다이어그램 자동 생성
            // 폴백은 항상 Kroki 통과 보장 (영문 ID + 단순 구조)
            const diagType = detectDiagramType(originalMermaid);
            const fallback = buildFallbackDiagram(diagType);
            try {
                const res = await fetchKroki(buildKrokiUrl(fallback));
                const svg = await res.text();
                if (isValidKrokiSvg(res, svg)) {
                    console.log(`  -> 🔄 [폴백 다이어그램] QA 실패 → 자동 생성 다이어그램으로 대체`);
                    const b64 = Buffer.from(svg).toString('base64');
                    mdText += `

<div class="diagram-wrap diagram-fallback">` +
                              `<img src="data:image/svg+xml;base64,${b64}" alt="시스템 다이어그램 (자동생성)" />` +
                              `</div>

`;
                } else {
                    brokenCount++;
                    console.log(`  -> 🚨 [다이어그램 복구 실패] 폴백도 실패. 블록 제거. (누적 ${brokenCount}개)`);
                }
            } catch {
                brokenCount++;
                console.log(`  -> 🚨 [다이어그램 복구 실패] 폴백 fetch 오류. 블록 제거. (누적 ${brokenCount}개)`);
            }
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
    <title>${escapeHtml(gameTitle)} 분석 문서</title>
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
        .report-body { padding: 40px 48px 48px; }

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

        /* ── 출처 접이식 ──────────────────────────────────────────────── */
        .source-details {
            margin: 8px 0 16px;
            font-size: 0.82em;
            color: var(--text-muted);
        }
        .source-details summary {
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 4px;
            color: var(--text-muted);
            font-weight: 500;
            padding: 3px 10px;
            background: #F1F5F9;
            border-radius: 20px;
            border: 1px solid var(--border);
            user-select: none;
            list-style: none;
        }
        .source-details summary:hover { background: #E2E8F0; }
        .source-details summary::before { content: '🔗 '; font-size: 0.9em; }
        .source-details ul { margin: 8px 0 0 4px; padding-left: 16px; }
        .source-details li { margin: 3px 0; }
        .source-details a {
            color: var(--text-muted);
            font-size: 0.9em;
            word-break: break-all;
            text-decoration: none;
        }
        .source-details a:hover { color: var(--primary); text-decoration: underline; }

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

        img:not(.diagram-wrap img) {
            max-width: 100%;
            height: auto;
            display: block;
            margin: 20px auto;
            border-radius: var(--radius);
        }

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
        (function () {
            const container = document.getElementById('report-content');
            const firstH1   = container.querySelector('h1');
            if (!firstH1) return;

            const header = document.createElement('div');
            header.className = 'report-header';
            const body   = document.createElement('div');
            body.className   = 'report-body';

            header.appendChild(firstH1);
            let node = container.firstElementChild;
            while (node) {
                const next = node.nextElementSibling;
                if (node.tagName === 'BLOCKQUOTE') {
                    header.appendChild(node);
                } else if (node.tagName !== 'HR') {
                    body.appendChild(node);
                } else {
                    node.remove();
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

            // 출처 blockquote → <details> 접이식 처리
            body.querySelectorAll('blockquote').forEach(bq => {
                const text = bq.textContent || '';
                if (!text.trim().startsWith('출처:')) return;
                const urls = [...text.matchAll(/https?:\/\/[^\s,]+/g)].map(m => m[0]);
                if (urls.length === 0) return;
                const details = document.createElement('details');
                details.className = 'source-details';
                const summary = document.createElement('summary');
                summary.textContent = '출처 ' + urls.length + '개';
                details.appendChild(summary);
                const ul = document.createElement('ul');
                urls.forEach(url => {
                    const li = document.createElement('li');
                    const a  = document.createElement('a');
                    const isGrounding = url.includes('vertexaisearch') || url.includes('grounding');
                    a.href        = url;
                    a.textContent = isGrounding ? '[검색 출처]' : url.replace(/^https?:\/\//, '');
                    a.target      = '_blank';
                    a.rel         = 'noopener noreferrer';
                    li.appendChild(a);
                    ul.appendChild(li);
                });
                details.appendChild(ul);
                bq.replaceWith(details);
            });

            container.appendChild(header);
            container.appendChild(body);
        })();
    </script>
</body>
</html>`;
}


// =============================================================================
//  📺  fetchYouTubeScoutData — YouTube 자막·설명에서 수치·명칭 수집 (4회차)
//  YouTube Data API v3: 검색 → 상위 3개 영상 snippet(설명+제목) 수집
//  YOUTUBE_API_KEY 없으면 null 반환 → Scout 4회차 자동 스킵
// =============================================================================

async function fetchYouTubeScoutData(game) {
    const apiKey = process.env.YOUTUBE_API_KEY;
    if (!apiKey) return null;

    const queries = [
        `${game.title} 공략 시스템 수치 쿨타임`,
        `${game.title} guide system stats cooldown`,
        `${game.title} review gameplay mechanics`,
    ];

    const snippets = [];

    for (const q of queries) {
        try {
            const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&q=${encodeURIComponent(q)}&type=video&maxResults=3&key=${apiKey}`;
            const res  = await fetch(url, { signal: AbortSignal.timeout(10000) }); // 응답 지연 시 Scout 4회차 행 방지
            if (!res.ok) continue;
            const data = await res.json();
            if (!data.items) continue;
            for (const item of data.items) {
                const s = item.snippet;
                // 제목 + 설명 앞 500자만 수집 (토큰 절약)
                // 외부 텍스트 → 제어 토큰 정화 후 프롬프트 주입 (인젝션 완화)
                snippets.push(`[영상제목] ${sanitizeExternalText(s.title)}\n[설명] ${sanitizeExternalText((s.description || '').substring(0, 500))}`);
            }
        } catch { continue; }
    }

    if (snippets.length === 0) return null;
    return snippets.join('\n\n---\n\n');
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
                    `${game.title} 공식 홈페이지 시스템 소개`,
                    `${game.title} ${game.developer} 공식 가이드 재화 시스템`,
                    `${game.title} 네이버 공식카페 공략 시스템 재화`,
                    `${game.title} 인벤 공략 시스템 수치 재화`,
                    `${game.title} 루리웹 공략 시스템 분석`,
                ],
                instruction: `
## 이번 회차 핵심 지시
개발사(${game.developer})의 공식 홈페이지·네이버 공식 카페를 최우선으로 확인하십시오.
공식 출처에서 시스템명·재화명이 UI에 표기된 그대로 수집하십시오.
공식 출처가 부족하면 인벤(inven.co.kr)·루리웹(ruliweb.com) 공략 게시판에서 보완하십시오.
공식 출처 또는 전문 공략 URL이 없으면 이번 회차는 실패로 처리됩니다.`,
            } : {
                label: '공식 가이드·공홈·커뮤니티 — 글로벌 (1순위 출처)',
                queries: [
                    `${game.title} official website system guide`,
                    `${game.title} ${game.developer} official guide mechanics`,
                    `${game.title} official discord OR forum system guide`,
                    `${game.title} Game8 guide system mechanics`,
                    `${game.title} AppGamer guide system currency`,
                ],
                instruction: `
## 이번 회차 핵심 지시
개발사(${game.developer})의 공식 홈페이지·Discord·포럼을 최우선으로 확인하십시오.
공식 출처에서 UI에 표기된 시스템명·재화명을 그대로 수집하십시오.
공식 출처가 부족하면 Game8(game8.co)·AppGamer(appgamer.com)·Pocket Gamer에서 보완하십시오.
공식 출처 또는 전문 공략 URL이 없으면 이번 회차는 실패로 처리됩니다.`,
            };
        })(),
        2: (() => {
            return isKorean ? {
                label: '전문 분석 글·수치 공략 — 국내 (2순위 출처)',
                queries: [
                    `${game.title} 인벤 공략 시스템 수치`,
                    `${game.title} 루리웹 공략 재화 수치`,
                    `${game.title} 인벤 OR 루리웹 쿨타임 수치 효율`,
                    `${game.title} 유튜브 심층 공략 수치 시스템 재화`,
                    `${game.title} 아프리카TV OR 치지직 공략 시스템`,
                ],
                instruction: `
## 이번 회차 핵심 지시
인벤(inven.co.kr)·루리웹(ruliweb.com) 공략 게시판을 최우선으로 확인하십시오.
**수치 데이터 (처리 시간·쿨타임·재화 획득량·확률) 가 언급된 글을 우선 탐색하십시오.**
유튜브·치지직·아프리카TV 심층 공략 영상에서 UI에 표시된 명칭과 수치를 직접 확인하십시오.
위키류(나무위키 등)는 이번 회차에서 제외합니다.
전문 공략 글 또는 영상 URL이 없으면 이번 회차는 실패로 처리됩니다.`,
            } : {
                label: '전문 분석 글·수치 공략 — 글로벌 (2순위 출처)',
                queries: [
                    `${game.title} Game8 guide system mechanics`,
                    `${game.title} reddit guide system mechanics numbers`,
                    `${game.title} NGA 攻略 系统 数值`,
                    `${game.title} AppGamer OR "Pocket Gamer" guide system stats`,
                    `${game.title} steamcommunity guide system`,
                    `${game.title} youtube guide system cooldown stats mechanics`,
                ],
                instruction: `
## 이번 회차 핵심 지시
Game8(game8.co)·Reddit·NGA·AppGamer·Steam 커뮤니티 허브를 우선 확인하십시오.
**수치 데이터 (처리 시간·쿨타임·재화 획득량·확률·스탯) 가 언급된 글을 우선 탐색하십시오.**
유튜브 심층 공략·리뷰에서 UI에 표시된 명칭과 수치를 직접 확인하십시오.
위키류는 이번 회차에서 제외합니다.
전문 공략 글 또는 영상 URL이 없으면 이번 회차는 실패로 처리됩니다.`,
            };
        })(),
        3: (() => {
            return isKorean ? {
                label: '위키·커뮤니티 교차 검증 — 국내 (3순위 출처)',
                queries: [
                    `나무위키 ${game.title} 시스템 수치 재화`,
                    `${game.title} 나무위키 시스템 공략 수치`,
                    `${game.title} 아카라이브 시스템 수치 재화 공략`,
                    `${game.title} 아카라이브 시스템 재화 공략`,
                    `${game.title} 디시인사이드 OR 네이버카페 공략 수치 시스템`,
                ],
                instruction: `
## 이번 회차 핵심 지시
나무위키(namu.wiki) 또는 아카라이브(arca.live)에서 ${game.title} 문서를 찾아 시스템명·재화명을 수집하십시오.
나무위키·아카라이브는 게임 수치·시스템 정보가 상세히 정리된 경우가 많습니다. 적극 활용하되 확인된 명칭만 기재하십시오.
이전 회차에서 수집된 명칭과 교차검증하여 일치하면 신뢰도 높음, 신규 명칭이면 보통으로 표기하십시오.`,
            } : {
                label: '위키·커뮤니티 교차 검증 — 글로벌 (3순위 출처)',
                queries: [
                    `${game.title} fandom wiki system mechanics`,
                    `${game.title} wiki.gg system guide stats`,
                    `${game.title} fandom wiki system currency cooldown`,
                    `${game.title} gamepedia OR wiki.gg mechanics numbers`,
                    `${game.title} reddit community tips system numbers`,
                ],
                instruction: `
## 이번 회차 핵심 지시
Fandom(fandom.com), Gamepedia, Wiki.gg에서 ${game.title} 위키 문서를 찾아 시스템명·재화명을 수집하십시오.
팬덤 위키는 게임 수치·시스템 구조가 상세히 정리된 경우가 많습니다. 적극 활용하되 확인된 명칭만 기재하십시오.
이전 회차에서 수집된 명칭과 교차검증하여 일치하면 신뢰도 높음, 신규 명칭이면 보통으로 표기하십시오.`,
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
- 높음: 공식 가이드·공홈·카페에서 직접 확인, 또는 복수 출처에서 동일 명칭 교차 확인
- 보통: 인벤·Game8·NGA·유튜브 심층 공략·나무위키·위키 등 단일 출처에서 확인
- 낮음: 불확실한 출처 단독, 또는 명칭 유추가 의심되는 경우

## 중단 조건
- ${game.title}이 아닌 다른 게임 명칭이 섞이면: [IP_CONFUSED]
- 지정 검색어 실행 후 관련 데이터가 전혀 없으면: [ABORT_NO_DATA]
`;
}

// =============================================================================
//  📝  buildAnalysisPrompt_Phase1 — 구조·다이어그램 집중 (01~04섹션)
// =============================================================================
function buildAnalysisPrompt_Phase1(game, rank, category, factSheet = '') {
    const storeUrl = `https://play.google.com/store/apps/details?id=${game.appId}`;
    return `
# ⚠️ [최우선] 타겟 게임 고정
* **게임명:** ${game.title} / **앱 ID:** ${game.appId} / **매출 순위:** ${rank}위
* **분석 영역:** ${category}
* **URL:** ${storeUrl}

# [고정 어휘 사전]
${factSheet ? factSheet : '⚠️ 팩트 사전 없음 — 딥 서치로 직접 확인.'}

# Step 0: 메타데이터 (절대 수정 금지)
메인장르: (RPG/MMORPG/방치형/SLG전략/캐주얼퍼즐/액션슈팅/SNG시뮬/스포츠레이싱/카지노보드/기타)
서브장르: (15자 이내)
시스템:   (15자 이내 명사형)

# Phase 1: 구조 및 명세 (01~04섹션) — 다이어그램 4개 집중

## 01. 정의서 (Definition)

### 1.1 시스템 개요
검색으로 확인된 시스템 정의·구성 요소·해금 조건. 공식 설명 우선. 최소 5문장.

### 1.2 핵심 목적
- **유저 관점**: 이 시스템을 통해 유저가 얻는 것 (보상·진행·재미) — 구체적 수치 포함
- **사업 관점**: 수익화·리텐션·트래픽 유도 관점에서의 역할

### 1.3 용어 정의
확인된 게임 내 고유 용어·명칭. (★ 표 강제, 최소 6행)
| 용어 | 정의 | 비고 |

### 1.4 분석 범위 및 관련 시스템
이 시스템과 연결된 다른 시스템·재화·콘텐츠 열거.

### 1.5 설계 의도 요약
이 시스템이 왜 이렇게 설계됐는지 한 문단으로 선요약.

---

## 02. 구조도 (Architecture)

### 2.1 메인 시스템 구조도
확인된 서브시스템 간 연결 관계.
(★ Mermaid \`graph LR\` 강제 — 노드 6~10개. 크롤링 기반 실제 구조)

### 2.2 서브시스템 구조도
가장 핵심적인 서브시스템 1개 내부 구조 상세화.
(★ Mermaid \`graph TD\` 강제 — 노드 4~8개. 서브시스템 상세화)

---

## 03. 플로우차트 (Flowchart)

### 3.1 메인 이용 플로우
유저의 핵심 이용 흐름 전체.
(★ Mermaid \`flowchart TD\` 강제 — 노드 8~12개. 전체 이용 흐름)

### 3.2 핵심 서브 플로우
가장 중요한 서브 플로우 1개 (획득·소비·강화 중 하나).
(★ Mermaid \`flowchart TD\` 강제 — 노드 6~10개. 핵심 서브 플로우)

---

## 04. 상세 명세서 (Specification)

### 4.1 UI 레이아웃
확인된 화면 구성·메뉴 뎁스·주요 버튼 배치. 최소 3문장.

### 4.2 인터랙션 명세
확인된 주요 유저 액션과 시스템 반응. (★ 표 강제)
| 액션 | 입력 조건 | 시스템 반응 | 결과 |

### 4.3 애니메이션·타이밍 명세
확인된 주요 연출 타이밍·처리 시간. (★ 표 강제)
| 이벤트 | 연출/처리 | 시간(초) | 비고 |
없으면 "데이터 비공개" 표기.

### 4.4 상태 전이
확인된 시스템 내 주요 상태 변화.
(★ Mermaid \`stateDiagram-v2\` 강제 — 상태 3~5개. 전체 상태 전이)

---

# ★ 다이어그램 품질 기준 (Phase 1 최우선)
공식 사이트 크롤링 데이터를 기반으로 실제 시스템 구조를 반영한 다이어그램 생성.

## 필수 4개 (원본 수준 목표)
- 2.1: graph LR — 6~10개 노드. 메인 시스템 전체 구조
- 2.2: graph TD — 4~8개 노드. 핵심 서브시스템 내부
- 3.1: flowchart TD — 8~12개 노드. 유저 전체 플로우
- 4.4: stateDiagram-v2 — 3~5개 상태. 시스템 상태 전이

## 절대 규칙 (위반 시 QA 실패)
- **노드 ID**: 영문+숫자만 (A1, B2, Node1). 한글 ID 절대 금지
- **레이블**: 한글 8자 이내 또는 영문 12자 이내. 콜론·따옴표 금지
- **화살표**: --> 또는 -> 하나만. 혼용 금지
- **괄호**: 노드 정의에 [] 또는 () 하나만. 중첩 금지
- **화살표 텍스트**: 10자 이내. -->|텍스트| 형식

## 크롤링 데이터 활용 원칙
- 공식 사이트에서 확인된 시스템명·재화명·수치를 노드 레이블에 사용
- 실제 게임 내 UI 흐름을 플로우차트에 반영
- 패치노트·업데이트 내역의 수치를 다이어그램 주석에 활용

## 데이터 부족 시
확인된 3개 이상 요소로 최소 구조 생성. 절대 생략 금지.

# ★ 딥 서치 철칙 (Phase 1)
1. 모든 검색은 "${game.title}" + 앱ID "${game.appId}" 기준.
2. 검색으로 확인된 사실만 기술. 확인 불가는 "데이터 비공개 (검색 불가)" 표기.
3. 데이터가 전혀 없는 게임이면 [ABORT_NO_DATA] 한 줄만 출력.
4. 타겟 게임이 아닌 다른 게임 데이터가 섞이면 [IP_CONFUSED] 한 줄만 출력.

# Output Constraints
* [Mermaid 규칙] 화살표 텍스트(\`-->|텍스트|\`)는 10자 이내. 대괄호/중괄호 안에 콜론·따옴표·쉼표 절대 금지.
* [노드 ID 규칙] 노드 ID는 반드시 띄어쓰기 없는 영문+숫자 조합. 한글 노드 ID 절대 금지.
* [subgraph 규칙] 모든 \`subgraph\` 이름은 반드시 큰따옴표로 감쌀 것.
* [erDiagram 규칙] \`erDiagram\` 속성은 따옴표·코멘트 없이 '타입 이름' 형식만.
* [금지 패턴] 노드 레이블 괄호 중첩(\`([...])\`) 절대 금지. 화살표 기호 혼용 절대 금지.
`;
}

// =============================================================================
//  📝  buildAnalysisPrompt_Phase2 — 데이터·분석·비교 집중 (05~09섹션)
// =============================================================================
function buildAnalysisPrompt_Phase2(game, rank, category, factSheet = '', phase1Text = '') {
    const storeUrl = `https://play.google.com/store/apps/details?id=${game.appId}`;
    return `
# ⚠️ [최우선] 타겟 게임 고정
* **게임명:** ${game.title} / **앱 ID:** ${game.appId} / **매출 순위:** ${rank}위
* **분석 영역:** ${category}

# [Phase 1 분석 결과 요약]
${phase1Text ? phase1Text.substring(0, 3000) : '없음 — 딥 서치로 직접 보완.'}

# [고정 어휘 사전]
${factSheet ? factSheet : '⚠️ 팩트 사전 없음 — 딥 서치 집중 모드.\n최소 5회 검색 시도: 공식→커뮤니티(인벤·나무위키·아카라이브)→영문(fandom·reddit·game8)→수치 전용→벤치마크 비교.'}

# Phase 2: 데이터·분석·비교 (05~09섹션) — 수치 집중

## 05. 데이터 테이블 (Data Table)

### 5.1 재화 Source / Sink 테이블
(★ 표 강제, 최소 6행)
| 재화명 | 주요 획득처 | 주요 소모처 | 일일 획득량(추정) |

### 5.2 핵심 수치 밸런스
(★ 표 강제, 최소 15행 목표)
아래 카테고리 전부 탐색 후 확인된 것만 기재:
- 진행·성장: 레벨 상한, 스테이지 수, 강화 단계, 승급 조건
- 시간·쿨타임: 쿨타임, 대기 시간, 자동 처리 주기, 세션 길이
- 확률·배율: 가챠 확률, 강화 성공률, 크리티컬 배율, 천장 수치
- 재화·경제: 일일 획득량, 소모량, 교환 비율, 패스 가격
- 전투·밸런스: 기본 스탯, 데미지 공식, 속성 배율, PvP 매칭 범위
| 항목 | 수치 | 출처 |

### 5.3 레시피·처리 속도 테이블
확인된 생산·처리·제작 레시피 전체. (★ 표 강제)
없으면 강화 단계별 비용·성공률 테이블로 대체.
| 항목 | 처리 시간 | 입력 재료 | 출력 결과 | 비고 |

### 5.4 DB 테이블 스키마 (구현 참고용)
확인된 시스템 구조 기반 핵심 테이블 추론. (★ 표 강제)
| 테이블명 | 주요 컬럼 | 설명 | 레코드 규모(추정) |

### 5.5 ORM 코드 템플릿 (TypeScript / Python)
핵심 테이블 2~3개를 Prisma 스키마 또는 SQLAlchemy 형식으로 작성.
불확실한 부분은 TODO 주석.

### 5.6 핵심 API·이벤트 흐름
확인된 주요 유저 액션 → 서버 처리 흐름 추론.
| 액션 | 요청 파라미터(추론) | 서버 처리(추론) | 응답(추론) |

---

## 06. 기획 의도 및 심리 설계 분석 (Design Intent)

### 6.1 설계 의도
이 시스템이 왜 이렇게 설계됐는가. 수익화·리텐션·트래픽 유도 관점. 최소 5문장.

### 6.2 심리 설계
수집된 팩트에서 역으로 읽히는 심리 트리거. FOMO·손실 회피·보상 스케줄·사회적 비교 등.
근거 없는 트리거 나열 금지.

---

## 07. 문제점 및 개선 제안 (Issues & Suggestions)
(★ 표 강제)

### 7.1 문제점
| 항목 | 문제 내용 | 근거 |

### 7.2 개선 제안
| 문제 항목 | 개선 방향 | 타 게임 사례 |

---

## 08. 벤치마크 비교 분석 (Benchmark)

### 8.1 비교 대상
동일 장르·유사 시스템 타 게임 2~3개 선정. 선정 근거 명시.

### 8.2 비교 매트릭스
(★ 표 강제)
| 항목 | ${game.title} | 비교 게임 A | 비교 게임 B |

### 8.3 트레이드오프 분석
이 게임 시스템이 타 게임 대비 얻은 것·포기한 것.

### 8.4 실개발 인사이트
타 게임 사례에서 이 시스템에 적용 가능한 개선 아이디어.

---

## 09. 예외 처리 및 엣지케이스 (Edge Cases)

### 9.1 엣지케이스
| 케이스 | 발생 조건 | 시스템 반응 | 출처 |

### 9.2 사이드 이펙트
이 시스템이 다른 시스템에 미치는 의도치 않은 영향.

### 9.3 동시성·레이스 컨디션
멀티플레이·길드·실시간 이벤트에서 발생 가능한 동시 접근 이슈.

### 9.4 예외 상황별 플로우
(★ Mermaid \`flowchart TD\` — 확인된 케이스만, 없으면 생략)

---

# ★ 딥 서치 철칙 (Phase 2)
1. 모든 검색은 "${game.title}" + 앱ID "${game.appId}" 기준. IP 혼동 즉시 키워드 변경.
2. **수치 탐색 최우선**: 검색 횟수 제한 없음. 수치 15행 채울 때까지 반복 탐색.
   - "${game.title} 쿨타임 수치", "${game.title} 가챠 확률", "${game.title} 강화 성공률"
   - "${game.title} 나무위키", "${game.title} fandom wiki", "${game.title} reddit guide"
   - 나무위키·팬덤 위키 수치 테이블 반드시 확인.
3. "데이터 비공개" 표기 전 최소 3가지 다른 검색어로 재시도 필수.
4. 복수 출처 교차 검증. 단일 출처는 *(단일 출처)* 주석.
5. 커뮤니티 측정값 적극 활용. *(커뮤니티 측정값)* 주석.
`;
}

// =============================================================================
//  🎨  buildDiagramPrompt — 다이어그램 전담 재생성 프롬프트
//  Phase1 텍스트에서 시스템 구조를 파악 후 고품질 다이어그램 4개만 집중 생성
//  검색 없이 주어진 텍스트만 참고 — QA 통과 최우선
// =============================================================================
function buildDiagramPrompt(game, phase1Text, factSheet = '') {
    return `
# 다이어그램 전담 생성 — QA 통과 최우선

## 게임 정보
- 게임명: ${game.title}

## Phase1 분석 결과 (참고용)
${phase1Text.substring(0, 4000)}

## 공식 데이터
${factSheet ? factSheet.substring(0, 1500) : '없음'}

---

# 지시사항
위 Phase1 분석 결과를 바탕으로 아래 4개의 Mermaid 다이어그램만 생성하라.
검색은 하지 말 것. 텍스트에서 구조를 파악해 표현.

## [다이어그램 1] 메인 시스템 구조도
\`\`\`mermaid
graph LR
    (6~10개 노드. 서브시스템 간 연결 관계)
\`\`\`

## [다이어그램 2] 서브시스템 구조도
\`\`\`mermaid
graph TD
    (4~8개 노드. 핵심 서브시스템 내부)
\`\`\`

## [다이어그램 3] 유저 이용 플로우
\`\`\`mermaid
flowchart TD
    (8~12개 노드. 전체 이용 흐름)
\`\`\`

## [다이어그램 4] 상태 전이
\`\`\`mermaid
stateDiagram-v2
    (3~5개 상태. 주요 상태 전이)
\`\`\`

---

# 절대 규칙
- 노드 ID: 영문+숫자만 (A1, B2). 한글 ID 절대 금지
- 레이블: 한글 8자 이내. 콜론·따옴표·쉼표 금지
- 화살표: --> 하나만. 혼용 금지
- 괄호: [] 또는 () 하나만. 중첩 금지
- subgraph 사용 금지
- 위 4개 다이어그램 외 다른 텍스트 출력 금지
`;
}

// Phase1 텍스트의 기존 Mermaid 블록을 새 다이어그램으로 교체
function replaceDiagramsInPhase1(phase1Text, newDiagramText) {
    const newDiagrams = [];
    const newDiagRegex = /```mermaid[\s\S]*?```/gi;
    for (const match of [...newDiagramText.matchAll(newDiagRegex)]) {
        newDiagrams.push(match[0]);
    }
    if (newDiagrams.length === 0) return phase1Text;

    let diagIdx = 0;
    let result = phase1Text.replace(/```mermaid[\s\S]*?```/gi, () => {
        if (diagIdx < newDiagrams.length) return newDiagrams[diagIdx++];
        return '';
    });
    if (diagIdx === 0) return result + '\n\n' + newDiagrams.join('\n\n');
    // Phase1 블록 수 < 새 다이어그램 수인 경우 잉여분을 뒤에 덧붙임 (조용한 유실 방지)
    if (diagIdx < newDiagrams.length) {
        result += '\n\n' + newDiagrams.slice(diagIdx).join('\n\n');
    }
    return result;
}

// =============================================================================
//  🏃  메인 파이프라인
// =============================================================================

async function main() {

    // ── 0. 환경 변수 사전 검증 ──────────────────────────────────────────────
    const REQUIRED_ENV = ['GCP_CLIENT_ID', 'GCP_CLIENT_SECRET', 'GCP_REFRESH_TOKEN', 'GDRIVE_FOLDER_ID', 'GEMINI_API_KEY'];
    // YOUTUBE_API_KEY는 선택 사항 — 없으면 Scout 4회차 자동 스킵
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

        const YOUTUBE_API_KEY   = process.env.YOUTUBE_API_KEY;
        const MAX_SCOUT_RETRIES = YOUTUBE_API_KEY ? 4 : 3;
        if (YOUTUBE_API_KEY) console.log('  -> 📺 YouTube API 키 감지 — Scout 4회차 활성화');

        const stats = {
            full: 0, partial: 0, skipped: 0, diagram: 0,
            scoutOk: 0, scoutFallback: 0, scoutCross: 0, scoutAbort: 0,
            phase2Ok: 0, phase2Fail: 0,
        };

        // ── 4. 게임별 분석 문서 생성 루프 ────────────────────────────────────
        for (let idx = 0; idx < targetGames.length; idx++) {
            // 게임 간 RPM 버퍼 — 첫 게임은 스킵, 이후 120초 대기
            // (프로젝트당 RPM 10 × 5키 = 분당 50회. 게임 1개당 최소 7~10회 호출이므로
            //  120초 간격이면 각 게임 처리 중 키 풀 냉각 시간 확보)
            if (idx > 0) await delay(120000);

            const game     = targetGames[idx];
            const rank     = game.actualRank;
            const progress = `[${idx + 1}/${targetGames.length}]`;

            // 4-1. 앱 상세 정보 수집 (출시일 + 스토어 설명 + 최근 업데이트)
            // 404/지역제한 등으로 실패할 수 있으므로 try-catch로 스킵 처리
            let detail, releaseDate, appDescription, appRecentChanges;
            try {
                detail           = await gplay.app({ appId: game.appId });
                releaseDate      = detail.released      || '정보 없음';
                appDescription   = detail.description   || '';
                appRecentChanges = detail.recentChanges || '';
            } catch (appErr) {
                console.log(`  -> ⚠️  [APP-SKIP] ${game.title} — 앱 상세 조회 실패 (${appErr.message?.substring(0, 40)}). 스킵.`);
                stats.skipped++;
                if (idx < targetGames.length - 1) await delay(30000);
                continue;
            }

            // 4-2. Gemini 모델 초기화 (Rate limit 시 키 전환 팩토리)
            // modelFactory: 호출할 때마다 다음 키로 새 모델 인스턴스 반환
            const makeModels = () => {
                const genAI = new GoogleGenerativeAI(apiKeyQueue.next());
                return initModels(genAI, game.title, game.appId);
            };
            const draftFactory = () => makeModels().draftModel;
            const qaFactory    = () => makeModels().qaModel;

            const category = pickCategory();
            console.log(`\n${progress} 매출 ${rank}위: ${game.title}`);
            console.log(`  -> 🎯 분석 영역: [${category}] / 출시일: ${releaseDate}`);

            // 4-3. Scout 0회차 — 공식 사이트·개발자 노트 직접 크롤링
            // Google Play 스토어 페이지 + 개발사 공식 URL 직접 fetch
            // 검색 경유 없이 원문 텍스트 추출 → 수치·패치노트·업데이트 내역 확보
            let officialCrawlData = '';
            try {
                const storeUrl = `https://play.google.com/store/apps/details?id=${game.appId}&hl=ko`;
                const crawlUrls = [storeUrl];

                // 개발사 공식 웹사이트 URL이 있으면 추가
                if (detail.developerWebsite) crawlUrls.push(detail.developerWebsite);

                for (const url of crawlUrls) {
                    try {
                        const res = await fetch(url, {
                            headers: {
                                'User-Agent': 'Mozilla/5.0 (compatible; GameAnalysisBot/1.0)',
                                'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8'
                            },
                            signal: AbortSignal.timeout(10000)
                        });
                        if (!res.ok) continue;
                        const html = await res.text();
                        // 태그 제거 후 순수 텍스트 추출
                        const text = html
                            .replace(/<script[\s\S]*?<\/script>/gi, '')
                            .replace(/<style[\s\S]*?<\/style>/gi, '')
                            .replace(/<[^>]+>/g, ' ')
                            .replace(/\s{2,}/g, ' ')
                            .trim()
                            .substring(0, 8000); // 8000자 제한
                        if (text.length > 200) {
                            // 외부 HTML → 제어 토큰 정화 후 프롬프트 주입 (인젝션 완화)
                            officialCrawlData += `\n\n## 크롤링 출처: ${url}\n${sanitizeExternalText(text)}`;
                        }
                    } catch { /* 개별 URL 실패 시 스킵 */ }
                }

                // 스토어 설명 + 최근 업데이트 추가 (프롬프트 주입분은 정화 적용)
                if (appDescription) officialCrawlData += `\n\n## 스토어 설명\n${sanitizeExternalText(appDescription.substring(0, 3000))}`;
                if (appRecentChanges) officialCrawlData += `\n\n## 최근 업데이트 내역\n${sanitizeExternalText(appRecentChanges.substring(0, 2000))}`;

                if (officialCrawlData.length > 300) {
                    console.log(`  -> 🌐 [CRAWL] 공식 사이트 크롤링 완료 (${officialCrawlData.length}자)`);
                }
            } catch (crawlErr) {
                console.log(`  -> ⚠️  [CRAWL] 크롤링 실패 — Scout 검색으로 진행`);
            }

            // 4-4. Scout — 공식 가이드 기준 시스템명 수집 (최대 4회)
            // 4회차: YouTube Data API 자막·설명 수집 (YOUTUBE_API_KEY 없으면 자동 스킵)

            // 크롤링 데이터를 factSheet 초기값으로 주입
            let factSheet    = officialCrawlData
                ? `## 공식 사이트 크롤링 데이터\n${officialCrawlData.substring(0, 6000)}\n[출처신뢰도] 높음`
                : '';
            let scoutAborted = false;
            let scoutFormatFallback = false; // 모든 회차가 형식 실패 → Writer 자체 딥서치로 진행

            for (let sAttempt = 1; sAttempt <= MAX_SCOUT_RETRIES; sAttempt++) {
                try {
                    // ── 4회차: YouTube 자막·설명 수집 ──────────────────────
                    if (sAttempt === 4) {
                        console.log(`  -> 📺 [SCOUT 4/${MAX_SCOUT_RETRIES}] YouTube 영상 수치·명칭 수집...`);
                        const ytData = await fetchYouTubeScoutData(game);
                        if (!ytData) {
                            console.log(`  -> ⚠️  [SCOUT-YT] YouTube 데이터 없음. FALLBACK.`);
                            break;
                        }
                        // YouTube 데이터를 Gemini로 요약 → factSheet 보완
                        const ytPrompt = [
                            '아래는 YouTube 영상 제목과 설명에서 수집한 "' + game.title + '" 관련 데이터입니다.',
                            '게임 내 시스템명·재화명·수치(쿨타임/처리시간/획득량 등)를 추출해 기존 팩트 사전을 보완하십시오.',
                            '',
                            '## YouTube 수집 데이터',
                            ytData,
                            '',
                            '## 기존 팩트 사전 (보완 대상)',
                            factSheet || '없음',
                            '',
                            '## 출력 형식 (기존 팩트 사전에 수치 정보를 추가·보완한 결과만 출력)',
                            '[시스템명]  (쉼표 구분)',
                            '[재화명]    (쉼표 구분)',
                            '[수치데이터] (항목명: 수치 형식, 쉼표 구분. 예: 쿨타임: 8초, 일일획득량: 500)',
                            '[출처URL]   (YouTube URL)',
                            '[출처신뢰도] 보통',
                        ].join('\n');
                        const ytResult = await callGeminiWithRetry(() => makeModels().scoutModel, ytPrompt, 2);
                        if (ytResult && ytResult.includes('[시스템명]')) {
                            // 기존 factSheet에 YouTube 수치 데이터 병합
                            const numericMatch = ytResult.match(/\[수치데이터\][^\n]*/);
                            if (numericMatch) {
                                factSheet = (factSheet || '') + '\n\n## YouTube 수집 수치 데이터\n' + numericMatch[0];
                            }
                            console.log('  -> ✅ [SCOUT-YT] YouTube 수치 데이터 병합 완료');
                        }
                        break; // 4회차는 보완 목적 — 성공 여부 무관하게 루프 종료
                    }

                    await delay(3000);
                    const scoutPrompt = buildScoutPrompt(game, sAttempt);
                    // 실제 전략 라벨을 프롬프트에서 추출 (첫 줄 ## 검색 전략: 이후)
                    const labelMatch  = scoutPrompt.match(/## 검색 전략: (.+)/);
                    const scoutLabel  = labelMatch ? labelMatch[1] : `${sAttempt}회차`;
                    console.log(`  -> 🔭 [SCOUT ${sAttempt}/${MAX_SCOUT_RETRIES}] ${scoutLabel} 탐색...`);
                    const scoutText = await callGeminiWithRetry(() => makeModels().scoutModel, scoutPrompt, 2);

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

                    // 신뢰도 낮음: 다음 회차가 있으면 계속 시도, 마지막 회차는 수용
                    // (나무위키 등 위키 소스는 교차검증 소스로 활용 — 무조건 버리지 않음)
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

            // 4-5. 분석 문서 초안 생성 — Phase 1 (구조·다이어그램 집중)
            console.log(`  -> 📐 [Phase 1] 구조·다이어그램 생성 중...`);
            const phase1Raw = await callGeminiWithRetry(draftFactory, buildAnalysisPrompt_Phase1(game, rank, category, factSheet), MAX_DRAFT_RETRIES);

            if (!phase1Raw) {
                const errMsg = `[${rank}위] ${game.title} — Phase1 생성 ${MAX_DRAFT_RETRIES}회 실패`;
                console.error(`  -> ❌ ${errMsg}`);
                errorLog.push(errMsg);
                stats.skipped++;
                continue;
            }
            if (phase1Raw.includes('[ABORT_NO_DATA]')) {
                const errMsg = `[${rank}위] ${game.title} — Writer ABORT_NO_DATA`;
                console.log(`  -> ⏭️  ${errMsg}`);
                errorLog.push(errMsg);
                stats.skipped++;
                continue;
            }

            // var 호이스팅 의존 제거 — 블록 밖에서 let으로 선언 (strict 코드베이스 일관성)
            let phase1Text;
            if (phase1Raw.includes('[IP_CONFUSED]')) {
                if (factSheet) {
                    console.log(`  -> ⚠️  [WRITER-IP] IP_CONFUSED — factSheet 제거 후 Phase1 재시도...`);
                    const retryP1 = await callGeminiWithRetry(draftFactory, buildAnalysisPrompt_Phase1(game, rank, category, ''), MAX_DRAFT_RETRIES);
                    if (!retryP1 || retryP1.includes('[IP_CONFUSED]') || retryP1.includes('[ABORT_NO_DATA]')) {
                        const errMsg = `[${rank}위] ${game.title} — Writer IP_CONFUSED (재시도 후에도 실패)`;
                        console.log(`  -> ⏭️  ${errMsg}`);
                        errorLog.push(errMsg);
                        stats.skipped++;
                        continue;
                    }
                    phase1Text = retryP1;
                    console.log(`  -> ✅ [WRITER-IP] Phase1 딥서치 재시도 성공`);
                } else {
                    const errMsg = `[${rank}위] ${game.title} — Writer IP_CONFUSED`;
                    console.log(`  -> ⏭️  ${errMsg}`);
                    errorLog.push(errMsg);
                    stats.skipped++;
                    continue;
                }
            } else {
                phase1Text = phase1Raw;
            }
            console.log(`  -> ✅ [Phase 1] 완료`);

            // 4-6. 다이어그램 전담 재생성 (Phase1 직후 — 검색 없이 구조 파악 후 집중 생성)
            console.log(`  -> 🎨 [DIAG] 다이어그램 전담 재생성 중...`);
            await delay(30000); // 짧은 냉각 후 다이어그램 전담 호출
            const diagRaw = await callGeminiWithRetry(draftFactory, buildDiagramPrompt(game, phase1Text, factSheet), MAX_DRAFT_RETRIES);
            if (diagRaw && diagRaw.includes('```mermaid')) {
                phase1Text = replaceDiagramsInPhase1(phase1Text, diagRaw);
                const diagCount = (diagRaw.match(/```mermaid/gi) || []).length;
                console.log(`  -> ✅ [DIAG] 다이어그램 ${diagCount}개 재생성 완료`);
            } else {
                console.log(`  -> ⚠️  [DIAG] 다이어그램 재생성 실패 — Phase1 원본 유지`);
            }

            // 4-7. Phase 2 (데이터·수치·분석·비교 집중)
            console.log(`  -> ⏳ [Phase 1→2] 키 냉각 대기 (150초)...`);
            await delay(150000);
            console.log(`  -> 📊 [Phase 2] 데이터·수치·분석 생성 중...`);
            const phase2Raw = await callGeminiWithRetry(draftFactory, buildAnalysisPrompt_Phase2(game, rank, category, factSheet, phase1Text), MAX_DRAFT_RETRIES);

            if (!phase2Raw) {
                console.log(`  -> ⚠️  [Phase 2] 생성 실패 — Phase 1만으로 진행`);
            }
            const phase2Text = phase2Raw || '';
            if (phase2Text) {
                stats.phase2Ok++;
                console.log(`  -> ✅ [Phase 2] 완료`);
            } else {
                stats.phase2Fail++;
            }

            // 4-8. Phase 1 + Phase 2 합치기
            // Phase 1에서 메타데이터(메인장르/서브장르/시스템) 추출 후
            // 두 결과를 하나의 MD로 병합
            const combinedRaw = phase1Text + '\n\n---\n\n' + phase2Text;

            // 4-9. 리포트 텍스트 정제
            // 끝 ``` 무조건 제거는 리포트가 정당한 코드블록(5.5 ORM / 9.4 flowchart)으로
            // 끝날 때 닫는 펜스를 지워 마지막 블록을 깨뜨림 → 문서 전체가
            // ```markdown 펜스로 래핑된 경우에만 앞뒤 짝을 함께 제거하도록 수정
            const effectiveReport = combinedRaw;
            let reportText = effectiveReport.trim();
            if (/^```(markdown|md)?\s*\n/i.test(reportText)) {
                reportText = reportText
                    .replace(/^```(markdown|md)?\s*\n/i, '')
                    .replace(/\n?```\s*$/, '');
            }
            reportText = reportText.trim();

            // Phase1+Phase2 병합 — 메타데이터 중복 제거
            // Phase2 시작 구분자(## 05.) 이후 중복 메타데이터 블록 제거
            const phase2StartIdx = reportText.indexOf('## 05.');
            if (phase2StartIdx > 0) {
                const phase1Part = reportText.substring(0, phase2StartIdx);
                let phase2Part = reportText.substring(phase2StartIdx);
                // 메타데이터 3줄(메인장르/서브장르/시스템) 제거
                phase2Part = phase2Part.split('\n').filter(line =>
                    !line.startsWith('메인장르:') &&
                    !line.startsWith('서브장르:') &&
                    !line.startsWith('시스템:')
                ).join('\n');
                reportText = phase1Part + phase2Part;
            }

            // 파일명용 핵심 시스템명 추출 (^+m 앵커 — 메타데이터 줄만 매칭)
            const systemMatch  = reportText.match(/^시스템:\s*([^\n]+)/m);
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
            // ^ + m 플래그로 줄 시작만 매칭 — 본문 중간의 "…핵심 시스템: 강화…" 같은
            // 문장이 오폭으로 잘려나가는 것 방지. 줄 끝 개행까지 제거(\n?는 마지막 줄 대응).
            reportText = reportText
                .replace(/^메인장르:.*\n?/gm, '')
                .replace(/^서브장르:.*\n?/gm, '')
                .replace(/^시스템:.*\n?/gm,   '')
                .trim();

            // ── MD 전용: YAML frontmatter + 순수 텍스트 본문 ────────────────
            // LLM 학습용. SVG/Base64 없음. Mermaid 코드블록 원본 유지.
            // 문자열 값은 전부 yamlStr로 이스케이프 — 따옴표/개행 포함 시 YAML 파손 방지
            const mdLlmText = [
                '---',
                `title: ${yamlStr(game.title + ' 분석 문서')}`,
                `date: ${yamlStr(dateString)}`,
                `rank: ${rank}`,
                `app_id: ${yamlStr(game.appId)}`,
                `developer: ${yamlStr(detail.developer     || game.developer)}`,
                `developer_id: ${yamlStr(detail.developerId  || '')}`,
                `release_date: ${yamlStr(releaseDate)}`,
                // updated: 라이브러리 버전에 따라 초/밀리초 epoch이 혼재 → 1e12 초과면 이미 ms로 간주
                `updated: ${yamlStr(detail.updated ? new Date(detail.updated > 1e12 ? detail.updated : detail.updated * 1000).toISOString().slice(0, 10) : '')}`,
                `version: ${yamlStr(detail.version         || '')}`,
                `category: ${yamlStr(category)}`,
                `category_code: ${yamlStr(categoryCode)}`,
                `core_system: ${yamlStr(coreSystemName)}`,
                `genre: ${yamlStr(detail.genre             || '')}`,
                `genre_id: ${yamlStr(detail.genreId        || '')}`,
                `content_rating: ${yamlStr(detail.contentRating || '')}`,
                `score: ${detail.score              || 0}`,
                `ratings: ${detail.ratings          || 0}`,
                `reviews: ${detail.reviews          || 0}`,
                `installs: ${yamlStr(detail.installs        || '')}`,
                `min_installs: ${detail.minInstalls  || 0}`,
                `free: ${detail.free                ?? true}`,
                `price: ${detail.price              || 0}`,
                `price_text: ${yamlStr(detail.priceText    || '무료')}`,
                `ad_supported: ${detail.adSupported  ?? false}`,
                `offers_iap: ${detail.offersIAP      ?? false}`,
                `iap_range: ${yamlStr(detail.IAPRange       || '')}`,
                `android_version: ${yamlStr(detail.androidVersion || '')}`,
                `summary: ${yamlStr(detail.summary        || '')}`,
                '---',
                '',
                // 스토어 설명 — YAML에 넣으면 깨지므로 본문 첫 섹션으로 분리
                // LLM이 게임의 공식 소개 텍스트를 컨텍스트로 학습할 수 있음
                ...(appDescription ? [
                    '## 스토어 설명 (Google Play 공식)',
                    '',
                    appDescription.replace(/\r\n/g, '\n').trim(),
                    '',
                    '---',
                    '',
                ] : []),
                // 최근 업데이트 내역 — 라이브 옵스·이벤트 패턴 학습에 유용
                ...(appRecentChanges ? [
                    '## 최근 업데이트 내역',
                    '',
                    appRecentChanges.replace(/\r\n/g, '\n').trim(),
                    '',
                    '---',
                    '',
                ] : []),
                reportText,
            ].join('\n');

            // ── PDF/HTML 전용: blockquote 헤더 + Mermaid → SVG 치환 ─────────
            // 사람이 읽는 시각화 문서용.
            // 공식 사이트 크롤링 성공 여부에 따라 출처 표기
            const crawlSourceLine = officialCrawlData
                ? `> **데이터 출처:** 공식 사이트 크롤링 + Google Search Grounding (신뢰도: 높음)`
                : `> **데이터 출처:** Google Search Grounding`;
            const crawlUrlLine = detail.developerWebsite
                ? `> **공식 사이트:** ${detail.developerWebsite}`
                : '';

            const reportTextForVisual = [
                `# [${rank}위] ${game.title} 분석 문서`,
                `> **분석 카테고리:** ${category}`,
                `> **핵심 시스템:** ${coreSystemName}`,
                `> **개발사:** ${game.developer}`,
                `> **매출 순위:** ${rank}위 (${dateString} 기준)`,
                `> **출시일:** ${releaseDate}`,
                crawlSourceLine,
                crawlUrlLine,
                '',
                '---',
                '',
                reportText,
            ].filter(Boolean).join('\n');

            // 4-10. Mermaid 블록 처리 — 게임당 1회만 실행 후 PDF/HTML 양쪽에 재사용
            // mdLlmText는 Mermaid 코드블록 원본 유지 (LLM 학습 노이즈 방지)
            // Base64 인라인 SVG: 오프라인 렌더링 + 포맷 간 다이어그램 일치 보장
            // (기존: pdf/html 2회 호출 → Kroki fetch·QA Gemini 호출·딜레이가 전부 2배였고
            //  QA 비결정성 때문에 두 포맷의 다이어그램이 서로 달라질 수 있었음)
            const { mdText: visualMdText, brokenCount } = await processMermaidBlocks(reportTextForVisual, qaFactory);
            if (brokenCount > 0) {
                stats.diagram++;
                console.log(`  -> ⚠️  [다이어그램 ${brokenCount}개 블록 제거] 나머지 콘텐츠로 저장 진행`);
            }

            // 4-11. 파일명 생성 (카테고리 코드 포함)
            const safeTitle    = game.title.replace(/[/\\?%*:|"<>]/g, '_');
            const baseFileName = `[${dateString}]_${String(rank).padStart(3, '0')}위_${safeTitle}_(${coreSystemName})_[${categoryCode}]`;

            // 4-12. MD / PDF / HTML 저장
            // MD: mdLlmText (순수 텍스트 + YAML frontmatter, LLM 학습용)
            // PDF/HTML: visualMdText (SVG 치환 완료, 사람이 읽는 시각화 문서 — 동일 소스 공유)
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
                        content = await convertToPdf(visualMdText, PDF_OPTIONS);
                    } else if (u.tag === 'HTML') {
                        console.log(`  -> 🌐 [HTML] 변환 시작...`);
                        const parsedBody = marked.parse(visualMdText);
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

            // 4-13. 결과 집계
            if      (savedCount === 3) { stats.full++; }
            else if (savedCount >= 1)  { stats.partial++; console.log(`  -> ⚠️  일부 포맷 저장 실패 (${savedCount}/3)`); }
            else                       { console.error(`  -> ❌ 모든 포맷 저장 실패`); }

            if (idx < targetGames.length - 1) await delay(30000);
        }

        // ── 5. 최종 결산 ─────────────────────────────────────────────────────
        // stats.diagram: 폴백까지 실패해 블록이 제거된 게임 수 — 문서 자체는 저장됨.
        //   (기존 결산은 이를 "스킵/저장 안 됨"으로 표기하고 skipped에서 차감해
        //    자동 스킵 수치가 음수가 될 수 있었음 → 실제 동작 기준으로 수정)
        const failedCount = targetGames.length - stats.full - stats.partial - stats.skipped;
        console.log(`\n${'='.repeat(56)}`);
        console.log(`[${dateString}] 📊 최종 결산`);
        console.log(`  목표 처리량              ${targetGames.length}개`);
        console.log(`  완전 성공 (3포맷 모두)   ${stats.full}개`);
        console.log(`  부분 성공 (1~2포맷)      ${stats.partial}개`);
        console.log(`  다이어그램 블록 제거 발생 ${stats.diagram}개  (문서는 저장됨)`);
        console.log(`  자동 스킵 (데이터 부족)  ${stats.skipped}개`);
        console.log(`  완전 실패                ${failedCount}개`);
        console.log(`──────────────────────────────────────────────────────`);
        console.log(`  Scout 성공 (factSheet)   ${stats.scoutOk}개`);
        console.log(`  Scout FALLBACK (딥서치)  ${stats.scoutFallback}개`);
        console.log(`  Scout CROSS (IP 오염)    ${stats.scoutCross}개`);
        console.log(`  Scout ABORT (데이터 없음)${stats.scoutAbort}개`);
        console.log(`──────────────────────────────────────────────────────`);
        console.log(`  Phase2 성공              ${stats.phase2Ok}개`);
        console.log(`  Phase2 실패 (P1만 저장)  ${stats.phase2Fail}개`);
        console.log(`${'='.repeat(56)}`);
        console.log(`🎉 Google Drive 동기화 완료`);
        console.log(`${'='.repeat(56)}\n`);

        // 누적 에러 상세 출력 — 결산 수치만으로는 어떤 게임이 왜 실패했는지 알 수 없음
        if (errorLog.length > 0) {
            console.error(`\n📋 에러 상세 (${errorLog.length}건):`);
            errorLog.forEach((e, i) => console.error(`  ${String(i + 1).padStart(2, ' ')}. ${e}`));
            console.error('');
        }



    } catch (fatalError) {
        console.error('💀 치명적 에러 발생:', fatalError);
        process.exit(1);
    }
}

main();
