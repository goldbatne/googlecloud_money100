'use strict';

// =============================================================================
//
//  📦 Google Play 게임 역기획서 자동 생성 파이프라인
//
//  흐름 요약:
//    1. Google Play 매출 순위 스크래핑
//    2. Gemini API로 역기획서 초안 생성 (딥서치 포함)
//    3. Mermaid 다이어그램 검증 및 자동 복구
//    4. MD / PDF / HTML 3포맷 변환
//    5. Google Drive 날짜별 폴더에 저장
//
//  환경 변수 (필수):
//    GCP_CLIENT_ID       - Google OAuth2 클라이언트 ID
//    GCP_CLIENT_SECRET   - Google OAuth2 클라이언트 시크릿
//    GCP_REFRESH_TOKEN   - Google OAuth2 리프레시 토큰
//    GDRIVE_FOLDER_ID    - 저장 대상 루트 폴더 ID
//    GEMINI_API_KEY      - Gemini API 키 (쉼표로 구분해 복수 등록 가능)
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

// Gemini API 호출 최대 재시도 횟수
const MAX_DRAFT_RETRIES         = 3; // API 오류(rate limit) 시 재시도
const MAX_QA_RETRIES            = 5; // Mermaid 다이어그램 복구 재시도
const MAX_HALLUCINATION_RETRIES = 5; // 할루시네이션 감지 시 재검색 최대 횟수

// Gemini 역기획서 분석 카테고리 목록 (매 게임마다 랜덤 선택)
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
//  🔑  API 키 Round-Robin Queue — 사용한 키를 맨 뒤로 순환
// =============================================================================

class ApiKeyQueue {
    constructor(keys) {
        if (!keys || keys.length === 0) {
            console.error('❌ GEMINI_API_KEY가 등록되지 않았습니다.');
            process.exit(1);
        }
        this._keys = [...keys];
    }

    /** 다음 키를 꺼내고 맨 뒤에 재삽입 (순환) */
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
//  🛠️  유틸리티 함수
// =============================================================================

/** Promise 기반 sleep */
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

/**
 * KST 기준 날짜 분해 (Intl.DateTimeFormat + Asia/Seoul)
 * @returns {{ dateString, yearStr, monthStr, dayStr }}
 */
function getKSTDateParts() {
    const now       = new Date();
    const toStr     = (options) => new Intl.DateTimeFormat('ko-KR', { timeZone: 'Asia/Seoul', ...options }).format(now);
    const year      = toStr({ year:  'numeric' }).replace(/\D/g, '');
    const month     = toStr({ month: '2-digit' }).replace(/\D/g, '').padStart(2, '0');
    const day       = toStr({ day:   '2-digit' }).replace(/\D/g, '').padStart(2, '0');

    return {
        dateString: `${year}-${month}-${day}`,  // 파일명용: 2025-06-01
        yearStr:    `${year}년`,                 // 폴더명용: 2025년
        monthStr:   `${month}월`,                // 폴더명용: 06월
        dayStr:     `${day}일`,                  // 폴더명용: 01일
    };
}

/**
 * Mermaid 코드를 Kroki.io SVG 렌더링 URL로 변환
 * Kroki는 deflate 압축 + URL-safe Base64 인코딩을 요구함
 *
 * @param {string} mermaidCode
 * @returns {string} Kroki SVG URL
 */
function buildKrokiUrl(mermaidCode) {
    const compressed = zlib.deflateSync(Buffer.from(mermaidCode, 'utf8'));
    const encoded    = compressed
        .toString('base64')
        .replace(/\+/g, '-')   // URL-safe: + → -
        .replace(/\//g, '_')   // URL-safe: / → _
        .replace(/=+$/g, '');  // 패딩 제거
    return `https://kroki.io/mermaid/svg/${encoded}`;
}

/**
 * Kroki 응답이 정상 SVG인지 검증
 * Kroki는 파싱 오류 시에도 HTTP 200을 반환하며 SVG 내에 에러 문자열을 포함함
 *
 * @param {Response} response fetch Response 객체
 * @param {string}   svgText  응답 본문
 * @returns {boolean}
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

/**
 * Drive에 폴더가 있으면 ID 반환, 없으면 새로 생성 후 ID 반환
 *
 * 기존: catch(err) { return parentId; } 로 에러를 묵음 처리
 * → 폴더 생성 실패 시 부모 폴더에 파일이 쌓이는 버그 존재
 * 개선: 에러를 그대로 throw → 호출자(main)에서 명시적으로 catch
 *
 * @param {string} folderName 생성할 폴더명
 * @param {string} parentId   부모 폴더 ID
 * @returns {Promise<string>} 폴더 ID
 */
async function getOrCreateFolder(folderName, parentId) {
    const res = await drive.files.list({
        q:      `name = '${folderName}' and '${parentId}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
        fields: 'files(id)',
    });

    if (res.data.files.length > 0) {
        return res.data.files[0].id; // 기존 폴더 ID 반환
    }

    const folder = await drive.files.create({
        resource: { name: folderName, mimeType: 'application/vnd.google-apps.folder', parents: [parentId] },
        fields:   'id',
    });
    return folder.data.id;
}

/**
 * Drive 특정 폴더에 동일 파일명이 이미 존재하는지 확인
 * 재실행 멱등성 보장: 존재하면 업로드 스킵
 *
 * @param {string} fileName
 * @param {string} folderId
 * @returns {Promise<boolean>}
 */
async function fileExistsInDrive(fileName, folderId) {
    try {
        const res = await drive.files.list({
            q:      `name = '${fileName}' and '${folderId}' in parents and trashed = false`,
            fields: 'files(id)',
        });
        return res.data.files.length > 0;
    } catch {
        return false; // 확인 불가 시 덮어쓰기 허용
    }
}

/**
 * Drive에 파일 업로드 (중복 시 스킵)
 *
 * @param {{ fileName: string, folderId: string, mimeType: string, content: string|Buffer }} opts
 * @returns {Promise<boolean>} 실제로 업로드했으면 true, 중복 스킵이면 false
 */
async function uploadToDrive({ fileName, folderId, mimeType, content }) {
    const alreadyExists = await fileExistsInDrive(fileName, folderId);
    if (alreadyExists) {
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
        .replace(/[\u200B-\u200D\uFEFF]/g, '')  // 유니코드 제로폭 문자 제거 (눈에 안 보이는 오류 원인)
        .replace(/\/\/.*$/gm, '')               // // 주석 제거
        .replace(/%%.*$/gm, '')                 // %% Mermaid 주석 제거
        .trim();

    code = code
        .replace(/["'*#]/g, '')                 // 파싱 오류 유발 특수문자 제거
        .replace(/^\s*(\d+\.|[-*])\s+/gm, ''); // 목록 기호(1. / - / *) 제거

    // ── erDiagram 전용 처리 ─────────────────────────────────────────────────
    if (code.match(/^erDiagram/i)) {

        // 엔티티 블록 내 속성을 '타입 이름' 두 단어만 남기고 나머지 제거
        const lines    = code.split('\n');
        let inEntity   = false;
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
            .replace(/erDiagram\s+(.*)/i, 'erDiagram\n$1') // erDiagram 키워드 뒤 줄바꿈 보장
            .replace(/\(.*?\)/g,  '')                       // 괄호 그룹 제거 (주석/설명 형태)
            .replace(/,/g,        '\n')                     // 쉼표를 줄바꿈으로 (속성 구분자 정규화)
            .replace(/\bENUM\b/gi, '')                      // Mermaid가 지원 안 하는 ENUM 키워드 제거
            .replace(/\b(PK|FK|UK|Optional)\b/gi, '')       // 파싱 오류 유발 키워드 제거
            // 잘못된 엔티티 접두사 제거 (관계 표현식 앞)
            .replace(/^[a-zA-Z가-힣0-9_]+\s*:\s*(?=[a-zA-Z0-9_]+\s*\|\|--)/gm, '')
            // 관계 레이블을 단순 "has"로 통일 (복잡한 한글 레이블이 파싱 오류 유발)
            .replace(/(\|\|--o{|}\|--\|{|}\|--o{|\|\|--\|{|}-o|}-\||-o|-\|)\s*([a-zA-Z0-9_]+)\s*:\s*(.*?)$/gm, '$1 $2 : "has"');

        return code;
    }

    // ── flowchart / graph 전용 처리 ─────────────────────────────────────────

    // 비표준 괄호 조합을 표준 괄호로 교체
    code = code
        .replace(/\(\[/g, '[').replace(/\]\)/g, ']')   // ([...]) → [...]
        .replace(/\[\[/g, '[').replace(/\]\]/g, ']')   // [[...]] → [...]
        .replace(/\(\(/g, '(').replace(/\)\)/g, ')')   // ((...)) → (...)
        .replace(/--\[/g, '-->[')                       // --[ → -->[
        .replace(/-\[/g,  '->[');                       // -[ → ->[

    const lines         = code.split('\n');
    const processedLines = [];
    let   autoIdCount   = 0;
    const nodeTexts     = [];  // @@N0@@, @@N1@@... 로 임시 대체된 노드 라벨
    const edgeTexts     = [];  // @@E0@@, @@E1@@... 로 임시 대체된 엣지 라벨

    for (let line of lines) {
        line = line.trim();
        if (!line) continue;

        // graph/flowchart 선언 줄 및 end 키워드는 그대로 통과
        if (line.match(/^(graph|flowchart)\s+[a-zA-Z]+/i) || line.toLowerCase() === 'end') {
            processedLines.push(line);
            continue;
        }

        // subgraph 이름에 따옴표 강제 적용 (없으면 한글 파싱 오류)
        if (line.match(/^subgraph\s+(.*)/i)) {
            const name = line.replace(/^subgraph\s+/i, '').replace(/["']/g, '');
            processedLines.push(`subgraph "${name}"`);
            continue;
        }

        // 화살표 뒤 콜론 레이블(A --> B : 설명)을 파이프 레이블(A -->|설명| B)로 변환
        if (line.match(/(-->|-\.->|==>|---)\s*([^:]+?)\s*:\s*(.+)$/)) {
            line = line.replace(/(-->|-\.->|==>|---)\s*([^:]+?)\s*:\s*(.+)$/, '$1|$3| $2');
        }

        // 엣지 레이블(|...|, -- ... -->)을 @@E{n}@@ 토큰으로 임시 치환
        // → 이후 공백 제거 단계에서 레이블 내용이 손상되는 것을 방지
        line = line.replace(/\|([^|]+)\|/g, (_, content) => {
            edgeTexts.push(content.replace(/["'\n]/g, ' ').trim());
            return `|@@E${edgeTexts.length - 1}@@|`;
        });
        line = line.replace(/--\s*([^>|@]+?)\s*-->/g, (_, content) => {
            edgeTexts.push(content.replace(/["'\n]/g, ' ').trim());
            return `-->|@@E${edgeTexts.length - 1}@@|`;
        });

        // 노드 라벨([...], {...}, (...))을 @@N{n}@@ 토큰으로 임시 치환
        // → 공백 제거 후 복원할 때 따옴표를 붙여 안전하게 렌더링
        line = line.replace(/\[([^\]]+)\]/g, (_, c) => { nodeTexts.push(`["${c.replace(/["'\n]/g, ' ').trim()}"]`); return `@@N${nodeTexts.length - 1}@@`; });
        line = line.replace(/\{([^}]+)\}/g,  (_, c) => { nodeTexts.push(`{"${c.replace(/["'\n]/g, ' ').trim()}"}`); return `@@N${nodeTexts.length - 1}@@`; });
        line = line.replace(/\(([^)]+)\)/g,  (_, c) => { nodeTexts.push(`("${c.replace(/["'\n]/g, ' ').trim()}")`); return `@@N${nodeTexts.length - 1}@@`; });

        // 노드 ID가 없는 단독 노드/화살표 타겟에 자동 ID 부여 (N_AUTO_0, N_AUTO_1, ...)
        // → 한글 노드 ID는 Mermaid 파서가 거부하므로 영문 ID 강제 부여
        line = line.replace(/^(\s*)(@@N\d+@@)/,                      (_, sp, n) => `${sp}N_AUTO_${autoIdCount++}${n}`);
        line = line.replace(/(-->|-\.->|==>|---)\s*(@@N\d+@@)/g,     (_, arr, n) => `${arr} N_AUTO_${autoIdCount++}${n}`);

        // 모든 공백 제거 후 화살표/연산자 주변에만 공백 복원
        line = line.replace(/\s+/g, '');
        line = line
            .replace(/-->/g,   ' --> ')
            .replace(/-\.->/g, ' -.-> ')
            .replace(/==>/g,   ' ==> ')
            .replace(/---/g,   ' --- ')
            .replace(/&/g,     ' & ');

        // 임시 토큰을 원래 텍스트로 복원
        line = line.replace(/@@E(\d+)@@/g, (_, i) => edgeTexts[parseInt(i)]);
        line = line.replace(/@@N(\d+)@@/g, (_, i) => nodeTexts[parseInt(i)]);

        processedLines.push(line);
    }

    return processedLines.join('\n');
}


// =============================================================================
//  🔄  Mermaid 블록 처리 (processMermaidBlocks)
//
//  리포트 텍스트 내 모든 ```mermaid 블록을 순서대로 처리한다.
//
//  2단계 복구 전략:
//    1단계 (Fast-Track): sanitizeMermaid로 정규식 정제 → Kroki 검증
//    2단계 (QA Agent):   Fast-Track 실패 시 Gemini에게 재작성 요청 → 최대 5회 시도
//
//  기존: 한 블록이라도 최종 실패하면 전체 리포트 폐기 (boolean 플래그로 루프 탈출)
//  개선: 실패한 블록만 ⚠️ 경고 플레이스홀더로 대체하고 나머지 블록/텍스트는 정상 저장
//
//  @param {string}         reportText 전체 리포트 마크다운 텍스트
//  @param {GenerativeModel} qaModel   다이어그램 복구 전용 Gemini 모델
//  @returns {Promise<{ mdText: string, pdfText: string, brokenCount: number }>}
// =============================================================================

async function processMermaidBlocks(reportText, qaModel) {
    const mermaidBlockRegex = /```mermaid\s*([\s\S]*?)```/gi;
    let mdText      = '';
    let pdfText     = '';
    let lastIndex   = 0;
    let brokenCount = 0; // 최종 복구 실패한 블록 수

    for (const match of [...reportText.matchAll(mermaidBlockRegex)]) {

        // 현재 블록 이전의 일반 텍스트를 먼저 추가
        const preText = reportText.substring(lastIndex, match.index);
        mdText  += preText;
        pdfText += preText;

        const originalMermaid = match[1];
        let   fixedMermaid    = null; // 최종 확정된 Mermaid 코드

        // ── 1단계: Fast-Track (정규식 정제) ───────────────────────────────
        try {
            const cleaned = sanitizeMermaid(originalMermaid);
            const res     = await fetch(buildKrokiUrl(cleaned));
            const svg     = await res.text();

            if (isValidKrokiSvg(res, svg)) {
                console.log(`  -> ⚡ [Fast-Track 성공]`);
                fixedMermaid = cleaned;
            }
        } catch {
            // Fast-Track fetch 자체가 실패한 경우 → 2단계로 진행
        }

        // ── 2단계: QA Agent (Gemini 재작성) ──────────────────────────────
        if (!fixedMermaid) {
            console.log(`  -> ⚠️  [Fast-Track 실패] QA 에이전트 호출...`);
            let currentMermaid = originalMermaid;

            for (let attempt = 1; attempt <= MAX_QA_RETRIES; attempt++) {

                // 2회차 이상부터는 이전 실패를 경고 메시지로 명시
                const warningMsg = attempt > 1
                    ? '**[경고] 이전 시도에서 파서 에러가 발생했습니다! 화살표 텍스트는 10자 이내로 짧게 쓰십시오.**\n'
                    : '';

                const qaPrompt = `
${warningMsg}
1. [ERD 규칙]:       \`erDiagram\` 속성에 따옴표나 코멘트를 모두 지우고 '타입 이름'만 남기세요.
2. [Flowchart 규칙]: 모든 \`subgraph\` 이름은 반드시 큰따옴표(\`""\`)로 감쌀 것.
3. [노드 ID 규칙]:   노드 ID는 반드시 띄어쓰기 없는 영문+숫자 조합(예: A1, Node2)으로만 작성. 한글 노드 ID 절대 금지.

[원본 코드]:
${currentMermaid}
`;

                // QA 모델 호출 (rate limit 에러 시 동적 대기 후 최대 3회 재시도)
                let qaResultText = '';
                for (let qaTry = 1; qaTry <= 3; qaTry++) {
                    try {
                        await delay(5000);
                        const res = await qaModel.generateContent(qaPrompt);
                        qaResultText = res.response.text();
                        break;
                    } catch (err) {
                        const msg      = err.message || '';
                        const matched  = msg.match(/retry in (\d+(?:\.\d+)?)s/i);
                        const waitTime = matched ? (Math.ceil(parseFloat(matched[1])) + 2) * 1000 : 15000;
                        console.log(`  -> ⏱️  [QA] ${waitTime / 1000}초 냉각 (내부 시도 ${qaTry}/3)...`);
                        await delay(waitTime);
                    }
                }

                if (!qaResultText) {
                    await delay(15000);
                    continue; // QA 응답 자체를 못 받은 경우 다음 시도로
                }

                // QA 결과를 정제 후 Kroki 재검증
                try {
                    const cleaned    = sanitizeMermaid(
                        qaResultText.replace(/```mermaid\s*/ig, '').replace(/```/g, '').trim()
                    );
                    const res        = await fetch(buildKrokiUrl(cleaned));
                    const svg        = await res.text();

                    if (isValidKrokiSvg(res, svg)) {
                        console.log(`  -> ✅ [시도 ${attempt}/${MAX_QA_RETRIES}] QA 복구 성공!`);
                        fixedMermaid   = cleaned;
                        await delay(15000); // 성공 후 다음 요청을 위한 안정화 딜레이
                        break;
                    } else {
                        currentMermaid = cleaned; // 실패한 코드를 다음 시도의 기반으로 사용
                    }
                } catch {
                    // Kroki fetch 실패 → 다음 시도
                }

                await delay(15000);
            }
        }

        // ── 블록 처리 결과 반영 ───────────────────────────────────────────
        if (fixedMermaid) {
            // 성공: MD에는 코드 블록 그대로, PDF/HTML에는 Kroki SVG 이미지로 삽입
            mdText  += `\`\`\`mermaid\n${fixedMermaid}\n\`\`\``;
            pdfText += `\n\n<div style="page-break-inside:avoid;break-inside:avoid;text-align:center;width:100%;">` +
                       `<img src="${buildKrokiUrl(fixedMermaid)}" alt="시스템 다이어그램" ` +
                       `style="max-width:100%;max-height:220mm;height:auto;object-fit:contain;margin:0 auto;display:block;" />` +
                       `</div>\n\n`;
        } else {
            // 실패: 해당 블록만 경고 메시지로 대체 (전체 리포트는 계속 진행)
            brokenCount++;
            console.log(`  -> 🚨 [다이어그램 복구 실패] 플레이스홀더로 대체합니다. (누적 ${brokenCount}개)`);
            const placeholder = `\n\n> ⚠️ **[다이어그램 렌더링 실패]** Mermaid 파싱 오류로 인해 이 다이어그램을 표시할 수 없습니다.\n\n`;
            mdText  += placeholder;
            pdfText += placeholder;
        }

        lastIndex = match.index + match[0].length;
    }

    // 마지막 Mermaid 블록 이후의 잔여 텍스트 추가
    const remaining = reportText.substring(lastIndex);
    mdText  += remaining;
    pdfText += remaining;

    return { mdText, pdfText, brokenCount };
}


// =============================================================================
//  🌐  HTML 리포트 템플릿
//
//  기존 버그: CSS @import URL이 마크다운 링크 문법과 혼용되어 폰트 로드 불가
//  수정:     올바른 문자열 URL로 교체
// =============================================================================

function buildHtmlReport(gameTitle, bodyHtml) {
    return `<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${gameTitle} 역기획서</title>
    <style>
        @import url('[https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css](https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css)');

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
//  📝  Gemini 역기획서 프롬프트 생성
// =============================================================================

/**
 * 게임 정보와 분석 카테고리를 받아 역기획서 생성 프롬프트를 반환
 *
 * @param {{ title: string, developer: string }} game
 * @param {number} rank
 * @param {string} category
 * @returns {string}
 */
// =============================================================================
//  🔭  팩트 수집 전담 프롬프트 (buildScoutPrompt)
//
//  목적: 역기획서 작성 전, 실제 게임 내 고유 명칭(시스템명·재화명·메뉴명)만
//        정확하게 수집한다. 이 결과가 writer의 '고정 어휘 사전'이 된다.
//
//  출력 형식 (텍스트, JSON 아님):
//    [시스템명]  성장의탑 / 마계원정 / 영웅 승급
//    [재화명]    다이아 / 골드 / 명성 / 우정 포인트
//    [메뉴명]    영웅 / 던전 / 길드 / 상점 / 이벤트
//    [출처URL]   https://...
//
//  출처 URL이 없으면 재시도 트리거 (scoutHasSource 검증에서 잡음)
// =============================================================================

// buildScoutPrompt — 3단계 순차 검색 (1:공식카페 2:나무위키/인벤 3:커뮤니티)
// 검색어를 코드가 직접 지정해 AI가 키워드를 임의로 결정하지 못하게 막는다.

function buildScoutPrompt(game, attempt = 1) {
    const storeUrl = `https://play.google.com/store/apps/details?id=${game.appId}`;

    // 회차별 검색 전략 — 검색 키워드까지 코드가 직접 지정
    // 검색 전략 설계 원칙:
    //   - site: 연산자는 Gemini Search에서 지원이 불안정하므로 사용하지 않음
    //   - 대신 도메인명을 검색어 안에 자연어로 포함시키거나 (예: "나무위키 메이플 키우기")
    //     검색 결과 우선순위 힌트를 trusted_domains로 명시해 AI가 필터링하도록 유도
    const strategies = {
        1: {
            label: '공식 1차 출처 (공식카페·공홈·디스코드)',
            queries: [
                // 자연어에 도메인명 포함 → site: 없이도 해당 출처 우선 검색
                `${game.title} 네이버 공식카페 시스템 재화 메뉴`,
                `${game.title} ${game.developer} 공식 홈페이지 콘텐츠`,
                `${game.title} 앱ID ${game.appId} 공략 시스템명`,
                `${game.title} 공식 디스코드 게임 시스템 안내`,
            ],
            priority: '공식 네이버카페 > 공식 홈페이지 > 공식 디스코드 > 구글플레이 설명',
            trusted_domains: 'cafe.naver.com (공식카페), 개발사 공식 도메인, discord.com',
        },
        2: {
            label: '나무위키·인벤 (정제된 2차 출처)',
            queries: [
                // 나무위키는 도메인명을 직접 검색어에 포함하면 결과 상위에 노출됨
                `나무위키 ${game.title} 시스템 재화 콘텐츠`,
                `인벤 ${game.title} 공략 재화 메뉴 정리`,
                `${game.title} 위키 콘텐츠 시스템 목록`,
            ],
            priority: 'namu.wiki > inven.co.kr > gamewith.kr',
            trusted_domains: 'namu.wiki, inven.co.kr, gamewith.kr, arca.live',
        },
        3: {
            label: '커뮤니티·유튜브 (최후 수단)',
            queries: [
                `아카라이브 OR 디시인사이드 ${game.title} 시스템 재화 정리`,
                `${game.title} 초보 가이드 재화 종류 메뉴 설명`,
                `유튜브 ${game.title} 공략 시스템 설명 영상`,
            ],
            priority: 'arca.live > dcinside.com > reddit.com > youtube.com',
            trusted_domains: 'arca.live, dcinside.com, reddit.com, youtube.com',
        },
    };

    const s = strategies[attempt] || strategies[3];

    return `
# [팩트 수집 ${attempt}회차] ${game.title} — 실제 게임 내 명칭만 수집

## ⚠️ 타겟 게임 고정 (절대 변경 금지)
- 게임명:    ${game.title}
- 앱 ID:     ${game.appId}
- 스토어URL: ${storeUrl}
- 개발사:    ${game.developer}

---

## 검색 전략: ${s.label}

### 이번 회차 지정 검색어 (아래 순서대로 실행)
${s.queries.map((q, i) => (i+1) + '. ' + q).join('\n')}
')}

### 신뢰 우선순위
${s.priority}

### 허용 도메인 (이 도메인 출처만 신뢰)
${s.trusted_domains}

---

## 수집 항목 (4가지만, 분석·추측 절대 금지)

**[시스템명]** 게임 UI에 실제로 표시되는 콘텐츠/기능 이름
- 조건: 유저가 실제로 탭하거나 메뉴에서 보는 명칭 그대로
- 형식: 최대 12개, 쉼표 구분
- ❌ 금지: 비슷한 다른 게임 시스템명 유추·혼용

**[재화명]** 게임 내 실제 화폐·포인트·재료 명칭 (UI 표기 그대로)
- 형식: 최대 12개, 쉼표 구분
- ❌ 금지: "골드", "다이아" 같은 일반 명칭 추측 (실제 명칭 확인 필수)

**[메뉴명]** 메인 화면 하단 탭 또는 주요 진입 버튼 명칭
- 형식: 최대 8개, 쉼표 구분

**[출처URL]** 위 정보를 직접 확인한 페이지 URL
- 필수: 최소 1개 이상 (URL 없으면 이번 회차 실패로 간주)
- 조건: 허용 도메인 내 URL만 유효
- ❌ 금지: google.com/search 결과 URL, 상상으로 만든 URL

---

## 출력 형식 (이 형식 외 다른 텍스트 절대 금지)
[시스템명]  (쉼표 구분 목록)
[재화명]    (쉼표 구분 목록)
[메뉴명]    (쉼표 구분 목록)
[출처URL]   (URL1, URL2, ...)
[출처신뢰도] 높음 / 보통 / 낮음

## 신뢰도 판단 기준
- 높음: 공식 카페·공홈·나무위키에서 직접 확인한 명칭
- 보통: 인벤·arca 등 신뢰할 만한 커뮤니티 다수 교차 확인
- 낮음: 단일 커뮤니티 글·유튜브 제목에서만 확인

## 중단 조건
- ${game.title}이 아닌 다른 게임 명칭이 섞이면: [IP_CONFUSED]
- 지정 검색어 3개 모두 실행해도 관련 데이터 없으면: [ABORT_NO_DATA]
`;
}

function buildAnalysisPrompt(game, rank, category, factSheet = '') {
    // Google Play 스토어 URL을 직접 박아 Gemini가 정확한 앱을 앵커로 삼도록 함
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


// detectHallucination — 3가지 기준으로 재검색 트리거 판단
// 감지1: [IP_CONFUSED] | 감지2: 게임명 3회 미만 | 감지3: 데이터비공개 5회+

function detectHallucination(text, gameTitle) {
    // 감지 1: AI 자율 IP 혼동 신호
    if (text.includes('[IP_CONFUSED]')) {
        return { detected: true, reason: 'AI가 IP 혼동을 스스로 감지 [IP_CONFUSED]' };
    }

    // 감지 2: 게임명 등장 횟수 미달
    const escapedTitle = gameTitle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const nameCount    = (text.match(new RegExp(escapedTitle, 'gi')) || []).length;
    if (nameCount < 3) {
        return { detected: true, reason: `게임명 등장 ${nameCount}회 미달 (기준: 3회) — IP 혼동 의심` };
    }

    // 감지 3: "데이터 비공개" 과다 — 사실상 검색 정보 없음
    const noDataCount = (text.match(/데이터 비공개/g) || []).length;
    if (noDataCount >= 5) {
        return { detected: true, reason: `"데이터 비공개" ${noDataCount}회 감지 — 검색 정보 부족` };
    }

    return { detected: false, reason: '' };
}

// buildRetryPrompt — 할루시네이션 감지 후 재검색 프롬프트 (1~5회차 전략 확대)
// 1:appId포함 2:나무위키/인벤 3:유튜브 4:Game8/NGA 5:최소정보+추정허용

function buildRetryPrompt(game, rank, category, prevText, failReason, attempt, factSheet = '') {
    const storeUrl = `https://play.google.com/store/apps/details?id=${game.appId}`;

    // 회차별 검색 전략 — 점진적으로 범위를 넓혀 실제 데이터 확보 시도
    const strategies = {
        1: `검색어에 앱ID(${game.appId})를 추가하여 재검색. 예: "${game.title} ${game.appId} 시스템"`,
        2: `나무위키·인벤·레딧(r/androidgaming)에서 "${game.title}" 전용 문서/스레드를 직접 탐색.`,
        3: `유튜브에서 "${game.title} 공략" "${game.title} 시스템 설명" 영상의 제목과 설명을 분석.`,
        4: `일본 Game8·중국 NGA·대만 巴哈姆特에서 "${game.title}" 관련 페이지를 검색.`,
        5: `확인된 최소 공개 정보만 사용. 나머지는 장르 특성 기반 합리적 추정으로 보완. 추정 항목은 *(추정)* 표시 필수.`,
    };

    return `
# ⚠️ [재검색 요청 — ${attempt}회차 / 최대 ${MAX_HALLUCINATION_RETRIES}회차]
${factSheet ? `
# [고정 어휘 사전] (스카우트에서 수집한 실제 명칭 — 재검색 시에도 반드시 준수)
${factSheet}

위 사전에 있는 명칭은 그대로 사용, 없는 시스템명·재화명은 임의 생성 금지.
` : ''}

이전 분석에서 다음 문제가 감지되었습니다:
> **감지된 문제:** ${failReason}

이번 재검색 핵심 전략:
> **${strategies[attempt]}**

---

# [최우선] 타겟 게임 식별 (절대 변경 금지)
* **정확한 게임명:** ${game.title}
* **앱 ID:**         ${game.appId}
* **스토어 URL:**    ${storeUrl}
* **분석 타겟 영역:** ${category}

위 게임이 아닌 다른 게임 데이터가 섞이면 즉시 검색어를 바꾸십시오.

---

# 이전 분석의 문제 구간 (참고용)
${prevText.substring(0, 800)}
...(이하 생략)

위 내용에서 **"데이터 비공개"로 처리된 항목**과 **다른 게임 데이터가 혼입된 항목**을
이번 재검색에서 반드시 올바른 정보로 채우십시오.

---

# Step 0: 메타데이터 정의 (절대 수정 금지)
메인장르: (RPG / MMORPG / 방치형 / SLG/전략 / 캐주얼/퍼즐 / 액션/슈팅 / SNG/시뮬레이션 / 스포츠/레이싱 / 카지노/보드 / 기타 중 하나)
서브장르: (15자 이내 자유 형식)
시스템:   (15자 이내 명사형, 파일명에 사용될 핵심 시스템명)

# Step 1 ~ Step 2: 9단계 구조로 처음부터 재작성
이전 내용을 그대로 복사하지 마십시오. 전체 재작성.

# Output Constraints
* [Mermaid 규칙]  화살표 텍스트 10자 이내. 대괄호/중괄호 안에 콜론·따옴표·쉼표 절대 금지.
* [노드 ID 규칙]  영문+숫자 조합만 허용 (예: A1, NodeB2). 한글 노드 ID 절대 금지.
* 5회차 후에도 타겟 게임 데이터를 찾을 수 없으면 [ABORT_NO_DATA] 출력.
* 타겟 외 데이터가 섞였다면 [IP_CONFUSED] 출력.
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

    const errorLog = []; // 전체 실행 중 발생한 에러 메시지 누적 (마지막에 Drive에 업로드)

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

        // actualRank: 배열 인덱스 기반 실제 순위 (1부터 시작)
        const allGames = rawGames.map((game, index) => ({ ...game, actualRank: index + 1 }));

        const { dateString, yearStr, monthStr, dayStr } = getKSTDateParts();

        // ── 2. Drive 폴더 구조 생성 ──────────────────────────────────────────
        // 구조: 루트 / 연도 / 연도_포맷 / 월_포맷 / 일_포맷
        // 예:   GDRIVE_FOLDER_ID / 2025년 / 2025년_pdf / 06월_pdf / 01일_pdf
        let mdFolderId, pdfFolderId, htmlFolderId;
        try {
            const yearId      = await getOrCreateFolder(yearStr,  ROOT_FOLDER_ID);
            const mdYearId    = await getOrCreateFolder(`${yearStr}_md`,   yearId);
            const pdfYearId   = await getOrCreateFolder(`${yearStr}_pdf`,  yearId);
            const htmlYearId  = await getOrCreateFolder(`${yearStr}_html`, yearId);
            const mdMonthId   = await getOrCreateFolder(`${monthStr}_md`,  mdYearId);
            const pdfMonthId  = await getOrCreateFolder(`${monthStr}_pdf`, pdfYearId);
            const htmlMonthId = await getOrCreateFolder(`${monthStr}_html`,htmlYearId);
            mdFolderId        = await getOrCreateFolder(`${dayStr}_md`,    mdMonthId);
            pdfFolderId       = await getOrCreateFolder(`${dayStr}_pdf`,   pdfMonthId);
            htmlFolderId      = await getOrCreateFolder(`${dayStr}_html`,  htmlMonthId);
        } catch (folderErr) {
            // 폴더 구조가 깨진 상태에서 계속 진행하면 데이터가 유실되므로 즉시 종료
            console.error(`❌ Drive 폴더 구조 생성 실패: ${folderErr.message}`);
            process.exit(1);
        }

        // ── 3. 처리 대상 게임 목록 슬라이싱 ────────────────────────────────
        const targetGames = allGames.slice(START_RANK - 1, END_RANK);
        console.log(`\n[${dateString}] 🗄️  파이프라인 가동 (${START_RANK}위 ~ ${END_RANK}위, 총 ${targetGames.length}개)`);

        // 집계 카운터 (full/partial/skipped 분리)
        let fullSuccessCount    = 0; // MD + PDF + HTML 모두 저장 성공
        let partialSuccessCount = 0; // 1~2개 포맷만 저장 성공
        let skippedCount        = 0; // AI 판단 스킵 또는 API 3회 실패
        let diagramBrokenCount  = 0; // 다이어그램 일부 복구 실패 (리포트 자체는 저장됨)

        // ── 4. 게임별 역기획서 생성 루프 ────────────────────────────────────
        for (let idx = 0; idx < targetGames.length; idx++) {
            const game      = targetGames[idx];
            const rank      = game.actualRank;
            const progress  = `[${idx + 1}/${targetGames.length}]`;

            // 4-1. 앱 출시일 수집 (별도 상세 조회 필요)
            let releaseDate = '정보 없음';
            try {
                const detail = await gplay.app({ appId: game.appId });
                releaseDate  = detail.released || '정보 없음';
            } catch {
                console.log(`  -> ⚠️  출시일 수집 실패`);
            }

            // 4-2. Gemini 모델 초기화 (Round-Robin 키 순환)
            const currentKey = apiKeyQueue.next();
            const genAI      = new GoogleGenerativeAI(currentKey);

            // ── Scout 모델: 시스템명·재화명·메뉴명 수집 전담 ──────────────────────
            //    - Google Search 활성화 (실제 명칭을 긁어와야 하므로 필수)
            //    - 출력은 짧은 텍스트 목록만. 분석·문서 작성 금지.
            const scoutModel = genAI.getGenerativeModel({
                model: 'gemini-2.5-flash',
                tools: [{ googleSearch: {} }],
                systemInstruction:
                    `당신은 게임 데이터 수집 전문 크롤러입니다. ` +
                    `분석·설명·추측은 절대 금지. ` +
                    `오직 실제 게임 UI에 표시되는 명칭만 수집해 지정된 형식으로 출력하십시오. ` +
                    `검색 대상은 반드시 "${game.title}" (앱ID: ${game.appId}) 단 하나입니다.`,
            });

            // 역기획서 초안 작성 모델 (Google Search 도구 활성화)
            // systemInstruction에도 게임명·appId를 고정해 모델 수준에서 혼동 방지
            const draftModel = genAI.getGenerativeModel({
                model: 'gemini-2.5-flash',
                tools: [{ googleSearch: {} }],
                systemInstruction:
                    '당신은 인간의 심리를 꿰뚫어 보는 15년 차 수석 게임 시스템 기획자이자 디렉터입니다. ' +
                    `이번 세션의 분석 대상은 오직 "${game.title}" (앱ID: ${game.appId}) 단 하나입니다. ` +
                    '같은 IP를 공유하더라도 이름이 다른 게임(예: "메이플스토리M"과 "메이플 키우기"는 별개)의 데이터를 절대 혼용하지 마십시오. ' +
                    '검색 결과가 타겟 게임과 다른 게임이면 즉시 검색어를 바꾸십시오. ' +
                    'UX와 BM을 바탕으로 한 합리적 역기획(Educated Guess)은 허용하나, 시스템 뼈대나 핵심 명칭을 지어내는 것은 금지합니다. ' +
                    '1차 검색에서 정보가 부족하면 검색 키워드를 바꿔 심층 사이트를 끝까지 추적하는 딥 서치(Deep Search)를 수행하십시오. ' +
                    '시스템의 흔적조차 없으면 [ABORT_NO_DATA], 타겟 외 게임 데이터가 섞였다고 판단되면 [IP_CONFUSED]를 출력하십시오.',
            });

            // 다이어그램 복구 전용 모델 (검색 불필요, 코드 출력만)
            const qaModel = genAI.getGenerativeModel({
                model: 'gemini-2.5-flash',
                systemInstruction:
                    '당신은 감정이 없는 엄격한 다이어그램 컴파일러입니다. ' +
                    '기획적 의도, 설명, 마크다운(```) 기호 없이 오직 완벽하게 동작하는 Mermaid 순수 코드만 반환하십시오.',
            });

            const category = ANALYSIS_CATEGORIES[Math.floor(Math.random() * ANALYSIS_CATEGORIES.length)];
            console.log(`\n${progress} 매출 ${rank}위: ${game.title}`);
            console.log(`  -> 🎯 분석 영역: [${category}] / 출시일: ${releaseDate}`);

            // 4-3. Scout — 공식카페→나무위키→커뮤니티 3단계 순서로 실제 명칭 수집
            //             높음/보통만 factSheet 채택, 낮음이면 다음 회차로 재시도

            // minInstalls 기준으로 scout 횟수 결정: 100만+→3회 / 10만+→2회 / 미만→1회
            const gameInstalls    = game.minInstalls || 0;
            const MAX_SCOUT_RETRIES = gameInstalls >= 1_000_000 ? 3
                                   : gameInstalls >= 100_000   ? 2
                                   : 1;
            if (MAX_SCOUT_RETRIES < 3) {
                console.log(`  -> ℹ️  [SCOUT-LIMIT] 설치수 ${gameInstalls.toLocaleString()}. scout 최대 ${MAX_SCOUT_RETRIES}회로 제한.`);
            }

            let factSheet           = '';
            let scoutTrustLevel     = 'none'; // none / low / medium / high
            let scoutAborted        = false;

            for (let sAttempt = 1; sAttempt <= MAX_SCOUT_RETRIES; sAttempt++) {
                try {
                    await delay(3000);
                    console.log(`  -> 🔭 [SCOUT ${sAttempt}/${MAX_SCOUT_RETRIES}] 팩트 수집 시도...`);
                    const scoutResult = await scoutModel.generateContent(buildScoutPrompt(game, sAttempt));
                    const scoutText   = scoutResult.response.text().trim();

                    // 중단 조건 확인
                    if (scoutText.includes('[ABORT_NO_DATA]')) {
                        console.log(`  -> ⏭️  [SCOUT-SKIP] 데이터 없음 확인. 스킵.`);
                        scoutAborted = true;
                        break;
                    }
                    if (scoutText.includes('[IP_CONFUSED]')) {
                        console.log(`  -> ⚠️  [SCOUT-IP ${sAttempt}회] IP 혼동 감지. 다음 회차로.`);
                        continue;
                    }

                    // URL 존재 여부 확인
                    const hasUrl = scoutText.includes('[출처URL]') && /https?:\/\//.test(scoutText);
                    if (!hasUrl) {
                        console.log(`  -> ⚠️  [SCOUT-NO-URL ${sAttempt}회] URL 없음. 다음 회차로.`);
                        continue;
                    }

                    // 신뢰도 레벨 파싱
                    const trustMatch = scoutText.match(/\[출처신뢰도\]\s*(높음|보통|낮음)/);
                    const trust      = trustMatch ? trustMatch[1] : '낮음';

                    if (trust === '낮음') {
                        console.log(`  -> ⚠️  [SCOUT-LOW-TRUST ${sAttempt}회] 신뢰도 낮음. 다음 회차로.`);
                        // 낮음이라도 마지막 회차면 사용 (없는 것보단 나음)
                        if (sAttempt === MAX_SCOUT_RETRIES) {
                            factSheet      = scoutText;
                            scoutTrustLevel = 'low';
                            console.log(`  -> ⚠️  [SCOUT-LOW-ACCEPT] 신뢰도 낮음이지만 최후 수단으로 채택.`);
                        }
                        continue;
                    }

                    // 높음 or 보통 → 채택
                    factSheet       = scoutText;
                    scoutTrustLevel = trust === '높음' ? 'high' : 'medium';
                    console.log(`  -> ✅ [SCOUT-OK] 팩트 수집 완료 (신뢰도: ${trust}, ${sAttempt}회차)`);
                    break;

                } catch (scoutErr) {
                    console.log(`  -> ⚠️  [SCOUT-ERR ${sAttempt}회] ${scoutErr.message?.substring(0,60)}`);
                }
            }

            if (scoutAborted) {
                skippedCount++;
                continue;
            }

            if (!factSheet) {
                console.log(`  -> ⚠️  [SCOUT-FAIL] 3회 모두 실패. 팩트 사전 없이 writer 직접 탐색.`);
            }

            // 4-4. 역기획서 초안 생성 (API 오류 최대 3회 재시도)
            let reportText   = '';
            let draftSuccess = false;

            // 1단계: API 오류 재시도 (rate limit, 네트워크 등)
            for (let attempt = 1; attempt <= MAX_DRAFT_RETRIES; attempt++) {
                try {
                    await delay(5000); // 기본 요청 간격 (rate limit 방지)
                    const result = await draftModel.generateContent(buildAnalysisPrompt(game, rank, category, factSheet));
                    reportText   = result.response.text();
                    draftSuccess  = true;
                    break;
                } catch (err) {
                    const msg      = err.message || '';
                    const matched  = msg.match(/retry in (\d+(?:\.\d+)?)s/i);
                    const waitTime = matched ? (Math.ceil(parseFloat(matched[1])) + 2) * 1000 : 15000;
                    console.log(`  -> 🚨 API 에러: ${msg.substring(0, 120).replace(/\n/g, ' ')}`);
                    console.log(`  -> ⏱️  ${waitTime / 1000}초 냉각 후 재시도 (${attempt}/${MAX_DRAFT_RETRIES})...`);
                    await delay(waitTime);
                }
            }

            if (!draftSuccess) {
                const errMsg = `[${rank}위] ${game.title} — Draft 생성 ${MAX_DRAFT_RETRIES}회 실패`;
                console.error(`  -> ❌ ${errMsg}`);
                errorLog.push(errMsg);
                skippedCount++;
                continue;
            }

            // 4-4. [ABORT_NO_DATA] 확인 — 재검색 없이 즉시 스킵
            //      게임 자체의 데이터가 없다는 AI 판단이므로 재검색해도 의미 없음
            if (reportText.includes('[ABORT_NO_DATA]')) {
                console.log(`  -> ⏭️  [AUTO-SKIP] 데이터 부족 게임으로 판단. 스킵합니다.`);
                skippedCount++;
                continue;
            }

            // 4-5. 할루시네이션 감지 → 최대 5회 재검색 (detectHallucination 기준)
            let hallucinationPassed = false;

            for (let hRetry = 0; hRetry <= MAX_HALLUCINATION_RETRIES; hRetry++) {
                const { detected, reason } = detectHallucination(reportText, game.title);

                if (!detected) {
                    // 할루시네이션 없음 → 정상 통과
                    hallucinationPassed = true;
                    if (hRetry > 0) {
                        console.log(`  -> ✅ [재검색 성공] ${hRetry}회차에서 할루시네이션 해소`);
                    }
                    break;
                }

                if (hRetry === MAX_HALLUCINATION_RETRIES) {
                    // 5회 재검색 후에도 해소 불가 → 스킵
                    const errMsg = `[${rank}위] ${game.title} — ${MAX_HALLUCINATION_RETRIES}회 재검색 후에도 할루시네이션 해소 실패: ${reason}`;
                    console.warn(`  -> ⚠️  [HALLUCINATION-SKIP] ${errMsg}`);
                    errorLog.push(errMsg);
                    break;
                }

                // 재검색 실행 (내부 API 에러 시 최대 3회 재시도)
                console.log(`  -> 🔄 [재검색 ${hRetry + 1}/${MAX_HALLUCINATION_RETRIES}] ${reason}`);
                let retrySuccess = false;

                for (let apiRetry = 1; apiRetry <= MAX_DRAFT_RETRIES; apiRetry++) {
                    try {
                        await delay(5000);
                        const retryResult = await draftModel.generateContent(
                            buildRetryPrompt(game, rank, category, reportText, reason, hRetry + 1, factSheet)
                        );
                        reportText   = retryResult.response.text();
                        retrySuccess = true;
                        break;
                    } catch (err) {
                        const msg      = err.message || '';
                        const matched  = msg.match(/retry in (\d+(?:\.\d+)?)s/i);
                        const waitTime = matched ? (Math.ceil(parseFloat(matched[1])) + 2) * 1000 : 15000;
                        console.log(`  -> ⏱️  [재검색 API 에러] ${waitTime / 1000}초 냉각...`);
                        await delay(waitTime);
                    }
                }

                if (!retrySuccess) {
                    const errMsg = `[${rank}위] ${game.title} — 재검색 ${hRetry + 1}회차 API 실패`;
                    console.error(`  -> ❌ ${errMsg}`);
                    errorLog.push(errMsg);
                    break;
                }

                // 재검색 후 [ABORT_NO_DATA] 재확인
                if (reportText.includes('[ABORT_NO_DATA]')) {
                    console.log(`  -> ⏭️  [재검색 중 AUTO-SKIP] 재검색 후에도 데이터 부족.`);
                    break;
                }

                await delay(10000); // 재검색 간 안정화 딜레이
            }

            if (!hallucinationPassed) {
                skippedCount++;
                continue;
            }

            // 4-5. 리포트 텍스트 정제
            // 마크다운 코드 펜스 제거
            reportText = reportText
                .replace(/^```(markdown|md)?/i, '')
                .replace(/```$/i, '')
                .trim();

            // AI가 메타데이터를 중복 출력한 경우 마지막 것만 사용
            const metaOccurrences = [...reportText.matchAll(/메인장르:/g)];
            if (metaOccurrences.length > 1) {
                reportText = reportText.substring(metaOccurrences[metaOccurrences.length - 1].index);
            }

            // 파일명에 사용할 핵심 시스템명 추출
            let coreSystemName    = '시스템_통합_분석';
            const systemNameMatch = reportText.match(/시스템:\s*([^\n]+)/);
            if (systemNameMatch) {
                coreSystemName = systemNameMatch[1]
                    .replace(/\[\/META\]/gi, '')
                    .replace(/[/\\?%*:|"<>]/g, '_')
                    .trim();
            }

            // 메타데이터 줄 제거 (파일 본문에는 불필요)
            reportText = reportText
                .replace(/메인장르:.*?\n/g, '')
                .replace(/서브장르:.*?\n/g, '')
                .replace(/시스템:.*?\n/g,   '')
                .trim();

            // 리포트 상단 헤더 추가
            const header = [
                `# [${rank}위] ${game.title} 역기획서`,
                `> **분석 타겟:** ${category}`,
                `> **핵심 시스템:** ${coreSystemName}`,
                `> **개발사:** ${game.developer}`,
                `> **작성일:** ${dateString}`,
                `> **출시일:** ${releaseDate}`,
                '',
                '---',
                '',
                '',
            ].join('\n');
            reportText = header + reportText;

            // 4-6. Mermaid 블록 처리 (Fast-Track → QA Agent → 플레이스홀더 폴백)
            const { mdText, pdfText, brokenCount } = await processMermaidBlocks(reportText, qaModel);
            if (brokenCount > 0) diagramBrokenCount++;

            // 4-7. 파일명 생성
            const safeTitle    = game.title.replace(/[/\\?%*:|"<>]/g, '_');
            const baseFileName = `[${dateString}]_${String(rank).padStart(3, '0')}위_${safeTitle}_(${coreSystemName})`;

            let mdSaved = false, pdfSaved = false, htmlSaved = false;

            // 4-8. MD 저장
            try {
                if (!mdText || mdText.length < 10) throw new Error('MD 데이터가 비어있습니다.');
                mdSaved = await uploadToDrive({
                    fileName: `${baseFileName}.md`,
                    folderId: mdFolderId,
                    mimeType: 'text/markdown',
                    content:  mdText,
                });
                if (mdSaved) console.log(`  -> 💾 [MD]   저장 완료`);
            } catch (e) {
                console.error(`  -> ❌ [MD]   저장 실패: ${e.message}`);
                errorLog.push(`[MD]   ${baseFileName}: ${e.message}`);
            }

            // 4-9. PDF 저장
            try {
                console.log(`  -> 📄 [PDF]  변환 시작...`);
                const pdfData = await mdToPdf({ content: pdfText }, PDF_OPTIONS);
                if (!pdfData?.content) throw new Error('PDF 엔진이 빈 데이터를 반환했습니다.');
                pdfSaved = await uploadToDrive({
                    fileName: `${baseFileName}.pdf`,
                    folderId: pdfFolderId,
                    mimeType: 'application/pdf',
                    content:  pdfData.content,
                });
                if (pdfSaved) console.log(`  -> 💾 [PDF]  저장 완료`);
            } catch (e) {
                console.error(`  -> ❌ [PDF]  저장 실패: ${e.message}`);
                errorLog.push(`[PDF]  ${baseFileName}: ${e.message}`);
            }

            // 4-10. HTML 저장
            try {
                console.log(`  -> 🌐 [HTML] 변환 시작...`);
                const parsedBody = marked.parse(pdfText);
                if (!parsedBody?.trim()) throw new Error('HTML 파싱 결과가 비어있습니다.');
                const fullHtml = buildHtmlReport(game.title, parsedBody);
                htmlSaved = await uploadToDrive({
                    fileName: `${baseFileName}.html`,
                    folderId: htmlFolderId,
                    mimeType: 'text/html',
                    content:  fullHtml,
                });
                if (htmlSaved) console.log(`  -> 💾 [HTML] 저장 완료`);
            } catch (e) {
                console.error(`  -> ❌ [HTML] 저장 실패: ${e.message}`);
                errorLog.push(`[HTML] ${baseFileName}: ${e.message}`);
            }

            // 4-11. 저장 결과 집계
            const savedFormats = [mdSaved, pdfSaved, htmlSaved].filter(Boolean).length;
            if (savedFormats === 3) {
                fullSuccessCount++;
            } else if (savedFormats >= 1) {
                partialSuccessCount++;
                console.log(`  -> ⚠️  일부 포맷 저장 실패 (MD:${mdSaved} PDF:${pdfSaved} HTML:${htmlSaved})`);
            } else {
                console.error(`  -> ❌ 모든 포맷 저장 실패`);
            }

            // 다음 게임 처리 전 30초 대기 (Drive API / Gemini rate limit 방지)
            if (idx < targetGames.length - 1) await delay(30000);
        }

        // ── 5. 최종 결산 (에러/검수 로그는 GitHub Actions 콘솔에서 확인)

        // ── 5. 최종 결산 출력 ────────────────────────────────────────────────
        const failedCount = targetGames.length - fullSuccessCount - partialSuccessCount - skippedCount;
        console.log(`\n${'='.repeat(56)}`);
        console.log(`[${dateString}] 📊 최종 결산`);
        console.log(`  목표 처리량              ${targetGames.length}개`);
        console.log(`  완전 성공 (3포맷 모두)   ${fullSuccessCount}개`);
        console.log(`  부분 성공 (1~2포맷)      ${partialSuccessCount}개`);
        console.log(`  다이어그램 일부 깨짐      ${diagramBrokenCount}개  ← 리포트는 저장됨`);
        console.log(`  자동 스킵 (데이터 부족)  ${skippedCount}개`);
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
