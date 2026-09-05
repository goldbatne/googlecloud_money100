const { google } = require('googleapis');

function requireEnv(name) {
    const value = process.env[name];

    if (!value) {
        throw new Error(
            `필수 시크릿 ${name} 이(가) 설정되지 않았습니다.`
        );
    }

    return value;
}

const oauth2Client = new google.auth.OAuth2(
    requireEnv('GCP_CLIENT_ID'),
    requireEnv('GCP_CLIENT_SECRET'),
    'https://developers.google.com/oauthplayground'
);

oauth2Client.setCredentials({
    refresh_token: requireEnv('GCP_REFRESH_TOKEN')
});

const drive = google.drive({
    version: 'v3',
    auth: oauth2Client
});

const SOURCE_ROOT_ID = requireEnv(
    'GDRIVE_FOLDER_ID'
);

function getKstDateStrings() {
    const parts = new Intl.DateTimeFormat(
        'en-US',
        {
            timeZone: 'Asia/Seoul',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit'
        }
    ).formatToParts(new Date());

    const get = type =>
        parts.find(
            part => part.type === type
        ).value;

    return {
        yearStr: `${get('year')}년`,
        monthStr: `${get('month')}월`,
        dayStr: `${get('day')}일`
    };
}

function escapeQueryValue(value) {
    return String(value)
        .replace(/\\/g, '\\\\')
        .replace(/'/g, "\\'");
}

async function getFolderIdByNameAndParent(
    folderName,
    parentId
) {
    const safeName =
        escapeQueryValue(folderName);

    const safeParent =
        escapeQueryValue(parentId);

    const res = await drive.files.list({
        q:
            `name = '${safeName}' ` +
            `and '${safeParent}' in parents ` +
            `and mimeType = 'application/vnd.google-apps.folder' ` +
            `and trashed = false`,
        fields: 'files(id, name)',
        pageSize: 10
    });

    return res.data.files[0]?.id || null;
}

async function findTodayFolder(
    format,
    yearStr,
    monthStr,
    dayStr
) {
    const yearId =
        await getFolderIdByNameAndParent(
            yearStr,
            SOURCE_ROOT_ID
        );

    if (!yearId) {
        throw new Error(
            `원본 연도 폴더 없음: ${yearStr}`
        );
    }

    const formatName =
        `${yearStr}_${format}`;

    const formatId =
        await getFolderIdByNameAndParent(
            formatName,
            yearId
        );

    if (!formatId) {
        throw new Error(
            `원본 포맷 폴더 없음: ${formatName}`
        );
    }

    const monthName =
        `${monthStr}_${format}`;

    const monthId =
        await getFolderIdByNameAndParent(
            monthName,
            formatId
        );

    if (!monthId) {
        throw new Error(
            `원본 월 폴더 없음: ${monthName}`
        );
    }

    const dayName =
        `${dayStr}_${format}`;

    const dayId =
        await getFolderIdByNameAndParent(
            dayName,
            monthId
        );

    return {
        format,
        dayName,
        dayId
    };
}

async function main() {
    console.log(
        '🧹 복사 완료 후 오늘자 원본 MD/PDF/HTML 영구 삭제 시작'
    );

    const {
        yearStr,
        monthStr,
        dayStr
    } = getKstDateStrings();

    const formats = [
        'md',
        'pdf',
        'html'
    ];

    try {
        /*
         * 삭제 전에 MD/PDF/HTML 경로를
         * 먼저 전부 확인한다.
         */
        const folders = [];

        for (const format of formats) {
            const folder =
                await findTodayFolder(
                    format,
                    yearStr,
                    monthStr,
                    dayStr
                );

            if (!folder.dayId) {
                console.log(
                    `  -> ℹ️ 이미 삭제되었거나 없는 폴더: ${folder.dayName}`
                );

                continue;
            }

            folders.push(folder);
        }

        if (folders.length === 0) {
            console.log(
                'ℹ️ 삭제할 오늘자 원본 폴더가 없습니다.'
            );

            return;
        }

        /*
         * files.delete는 휴지통 이동이 아니라
         * 영구 삭제.
         *
         * copy.js와 copy_public.js가 모두
         * 성공한 경우에만 이 코드가 실행된다.
         */
        for (const folder of folders) {
            await drive.files.delete({
                fileId: folder.dayId
            });

            console.log(
                `  -> 🗑️ 영구 삭제 완료: ${folder.dayName}`
            );
        }

        console.log(
            '\n✅ 오늘자 원본 MD/PDF/HTML 정리가 완료되었습니다.'
        );
    } catch (err) {
        console.error(
            '\n❌ 원본 정리 실패:',
            err.message || err
        );

        process.exit(1);
    }
}

main();
