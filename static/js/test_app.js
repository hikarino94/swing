/**
 * フロントエンドJavaScriptのテスト
 * Jest環境で実行することを想定
 */

// Alpine.jsのモック
global.Alpine = {
    data: jest.fn(),
    store: jest.fn()
};

// Toastifyのモック
global.Toastify = jest.fn(() => ({
    showToast: jest.fn()
}));

// fetchのモック
global.fetch = jest.fn();

// APIヘルパー関数のテスト
describe('apiCall', () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    test('成功時の処理', async () => {
        const mockResponse = {
            success: true,
            output: 'テスト成功',
            description: 'テスト実行'
        };

        fetch.mockResolvedValueOnce({
            json: async () => mockResponse
        });

        const result = await apiCall('/test', { data: 'test' });

        expect(fetch).toHaveBeenCalledWith('/api/test', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ data: 'test' })
        });

        expect(result).toEqual(mockResponse);
        expect(Toastify).toHaveBeenCalledWith(
            expect.objectContaining({
                text: 'テスト実行 - 完了',
                backgroundColor: '#10b981'
            })
        );
    });

    test('エラー時の処理', async () => {
        const mockResponse = {
            success: false,
            error: 'エラーが発生しました',
            description: 'テスト実行'
        };

        fetch.mockResolvedValueOnce({
            json: async () => mockResponse
        });

        const result = await apiCall('/test', {});

        expect(result).toEqual(mockResponse);
        expect(Toastify).toHaveBeenCalledWith(
            expect.objectContaining({
                text: 'テスト実行 - エラー: エラーが発生しました',
                backgroundColor: '#ef4444'
            })
        );
    });

    test('ネットワークエラー時の処理', async () => {
        fetch.mockRejectedValueOnce(new Error('Network error'));

        const result = await apiCall('/test', {});

        expect(result).toEqual({
            success: false,
            error: 'Network error'
        });
        expect(Toastify).toHaveBeenCalledWith(
            expect.objectContaining({
                text: 'API呼び出しエラー: Network error',
                backgroundColor: '#ef4444'
            })
        );
    });
});

// ログ表示関数のテスト
describe('appendLog', () => {
    let logElement;

    beforeEach(() => {
        logElement = document.createElement('div');
        document.body.appendChild(logElement);
    });

    afterEach(() => {
        document.body.removeChild(logElement);
    });

    test('通常ログの追加', () => {
        appendLog(logElement, 'テストメッセージ');

        const logLines = logElement.querySelectorAll('div');
        expect(logLines.length).toBe(1);
        expect(logLines[0].className).toBe('text-gray-700');
        expect(logLines[0].textContent).toMatch(/\[.*\] テストメッセージ/);
    });

    test('エラーログの追加', () => {
        appendLog(logElement, 'エラーメッセージ', true);

        const logLines = logElement.querySelectorAll('div');
        expect(logLines.length).toBe(1);
        expect(logLines[0].className).toBe('text-red-600');
    });

    test('スクロール位置の自動調整', () => {
        // 複数のログを追加
        for (let i = 0; i < 10; i++) {
            appendLog(logElement, `ログ ${i}`);
        }

        // scrollTopがscrollHeightに設定されることを確認
        expect(logElement.scrollTop).toBe(logElement.scrollHeight);
    });
});

// ファイルサイズフォーマット関数のテスト
describe('formatFileSize', () => {
    test('バイト単位', () => {
        expect(formatFileSize(500)).toBe('500 B');
    });

    test('キロバイト単位', () => {
        expect(formatFileSize(1500)).toBe('1.5 KB');
    });

    test('メガバイト単位', () => {
        expect(formatFileSize(1500000)).toBe('1.4 MB');
    });
});

// 日付フォーマット関数のテスト
describe('formatDate', () => {
    test('ISO文字列の変換', () => {
        const isoString = '2024-01-01T12:00:00';
        const result = formatDate(isoString);
        expect(result).toMatch(/2024/);
        expect(result).toMatch(/12:00/);
    });
});

// Alpine.jsコンポーネントのテスト
describe('swingApp', () => {
    let app;

    beforeEach(() => {
        app = swingApp();
    });

    test('初期状態', () => {
        expect(app.activeTab).toBe('data');
        expect(app.isLoading).toBe(false);
        expect(app.dataFetch.quotes.enabled).toBe(false);
        expect(app.screening.type).toBe('fundamental');
        expect(app.backtest.type).toBe('fundamental');
    });

    test('データ取得の実行', async () => {
        const mockApiCall = jest.fn().mockResolvedValue({
            success: true,
            output: '取得完了'
        });
        global.apiCall = mockApiCall;

        // 株価取得を有効化
        app.dataFetch.quotes.enabled = true;
        app.dataFetch.quotes.startDate = '2024-01-01';
        app.dataFetch.quotes.endDate = '2024-01-31';

        await app.executeDataFetch();

        expect(mockApiCall).toHaveBeenCalledWith('/fetch/quotes', {
            start_date: '2024-01-01',
            end_date: '2024-01-31'
        });
    });

    test('スクリーニングタイプの切り替え', () => {
        app.screening.type = 'technical';
        expect(app.screening.type).toBe('technical');

        app.screening.type = 'ml';
        expect(app.screening.type).toBe('ml');
    });

    test('ローディング状態の管理', async () => {
        const mockApiCall = jest.fn().mockImplementation(() => {
            return new Promise(resolve => {
                setTimeout(() => resolve({ success: true }), 100);
            });
        });
        global.apiCall = mockApiCall;

        const promise = app.executeScreening();
        expect(app.isLoading).toBe(true);

        await promise;
        expect(app.isLoading).toBe(false);
    });

    test('閾値の読み込みと保存', async () => {
        const mockThresholds = {
            per_min: 5,
            per_max: 20
        };

        const mockApiCall = jest.fn()
            .mockResolvedValueOnce({ success: true, data: mockThresholds })
            .mockResolvedValueOnce({ success: true });

        global.apiCall = mockApiCall;

        // 読み込み
        await app.loadThresholds();
        expect(app.settings.thresholds).toBe(JSON.stringify(mockThresholds, null, 2));

        // 保存
        await app.saveThresholds();
        expect(mockApiCall).toHaveBeenLastCalledWith('/utils/thresholds', mockThresholds);
    });

    test('結果ファイルの一覧取得', async () => {
        const mockFiles = [
            { name: 'result1.xlsx', size: 1024, modified: '2024-01-01T12:00:00' },
            { name: 'result2.json', size: 2048, modified: '2024-01-02T12:00:00' }
        ];

        fetch.mockResolvedValueOnce({
            json: async () => ({ success: true, files: mockFiles })
        });

        await app.loadResultFiles();
        expect(app.resultFiles).toEqual(mockFiles);
    });
});

// タイマー関数のテスト
describe('updateTime', () => {
    let timeElement;

    beforeEach(() => {
        timeElement = document.createElement('span');
        timeElement.id = 'currentTime';
        document.body.appendChild(timeElement);
        jest.useFakeTimers();
    });

    afterEach(() => {
        document.body.removeChild(timeElement);
        jest.useRealTimers();
    });

    test('時刻の更新', () => {
        updateTime();
        const initialTime = timeElement.textContent;
        expect(initialTime).toMatch(/\d{4}/); // 年が含まれる

        // 1秒後
        jest.advanceTimersByTime(1000);
        updateTime();
        const updatedTime = timeElement.textContent;
        expect(updatedTime).toBeTruthy();
    });
});

// 実際に定義する必要のある関数（テスト対象）
function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function formatDate(isoString) {
    return new Date(isoString).toLocaleString('ja-JP');
}

function updateTime() {
    const now = new Date();
    const timeString = now.toLocaleDateString('ja-JP') + ' ' + now.toLocaleTimeString('ja-JP');
    const element = document.getElementById('currentTime');
    if (element) {
        element.textContent = timeString;
    }
}

function appendLog(logElement, text, isError = false) {
    const timestamp = new Date().toLocaleTimeString('ja-JP');
    const line = document.createElement('div');
    line.className = isError ? 'text-red-600' : 'text-gray-700';
    line.textContent = `[${timestamp}] ${text}`;
    logElement.appendChild(line);
    logElement.scrollTop = logElement.scrollHeight;
}

async function apiCall(endpoint, data = {}, method = 'POST') {
    try {
        const options = {
            method: method,
            headers: {
                'Content-Type': 'application/json',
            }
        };

        if (method !== 'GET') {
            options.body = JSON.stringify(data);
        }

        const response = await fetch(`/api${endpoint}`, options);
        const result = await response.json();

        if (result.success) {
            showNotification('success', result.description + ' - 完了');
        } else {
            showNotification('error', result.description + ' - エラー: ' + result.error);
        }

        return result;
    } catch (error) {
        showNotification('error', 'API呼び出しエラー: ' + error.message);
        return { success: false, error: error.message };
    }
}

function showNotification(type, message) {
    const bgColor = type === 'success' ? '#10b981' : '#ef4444';

    Toastify({
        text: message,
        duration: 3000,
        gravity: "top",
        position: "right",
        backgroundColor: bgColor,
        stopOnFocus: true
    }).showToast();
}

// Alpine.jsコンポーネントの定義（簡略版）
function swingApp() {
    return {
        activeTab: 'data',
        isLoading: false,
        dataFetch: {
            quotes: {
                enabled: false,
                startDate: '',
                endDate: ''
            },
            listed: {
                enabled: false
            },
            statements: {
                enabled: false,
                mode: '2',
                startDate: '',
                endDate: ''
            }
        },
        screening: {
            type: 'fundamental',
            fundamental: { lookback: 60, recent: 30, asOf: '' },
            technical: { action: 'screen', asOf: '', lookback: 100 },
            ml: { action: 'screen', force: false, top: 10, lookback: 250 }
        },
        backtest: {
            type: 'fundamental',
            common: { capital: 1000000, holdDays: 20, startDate: '', endDate: '' },
            fundamental: { entryOffset: 1 },
            technical: { stopLoss: 5.0 },
            ml: { top: 10 }
        },
        settings: {
            thresholds: ''
        },
        resultFiles: [],

        async executeDataFetch() {
            this.isLoading = true;
            if (this.dataFetch.quotes.enabled) {
                await apiCall('/fetch/quotes', {
                    start_date: this.dataFetch.quotes.startDate,
                    end_date: this.dataFetch.quotes.endDate
                });
            }
            this.isLoading = false;
        },

        async executeScreening() {
            this.isLoading = true;
            // 実装省略
            this.isLoading = false;
        },

        async loadThresholds() {
            const result = await apiCall('/utils/thresholds', {}, 'GET');
            if (result.success) {
                this.settings.thresholds = JSON.stringify(result.data, null, 2);
            }
        },

        async saveThresholds() {
            try {
                const data = JSON.parse(this.settings.thresholds);
                await apiCall('/utils/thresholds', data);
            } catch (e) {
                showNotification('error', 'JSON形式が不正です: ' + e.message);
            }
        },

        async loadResultFiles() {
            const response = await fetch('/api/results/list');
            const data = await response.json();
            if (data.success) {
                this.resultFiles = data.files;
            }
        }
    };
}
