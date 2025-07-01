// Jest セットアップファイル
// グローバルな設定やモックを定義

// console.errorをモック（テスト中のエラー出力を抑制）
global.console = {
  ...console,
  error: jest.fn(),
  warn: jest.fn()
};

// LocalStorageのモック
const localStorageMock = {
  getItem: jest.fn(),
  setItem: jest.fn(),
  removeItem: jest.fn(),
  clear: jest.fn(),
};
global.localStorage = localStorageMock;

// scrollToのモック
Element.prototype.scrollTo = jest.fn();
