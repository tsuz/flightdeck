/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Override the WebSocket endpoint. Defaults to ws(s)://<page-host>/ws. */
  readonly VITE_WS_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

interface Window {
  /** Runtime config injected by /env.js (templated from the WS_URL env var in Docker). */
  __ENV__?: { WS_URL?: string };
}
