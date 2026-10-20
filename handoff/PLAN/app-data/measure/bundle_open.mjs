import fs from "node:fs"; import { createRequire } from "node:module";
const require = createRequire("/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/app/package.json");
const SQL = await require("sql.js")(); const t=performance.now();
const db = new SQL.Database(fs.readFileSync(new URL("./b.sqlite", import.meta.url).pathname)); db.exec("select count(*) from sqlite_master");
console.log("wasm open whole bundle ms", (performance.now()-t).toFixed(0), "heap MB", (process.memoryUsage().rss/1e6).toFixed(0));
const t2=performance.now(); const b2 = new SQL.Database(fs.readFileSync(new URL("./pkg/core_pk_idx.sqlite", import.meta.url).pathname)); b2.exec("select 1"); console.log("wasm open core_pk_idx ms",(performance.now()-t2).toFixed(1));
