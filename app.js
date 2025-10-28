const tbody = document.querySelector("#tbl tbody");
const statusSpan = document.getElementById("status");
const windowInput = document.getElementById("windowSec");
const sortKeySel = document.getElementById("sortKey");
const sortDirSel = document.getElementById("sortDir");
document.getElementById("apply").addEventListener("click", loadOnce);

let timer = null;
function start() {
  if (timer) clearInterval(timer);
  timer = setInterval(loadOnce, 1000);
  loadOnce();
}
function fmt(x, d=6) {
  if (x === null || x === undefined) return "";
  return Number(x).toFixed(d);
}
function loadOnce() {
  const w = parseInt(windowInput.value || "300", 10);
  fetch(`/api/data?window=${w}`)
    .then(r => r.json())
    .then(j => {
      if (!j.ok) {
        statusSpan.textContent = `错误：${j.error || "unknown"}`;
        return;
      }
      const items = j.items || [];
      // 前端再排序一遍以匹配用户选项
      const key = sortKeySel.value;
      const dir = sortDirSel.value;
      items.sort((a,b) => {
        let va, vb;
        if (key === "abs_spread") { va = Math.abs(a.spread_pct); vb = Math.abs(b.spread_pct); }
        else if (key === "spread") { va = a.spread_pct; vb = b.spread_pct; }
        else if (key === "avg") { va = a.avg_spread_pct ?? -1e9; vb = b.avg_spread_pct ?? -1e9; }
        else if (key === "z") { va = (a.zscore ?? -1e9); vb = (b.zscore ?? -1e9); }
        else if (key === "funding") { va = (a.funding_avg ?? -1e9); vb = (b.funding_avg ?? -1e9); }
        else { va = 0; vb = 0; }
        return dir === "asc" ? (va - vb) : (vb - va);
      });

      tbody.innerHTML = "";
      for (const it of items) {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${it.symbol}</td>
          <td>${it.exA}</td>
          <td>${it.exB}</td>
          <td>${fmt(it.midA, 8)}</td>
          <td>${fmt(it.midB, 8)}</td>
          <td class="${it.spread_pct>0?'pos':'neg'}">${fmt(it.spread_pct, 4)}</td>
          <td>${it.avg_spread_pct==null?"":fmt(it.avg_spread_pct,4)}</td>
          <td>${it.zscore==null?"":fmt(it.zscore,3)}</td>
          <td>${it.fundingA==null?"":fmt(it.fundingA,6)}</td>
          <td>${it.fundingB==null?"":fmt(it.fundingB,6)}</td>
          <td>${it.funding_avg==null?"":fmt(it.funding_avg,6)}</td>
          <td>${it.samples}</td>
        `;
        tbody.appendChild(tr);
      }
      statusSpan.textContent = `更新于 ${j.updated} · 共 ${items.length} 条`;
    })
    .catch(e => {
      statusSpan.textContent = `请求失败：${e}`;
    });
}
start();
