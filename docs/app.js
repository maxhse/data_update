(() => {
  const elMeta = document.getElementById('meta');
  const elWrap = document.getElementById('tableWrap');
  const elQ = document.getElementById('q');
  const elDownload = document.getElementById('download');

  function escapeHtml(s) {
    return String(s)
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#039;');
  }

  function isNumericLike(v) {
    return /^[\d,.-]+$/.test(v);
  }

  function renderTable(fields, rows) {
    const thead = `<thead><tr>${fields.map(f => `<th>${escapeHtml(f)}</th>`).join('')}</tr></thead>`;
    const tbody = `<tbody>${rows.map(r => {
      const tds = r.map(v => {
        const cls = isNumericLike(v) ? ' class="right"' : '';
        return `<td${cls}>${escapeHtml(v)}</td>`;
      }).join('');
      return `<tr>${tds}</tr>`;
    }).join('')}</tbody>`;

    elWrap.innerHTML = `<table id="t">${thead}${tbody}</table>`;
  }

  function bindSearch() {
    const table = document.getElementById('t');
    if (!table) return;
    const rows = Array.from(table.querySelectorAll('tbody tr'));

    const onInput = () => {
      const q = (elQ.value || '').trim().toLowerCase();
      if (!q) {
        rows.forEach(tr => tr.style.display = '');
        return;
      }
      rows.forEach(tr => {
        const text = tr.innerText.toLowerCase();
        tr.style.display = text.includes(q) ? '' : 'none';
      });
    };

    elQ.addEventListener('input', onInput);
  }

  async function main() {
    try {
      const cacheBust = Date.now();
      const res = await fetch(`data/latest.json?cb=${cacheBust}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();

      const base = data.base_date;
      const trading = data.trading_dates || [];
      const labels = data.labels || [];

      const dateLine = labels.map((lb, i) => `${lb}=${trading[i] ?? ''}`).join('、');
      elMeta.textContent = `基準日（台北）：${base}｜本次 D0~D-4 交易日：${dateLine}｜最後產生：${data.generated_at}`;

      const csvFile = data.csv?.file;
      elDownload.href = csvFile ? `data/${csvFile}` : '#';
      elDownload.download = csvFile || 'latest.csv';

      renderTable(data.fields, data.rows);
      bindSearch();
    } catch (e) {
      elMeta.textContent = '載入失敗';
      elWrap.innerHTML = `<div class="error">讀取 data/latest.json 失敗：${escapeHtml(e.message || e)}</div>`;
    }
  }

  main();
})();