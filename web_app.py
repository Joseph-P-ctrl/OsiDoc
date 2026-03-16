from __future__ import annotations

import html
import os
import sqlite3
import subprocess
import threading
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

app = FastAPI(title="OsiDOc Viewer", version="1.0.0")

WORKSPACE_DIR = Path(__file__).resolve().parent
_DOWNLOADS_ENV = os.getenv("OSI_DOWNLOAD_DIR", "downloads")
DOWNLOADS_DIR = Path(_DOWNLOADS_ENV)
if not DOWNLOADS_DIR.is_absolute():
    DOWNLOADS_DIR = (WORKSPACE_DIR / DOWNLOADS_DIR).resolve()

_DB_ENV = os.getenv("OSI_SQLITE_PATH", str(DOWNLOADS_DIR / "notificaciones.db"))
DB_PATH = Path(_DB_ENV)
if not DB_PATH.is_absolute():
    DB_PATH = (WORKSPACE_DIR / DB_PATH).resolve()

PAGE_SIZE_DEFAULT = int(os.getenv("OSI_WEB_PAGE_SIZE", "10"))
COLUMNS_TO_DISPLAY = [
    "nro__notificacion",
    "asunto",
    "fecha_de_notificacion",
    "fecha_importacion",
]

UPDATE_STATE = {"running": False, "progress": 0, "error": None, "message": ""}


def _run_update():
    """Ejecuta osinergmin_auth.py en thread separado."""
    try:
        UPDATE_STATE["running"] = True
        UPDATE_STATE["error"] = None
        UPDATE_STATE["message"] = "Iniciando actualización incremental..."
        UPDATE_STATE["progress"] = 10

        script_path = WORKSPACE_DIR / "osinergmin_auth.py"
        result = subprocess.run(
            [
                str(WORKSPACE_DIR / ".venv" / "Scripts" / "python.exe"),
                str(script_path),
                "--incremental-only",
                "--skip-existing-notifications",
            ],
            cwd=str(WORKSPACE_DIR),
            capture_output=True,
            text=True,
            timeout=600,
        )

        combined_output = (result.stdout or "") + "\n" + (result.stderr or "")

        if result.returncode == 0:
            UPDATE_STATE["progress"] = 100
            if "No hay notificaciones nuevas o pendientes por descargar." in combined_output:
                UPDATE_STATE["message"] = "No hay nada nuevo para descargar."
            elif "No se descargaron documentos notificados." in combined_output:
                UPDATE_STATE["message"] = "No hubo documentos nuevos para descargar."
            else:
                UPDATE_STATE["message"] = "Actualización completada."
        else:
            UPDATE_STATE["error"] = f"Proceso finalizado con código {result.returncode}"
            UPDATE_STATE["message"] = "La actualización terminó con error."
    except Exception as e:
        UPDATE_STATE["error"] = str(e)
        UPDATE_STATE["progress"] = 0
        UPDATE_STATE["message"] = "Error durante la actualización."
    finally:
        UPDATE_STATE["running"] = False


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    return con


def _get_table_columns(con: sqlite3.Connection) -> list[str]:
    rows = con.execute("PRAGMA table_info(notificaciones)").fetchall()
    return [str(r["name"]) for r in rows]


def _find_notification_column(columns: list[str]) -> str | None:
    for col in columns:
        if "notif" in col.lower():
            return col
    return None


def _html_page(title: str, body: str) -> HTMLResponse:
    page = f"""<!doctype html>
<html lang=\"es\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      --bg: #f4f6f8;
      --card: #ffffff;
      --ink: #13212f;
      --muted: #5d6b78;
      --brand: #0f5aa5;
      --line: #d9e1e8;
      --accent: #f3f8fe;
      --success: #27ae60;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
      color: var(--ink);
      background: radial-gradient(circle at top right, #deebf9 0, var(--bg) 45%);
    }}
    .wrap {{ max-width: 1380px; margin: 0 auto; padding: 20px; }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      box-shadow: 0 8px 24px rgba(10, 28, 46, 0.06);
      overflow: hidden;
    }}
    .head {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 20px;
      background: linear-gradient(110deg, #0f5aa5, #2f7ec6);
      color: #fff;
      padding: 16px 20px;
      flex-wrap: wrap;
    }}
    .head > div:first-child {{ flex: 1; }}
    .head h1 {{ margin: 0; font-size: 20px; font-weight: 600; }}
    .meta {{ font-size: 12px; opacity: 0.9; margin-top: 4px; }}
    .content {{ padding: 18px; }}
    .toolbar {{ display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 14px; align-items: center; }}
    input[type=text] {{
      min-width: 260px;
      flex: 1;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px 12px;
      font-size: 14px;
      transition: all 0.2s;
    }}
    input[type=text]:focus {{
      outline: none;
      border-color: var(--brand);
      box-shadow: 0 0 0 3px rgba(15, 90, 165, 0.1);
    }}
    button, .btn {{
      border: 0;
      border-radius: 8px;
      background: var(--brand);
      color: #fff;
      padding: 10px 14px;
      text-decoration: none;
      font-size: 14px;
      font-weight: 500;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: all 0.2s ease;
      white-space: nowrap;
    }}
    button:hover, .btn:hover {{
      background: #0d4a8f;
      transform: translateY(-1px);
      box-shadow: 0 4px 12px rgba(15, 90, 165, 0.25);
    }}
    button:active {{ transform: translateY(0); }}
    button:disabled {{
      opacity: 0.5;
      cursor: not-allowed;
      transform: none;
    }}
    .btn.secondary {{ background: #4e6479; }}
    .btn.secondary:hover {{ background: #3d545f; }}
    .btn.refresh {{
      background: linear-gradient(110deg, #27ae60, #2ecc71);
      padding: 10px 16px;
      font-weight: 600;
    }}
    .btn.refresh:hover {{ background: linear-gradient(110deg, #229954, #27ae60); }}
    .spinner {{
      display: inline-block;
      width: 14px;
      height: 14px;
      border: 2px solid rgba(255,255,255,0.3);
      border-top-color: #fff;
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
    }}
    @keyframes spin {{
      to {{ transform: rotate(360deg); }}
    }}
    .modal {{
      display: none;
      position: fixed;
      top: 0; left: 0; right: 0; bottom: 0;
      background: rgba(0,0,0,0.5);
      z-index: 1000;
      align-items: center;
      justify-content: center;
      backdrop-filter: blur(4px);
    }}
    .modal.active {{ display: flex; }}
    .modal-content {{
      background: var(--card);
      border-radius: 16px;
      padding: 40px;
      text-align: center;
      box-shadow: 0 20px 60px rgba(0,0,0,0.3);
      max-width: 420px;
      animation: slideUp 0.3s ease;
    }}
    @keyframes slideUp {{
      from {{ opacity: 0; transform: translateY(20px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}
    .modal-content h2 {{ margin: 0 0 12px; color: var(--ink); font-size: 20px; }}
    .modal-spinner {{
      width: 52px; height: 52px;
      margin: 0 auto 24px;
      border: 3px solid var(--line);
      border-top-color: var(--brand);
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
    }}
    .modal-text {{ color: var(--muted); font-size: 14px; line-height: 1.5; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid var(--line); padding: 10px; text-align: left; vertical-align: top; }}
    th {{ background: var(--accent); position: sticky; top: 0; font-weight: 600; color: var(--brand); }}
    .table-wrap {{ max-height: 70vh; overflow: auto; border: 1px solid var(--line); border-radius: 10px; }}
    tr:hover {{ background: #fafbfc; }}
    .muted {{ color: var(--muted); font-size: 13px; }}
    .badge {{
      background: #eaf3ff;
      color: #0f5aa5;
      border: 1px solid #cde0f7;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 500;
      padding: 3px 9px;
      white-space: nowrap;
      display: inline-block;
    }}
    ul.file-list {{ margin: 12px 0 0; padding-left: 18px; }}
    .file-list li {{ margin: 8px 0; }}
    .file-list a {{ color: var(--brand); text-decoration: none; font-weight: 500; }}
    .file-list a:hover {{ text-decoration: underline; }}
    .info-box {{
      background: #eaf3ff;
      border-left: 4px solid var(--brand);
      border-radius: 6px;
      padding: 12px 14px;
      margin-bottom: 12px;
      font-size: 13px;
      color: #0d4a8f;
    }}
    .accordion-row {{ display: none; background: #f8fbff; }}
    .accordion-row.open {{ display: table-row; }}
    .accordion-cell {{ padding: 14px 12px !important; }}
    .accordion-box {{
      border: 1px solid #d7e6f7;
      border-radius: 8px;
      background: #fff;
      padding: 10px 12px;
    }}
    .docs-empty {{ color: var(--muted); font-size: 13px; }}
    .docs-list {{ margin: 0; padding-left: 18px; }}
    .docs-list li {{ margin: 6px 0; }}
  </style>
</head>
<body>
  <div class=\"wrap\">{body}</div>
  <div id=\"updateModal\" class=\"modal\">
    <div class=\"modal-content\">
      <h2>Actualizando notificaciones...</h2>
      <div class=\"modal-spinner\"></div>
      <p class=\"modal-text\" id=\"updateProgress\">Conectando con el servidor...</p>
    </div>
  </div>
  <script>
    async function abrirActualizacion() {{
      const modal = document.getElementById('updateModal');
      const btn = event.target.closest('button');
      btn.disabled = true;
      modal.classList.add('active');
      
      try {{
        const resp = await fetch('/api/actualizar', {{ method: 'POST' }});
        const data = await resp.json();
        
        if(data.success) {{
          const checkInterval = setInterval(async () => {{
            const status = await fetch('/api/estado').then(r => r.json());
            document.getElementById('updateProgress').textContent = `Descargando... ${{Math.min(status.progress || 0, 99)}}%`;
            
            if(!status.running) {{
              clearInterval(checkInterval);
              document.getElementById('updateProgress').textContent = status.message || '¡Completado! Recargando...';
              setTimeout(() => {{
                modal.classList.remove('active');
                btn.disabled = false;
                location.reload();
              }}, 1500);
            }}
          }}, 800);
        }} else {{
          alert('Error: ' + (data.error || 'Desconocido'));
          modal.classList.remove('active');
          btn.disabled = false;
        }}
      }} catch(err) {{
        alert('Error de conexión: ' + err);
        modal.classList.remove('active');
        btn.disabled = false;
      }}
    }}

    async function toggleDocs(btn, numero, rowId) {{
      const row = document.getElementById(`docs-row-${{rowId}}`);
      const body = document.getElementById(`docs-body-${{rowId}}`);
      if (!row || !body) return;

      if (row.classList.contains('open')) {{
        row.classList.remove('open');
        btn.textContent = 'Ver documentos';
        return;
      }}

      row.classList.add('open');
      btn.textContent = 'Ocultar documentos';

      if (body.dataset.loaded === '1') return;
      body.innerHTML = '<div class="docs-empty">Cargando documentos...</div>';

      try {{
        const resp = await fetch(`/api/notificaciones/${{encodeURIComponent(numero)}}/documentos`);
        const data = await resp.json();
        if (!data || !Array.isArray(data.files) || data.files.length === 0) {{
          body.innerHTML = '<div class="docs-empty">No hay documentos para esta notificación.</div>';
          body.dataset.loaded = '1';
          return;
        }}

        const items = data.files.map((f) => (
          `<li><a href="${{f.href}}" target="_blank"><strong>${{f.name}}</strong></a> ` +
          `<span class="muted">(${{f.date_folder}} | ${{f.size_kb}} KB)</span></li>`
        )).join('');
        body.innerHTML = `<ul class="docs-list">${{items}}</ul>`;
        body.dataset.loaded = '1';
      }} catch (err) {{
        body.innerHTML = '<div class="docs-empty">No se pudo cargar el detalle de documentos.</div>';
      }}
    }}
  </script>
</body>
</html>"""
    return HTMLResponse(content=page)


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "db": str(DB_PATH),
        "downloads": str(DOWNLOADS_DIR),
    }


@app.post("/api/actualizar")
def actualizar():
    """Inicia la actualización en background."""
    if UPDATE_STATE["running"]:
        return JSONResponse({"success": False, "error": "Ya se está ejecutando una actualización"}, status_code=400)

    UPDATE_STATE["progress"] = 5
    thread = threading.Thread(target=_run_update, daemon=True)
    thread.start()
    return JSONResponse({"success": True, "message": "Actualización iniciada"})


@app.get("/api/estado")
def estado():
    """Devuelve el estado actual de la actualización."""
    return JSONResponse({
        "running": UPDATE_STATE["running"],
        "progress": UPDATE_STATE["progress"],
        "error": UPDATE_STATE["error"],
      "message": UPDATE_STATE["message"],
    })


@app.get("/", response_class=HTMLResponse)
def index(
    q: str = Query(default="", description="Texto para buscar"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=500),
) -> HTMLResponse:
    if not DB_PATH.exists():
        return _html_page(
            "OsiDOc Viewer",
            """
<div class="card">
  <div class="head">
    <div><h1>OsiDOc Viewer</h1><div class="meta">Base de datos no encontrada</div></div>
    <button class="btn refresh" onclick="abrirActualizacion()"><span class="spinner"></span>Actualizar</button>
  </div>
  <div class="content">
    <div class="info-box">No existe la base de datos SQLite. Primero ejecuta <code>osinergmin_auth.py</code> para generar la data.</div>
  </div>
</div>
""",
        )

    with _connect() as con:
        columns = _get_table_columns(con)
        if not columns:
            return _html_page(
                "OsiDOc Viewer",
                """
<div class="card">
  <div class="head"><h1>OsiDOc Viewer</h1><span class="badge">Sin tabla</span></div>
  <div class="content">No se encontró la tabla notificaciones.</div>
</div>
""",
            )

        where_clause = ""
        params: list[str | int] = []
        if q.strip():
            where_parts = [f'CAST("{c}" AS TEXT) LIKE ?' for c in columns]
            where_clause = " WHERE " + " OR ".join(where_parts)
            params.extend([f"%{q.strip()}%"] * len(columns))

        total = int(con.execute(f"SELECT COUNT(*) FROM notificaciones{where_clause}", params).fetchone()[0])
        offset = (page - 1) * page_size
        query = f"SELECT rowid, * FROM notificaciones{where_clause} ORDER BY rowid DESC LIMIT ? OFFSET ?"
        rows = con.execute(query, params + [page_size, offset]).fetchall()

    notif_col = _find_notification_column(columns)

    display_cols = [c for c in columns if c.lower() in [dc.lower() for dc in COLUMNS_TO_DISPLAY]]
    head = "".join(f"<th>{html.escape(c)}</th>" for c in display_cols) + "<th>Documentos</th>"
    body_rows: list[str] = []
    for row in rows:
        tds = []
        for c in display_cols:
            val = str(row[c] if row[c] is not None else "")
            tds.append(f"<td>{html.escape(val[:50])}</td>" if len(val) > 50 else f"<td>{html.escape(val)}</td>")

        notif = str(row[notif_col]) if notif_col and row[notif_col] else ""
        row_id = int(row["rowid"])
        if notif:
            docs_link = (
                f"<button type='button' class='btn secondary' "
                f"onclick=\"toggleDocs(this, '{html.escape(notif)}', {row_id})\">Ver documentos</button>"
            )
        else:
            docs_link = "<span class='muted'>Sin Nro.</span>"
        tds.append(f"<td>{docs_link}</td>")

        body_rows.append("<tr>" + "".join(tds) + "</tr>")
        body_rows.append(
            "<tr id='docs-row-{}' class='accordion-row'><td colspan='{}' class='accordion-cell'>"
            "<div id='docs-body-{}' class='accordion-box' data-loaded='0'></div>"
            "</td></tr>".format(row_id, len(display_cols) + 1, row_id)
        )

    prev_page = max(1, page - 1)
    next_page = page + 1

    body = f"""
<div class="card">
  <div class="head">
    <div>
      <h1>OsiDOc Viewer</h1>
      <div class="meta">📊 {total} registros en BD</div>
    </div>
    <button class="btn refresh" onclick="abrirActualizacion()"><span class="spinner"></span>Actualizar</button>
  </div>
  <div class="content">
    <form class="toolbar" method="get" action="/">
      <input type="text" name="q" value="{html.escape(q)}" placeholder="🔍 Buscar en cualquier columna..." />
      <input type="hidden" name="page_size" value="{page_size}" />
      <button type="submit">Buscar</button>
      <a class="btn secondary" href="/">Limpiar</a>
    </form>
    <div class="info-box">
      📄 Página {page} de {(total + page_size - 1) // page_size} | Total: {total} registros
    </div>
    <div class="toolbar">
      <a class="btn secondary" href="/?q={quote(q)}&page={prev_page}&page_size={page_size}">← Anterior</a>
      <a class="btn secondary" href="/?q={quote(q)}&page={next_page}&page_size={page_size}">Siguiente →</a>
    </div>
    <div class="table-wrap">
      <table>
        <thead><tr>{head}</tr></thead>
        <tbody>{''.join(body_rows) if body_rows else '<tr><td colspan="999" style="text-align:center; color:#999;">Sin resultados</td></tr>'}</tbody>
      </table>
    </div>
  </div>
</div>
"""
    return _html_page("OsiDOc Viewer", body)


@app.get("/notificaciones/{numero}/documentos", response_class=HTMLResponse)
def documentos(numero: str) -> HTMLResponse:
    docs = sorted(DOWNLOADS_DIR.glob(f"*/{numero}/*"), key=lambda p: p.name.lower())

    if not docs:
        body = f"""
<div class="card">
  <div class="head"><h1>Documentos de {html.escape(numero)}</h1><span class="badge">0 archivos</span></div>
  <div class="content">
    <p>No se encontraron archivos para este número.</p>
    <a class="btn secondary" href="/">Volver al listado</a>
  </div>
</div>
"""
        return _html_page(f"Documentos {numero}", body)

    items: list[str] = []
    for path in docs:
        relative = path.relative_to(DOWNLOADS_DIR).as_posix()
        href = f"/files/{quote(relative)}"
        date_folder = path.parents[1].name if len(path.parents) > 1 else ""
        items.append(
            f"<li><a href='{href}' target='_blank'><strong>{html.escape(path.name)}</strong></a> "
            f"<span class='muted'>({html.escape(date_folder)})</span></li>"
        )

    body = f"""
<div class="card">
  <div class="head"><h1>📄 Documentos de {html.escape(numero)}</h1><span class="badge">{len(docs)} archivos</span></div>
  <div class="content">
    <a class="btn secondary" href="/">← Volver al listado</a>
    <ul class="file-list">{''.join(items)}</ul>
  </div>
</div>
"""
    return _html_page(f"Documentos {numero}", body)


@app.get("/api/notificaciones/{numero}/documentos")
def documentos_api(numero: str):
    docs = sorted(DOWNLOADS_DIR.glob(f"*/{numero}/*"), key=lambda p: p.name.lower())
    files: list[dict[str, str]] = []
    for path in docs:
        relative = path.relative_to(DOWNLOADS_DIR).as_posix()
        href = f"/files/{quote(relative)}"
        date_folder = path.parents[1].name if len(path.parents) > 1 else ""
        try:
            size_kb = f"{path.stat().st_size / 1024:.2f}"
        except OSError:
            size_kb = "0.00"
        files.append(
            {
                "name": path.name,
                "href": href,
                "date_folder": date_folder,
                "size_kb": size_kb,
            }
        )

    return JSONResponse({"numero": numero, "files": files})


@app.get("/files/{file_path:path}")
def serve_file(file_path: str):
    candidate = (DOWNLOADS_DIR / file_path).resolve()

    if DOWNLOADS_DIR not in candidate.parents and candidate != DOWNLOADS_DIR:
        raise HTTPException(status_code=400, detail="Ruta invalida")

    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    return FileResponse(
        str(candidate),
        media_type="application/pdf",
        headers={"Content-Disposition": f"inline; filename=\"{quote(candidate.name)}\""}
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("web_app:app", host="127.0.0.1", port=8000, reload=False)
