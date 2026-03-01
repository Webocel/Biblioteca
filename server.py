"""
╔══════════════════════════════════════════════════════════╗
║  BIBLIOTECA - Backend API                                ║
╚══════════════════════════════════════════════════════════╝
"""

import sqlite3
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from telethon import TelegramClient
from telethon.tl.types import InputDocument, InputPhoto
import uvicorn

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
API_ID       = 25510659
API_HASH     = "24221875dc27a5b125d70ab584a46899"
GROUP_ID     = -1002945208183
SESSION_NAME = "biblioteca_session"
DB_PATH      = "biblioteca.db"
PAGE_SIZE    = 48
# ─────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
#  BASE DE DATOS
# ═══════════════════════════════════════════════════════════

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def book_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["file_size_mb"] = round(d["file_size"] / 1024 / 1024, 1) if d["file_size"] else 0
    return d


# ═══════════════════════════════════════════════════════════
#  CLIENTE TELEGRAM
# ═══════════════════════════════════════════════════════════

tg_client: TelegramClient = None

async def get_telegram_client() -> TelegramClient:
    global tg_client
    if tg_client is None or not tg_client.is_connected():
        tg_client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        await tg_client.start()
        log.info("Cliente de Telegram conectado")
    return tg_client


# ═══════════════════════════════════════════════════════════
#  APP FASTAPI
# ═══════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Iniciando servidor…")
    await get_telegram_client()
    yield
    if tg_client:
        await tg_client.disconnect()
        log.info("Cliente desconectado")


app = FastAPI(title="Biblioteca Digital API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════════════════════
#  ENDPOINTS
# ═══════════════════════════════════════════════════════════

@app.get("/")
async def root():
    return {"status": "ok", "message": "Biblioteca API corriendo"}


@app.get("/biblioteca")
async def serve_web():
    return FileResponse("biblioteca.html")


# ── GET /api/books ─────────────────────────────────────────
@app.get("/api/books")
async def get_books(
    q:        str = Query("", description="Búsqueda"),
    category: str = Query("", description="Categoría"),
    page:     int = Query(1, ge=1),
):
    conn = get_db()
    try:
        conditions, params = [], []

        if q:
            conditions.append("(LOWER(title) LIKE ? OR LOWER(author) LIKE ?)")
            params.extend([f"%{q.lower()}%", f"%{q.lower()}%"])

        if category:
            conditions.append("category = ?")
            params.append(category)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        total  = conn.execute(f"SELECT COUNT(*) FROM books {where}", params).fetchone()[0]
        offset = (page - 1) * PAGE_SIZE
        rows   = conn.execute(f"""
            SELECT b.*, c.file_id as cover_file_id
            FROM books b
            LEFT JOIN covers c ON b.cover_msg_id = c.msg_id
            {where}
            ORDER BY b.id DESC
            LIMIT ? OFFSET ?
        """, params + [PAGE_SIZE, offset]).fetchall()

        return {
            "total":   total,
            "page":    page,
            "pages":   -(-total // PAGE_SIZE),
            "results": [book_to_dict(r) for r in rows],
        }
    finally:
        conn.close()


# ── GET /api/groups ────────────────────────────────────────
# Devuelve grupos paginados por portada (una entrada por cover_msg_id)
@app.get("/api/groups")
async def get_groups(
    q:        str = Query("", description="Búsqueda"),
    category: str = Query("", description="Categoría"),
    page:     int = Query(1, ge=1),
):
    conn = get_db()
    try:
        conditions, params = [], []
        if q:
            conditions.append("(LOWER(title) LIKE ? OR LOWER(author) LIKE ?)")
            params.extend([f"%{q.lower()}%", f"%{q.lower()}%"])
        if category:
            conditions.append("category = ?")
            params.append(category)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        # Cuenta grupos únicos (por cover_msg_id, o id si no tiene portada)
        total_groups = conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT COALESCE(cover_msg_id, -id) as grp
                FROM books {where}
                GROUP BY grp
            )
        """, params).fetchone()[0]

        offset = (page - 1) * PAGE_SIZE

        # Obtiene los cover_msg_id de esta página, ordenados por el mensaje más reciente del grupo
        group_keys = conn.execute(f"""
            SELECT COALESCE(cover_msg_id, -id) as grp, MAX(message_id) as last_msg
            FROM books {where}
            GROUP BY grp
            ORDER BY last_msg DESC
            LIMIT ? OFFSET ?
        """, params + [PAGE_SIZE, offset]).fetchall()

        # Para cada grupo, trae todos los libros ordenados por message_id ASC (tomo 1 primero)
        results = []
        for grow in group_keys:
            grp = grow["grp"]
            if grp > 0:
                books = conn.execute("""
                    SELECT * FROM books WHERE cover_msg_id = ?
                    ORDER BY message_id ASC
                """, (grp,)).fetchall()
            else:
                books = conn.execute("""
                    SELECT * FROM books WHERE id = ?
                """, (-grp,)).fetchall()

            if not books:
                continue

            vols = [book_to_dict(b) for b in books]
            results.append({
                "cover_msg_id": vols[0]["cover_msg_id"],
                "category":     vols[0]["category"],
                "volumes":      vols,
            })

        return {
            "total":   total_groups,
            "page":    page,
            "pages":   -(-total_groups // PAGE_SIZE),
            "results": results,
        }
    finally:
        conn.close()


# ── GET /api/books/{id} ────────────────────────────────────
@app.get("/api/books/{book_id}")
async def get_book(book_id: int):
    conn = get_db()
    try:
        row = conn.execute("""
            SELECT b.*, c.file_id as cover_file_id
            FROM books b
            LEFT JOIN covers c ON b.cover_msg_id = c.msg_id
            WHERE b.id = ?
        """, (book_id,)).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Libro no encontrado")

        return book_to_dict(row)
    finally:
        conn.close()


# ── GET /api/download/{id} ─────────────────────────────────
@app.get("/api/download/{book_id}")
async def download_book(book_id: int):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM books WHERE id = ?", (book_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Libro no encontrado")

        book   = dict(row)
        client = await get_telegram_client()

        # Intenta obtener el mensaje directo del grupo
        try:
            message = await client.get_messages(entity=GROUP_ID, ids=book["message_id"])
            if message and message.media:
                filename = book["title"] + ".cbr"

                async def gen_msg():
                    async for chunk in client.iter_download(message.media):
                        yield chunk

                conn.execute("UPDATE books SET downloads = downloads + 1 WHERE id = ?", (book_id,))
                conn.commit()

                return StreamingResponse(
                    gen_msg(),
                    media_type="application/x-cbr",
                    headers={"Content-Disposition": f'attachment; filename="{filename}"',
                             "Content-Length": str(book["file_size"])}
                )
        except Exception as e:
            log.warning(f"Fallback InputDocument para libro {book_id}: {e}")

        # Fallback: usa file_id guardado
        doc = InputDocument(
            id=int(book["file_id"]),
            access_hash=0,
            file_reference=bytes.fromhex(book["file_ref"]),
        )
        filename = book["title"] + ".cbr"

        async def gen_doc():
            async for chunk in client.iter_download(doc):
                yield chunk

        conn.execute("UPDATE books SET downloads = downloads + 1 WHERE id = ?", (book_id,))
        conn.commit()

        return StreamingResponse(
            gen_doc(),
            media_type="application/x-cbr",
            headers={"Content-Disposition": f'attachment; filename="{filename}"',
                     "Content-Length": str(book["file_size"])}
        )

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error descargando libro {book_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ── GET /api/cover/{msg_id} ────────────────────────────────
@app.get("/api/cover/{msg_id}")
async def get_cover(msg_id: int):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM covers WHERE msg_id = ?", (msg_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Portada no encontrada")

        client = await get_telegram_client()

        try:
            message = await client.get_messages(entity=GROUP_ID, ids=msg_id)
            if message and message.media:
                async def gen_img_msg():
                    async for chunk in client.iter_download(message.media):
                        yield chunk
                return StreamingResponse(gen_img_msg(), media_type="image/jpeg")
        except Exception as e:
            log.warning(f"Fallback InputPhoto para portada {msg_id}: {e}")

        photo = InputPhoto(
            id=int(row["file_id"]),
            access_hash=0,
            file_reference=bytes.fromhex(row["file_ref"]),
        )

        async def gen_img_photo():
            async for chunk in client.iter_download(photo):
                yield chunk

        return StreamingResponse(gen_img_photo(), media_type="image/jpeg")

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error portada {msg_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ── GET /api/stats ─────────────────────────────────────────
@app.get("/api/stats")
async def get_stats():
    conn = get_db()
    try:
        total      = conn.execute("SELECT COUNT(*) FROM books").fetchone()[0]
        with_cover = conn.execute("SELECT COUNT(*) FROM books WHERE cover_msg_id IS NOT NULL").fetchone()[0]
        total_dl   = conn.execute("SELECT SUM(downloads) FROM books").fetchone()[0] or 0
        categories = conn.execute("""
            SELECT category, COUNT(*) as count FROM books
            WHERE category != '' GROUP BY category ORDER BY count DESC
        """).fetchall()

        return {
            "total_books":      total,
            "books_with_cover": with_cover,
            "total_downloads":  total_dl,
            "categories":       [dict(r) for r in categories],
        }
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════
#  ENTRADA
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False, log_level="info")
