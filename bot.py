import os
import json
import time
import random
import sqlite3
import requests
from dotenv import load_dotenv

import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from vk_api.keyboard import VkKeyboard, VkKeyboardColor

load_dotenv()

VK_TOKEN = os.getenv("VK_TOKEN")
VK_GROUP_ID = int(os.getenv("VK_GROUP_ID", "0"))
CHAT_PEER_ID = int(os.getenv("CHAT_PEER_ID", "2000000190"))

COMPANY = os.getenv("COMPANY", "company1")
API_BASE = os.getenv("API_BASE", "https://rotorbus.ru").rstrip("/")

DB_PATH = "bot.sqlite3"

ROUTES_CACHE_TTL = 300
USERS_CACHE_TTL = 300
VEHICLES_CACHE_TTL = 300
VK_DOMAIN_CACHE_TTL = 3600


# ---------------- DB ----------------
def db_init():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
    CREATE TABLE IF NOT EXISTS states(
        peer_id INTEGER,
        user_id INTEGER,
        status TEXT,
        route_id INTEGER,
        route_name TEXT,
        vehicle_id INTEGER,
        board_number TEXT,
        updated_at INTEGER,
        PRIMARY KEY(peer_id, user_id)
    )
    """)
    con.commit()

    # Если таблица была создана раньше без новых колонок — попробуем добавить.
    # (Если колонка уже есть, ALTER упадёт — это нормально, игнорируем.)
    for ddl in [
        "ALTER TABLE states ADD COLUMN vehicle_id INTEGER",
        "ALTER TABLE states ADD COLUMN board_number TEXT",
    ]:
        try:
            con.execute(ddl)
            con.commit()
        except Exception:
            pass

    con.close()


def db_set(peer_id, user_id, status, route_id=None, route_name=None, vehicle_id=None, board_number=None):
    con = sqlite3.connect(DB_PATH)
    con.execute("""
    INSERT INTO states(peer_id,user_id,status,route_id,route_name,vehicle_id,board_number,updated_at)
    VALUES(?,?,?,?,?,?,?,?)
    ON CONFLICT(peer_id,user_id) DO UPDATE SET
      status=excluded.status,
      route_id=excluded.route_id,
      route_name=excluded.route_name,
      vehicle_id=excluded.vehicle_id,
      board_number=excluded.board_number,
      updated_at=excluded.updated_at
    """, (peer_id, user_id, status, route_id, route_name, vehicle_id, board_number, int(time.time())))
    con.commit()
    con.close()


def db_get(peer_id, user_id):
    con = sqlite3.connect(DB_PATH)
    row = con.execute(
        "SELECT status, route_id, route_name, vehicle_id, board_number FROM states WHERE peer_id=? AND user_id=?",
        (peer_id, user_id)
    ).fetchone()
    con.close()
    if not row:
        return None
    return {
        "status": row[0],
        "route_id": row[1],
        "route_name": row[2],
        "vehicle_id": row[3],
        "board_number": row[4],
    }


def db_delete(peer_id, user_id):
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM states WHERE peer_id=? AND user_id=?", (peer_id, user_id))
    con.commit()
    con.close()


def db_active(peer_id):
    # "Сход" не показываем
    con = sqlite3.connect(DB_PATH)
    rows = con.execute("""
        SELECT user_id, status, route_name, board_number
        FROM states
        WHERE peer_id=? AND status IS NOT NULL AND status != 'Сход'
        ORDER BY updated_at DESC
    """, (peer_id,)).fetchall()
    con.close()
    return rows


# ---------------- API caches ----------------
_routes_cache = {"ts": 0, "data": []}
_users_cache = {"ts": 0, "map": {}}        # domain -> name
_vehicles_cache = {"ts": 0, "data": []}    # list of {"id": int, "board": str}
_vk_domain_cache = {"ts": 0, "map": {}}    # vk_user_id -> domain


def normalize_vk_value(v: str) -> str:
    if not v:
        return ""
    v = str(v).strip()
    v = v.replace("http://", "").replace("https://", "")
    if v.startswith("vk.com/"):
        v = v[len("vk.com/"):]
    v = v.strip().strip("/")
    v = v.lstrip("@").strip()
    return v


def api_routes():
    now = time.time()
    if _routes_cache["data"] and (now - _routes_cache["ts"]) < ROUTES_CACHE_TTL:
        return _routes_cache["data"]

    url = f"{API_BASE}/api/routes/{COMPANY}"
    r = requests.get(url, timeout=10, headers={"Content-Type": "application/json"})
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "ok":
        raise RuntimeError(data.get("message", "API error"))

    routes = data.get("routes", [])
    simple = [{"id": int(x["id"]), "name": str(x["route"])} for x in routes]
    _routes_cache["ts"] = now
    _routes_cache["data"] = simple
    return simple


def api_vehicles():
    now = time.time()
    if _vehicles_cache["data"] and (now - _vehicles_cache["ts"]) < VEHICLES_CACHE_TTL:
        return _vehicles_cache["data"]

    url = f"{API_BASE}/api/vehicles/{COMPANY}"
    r = requests.get(url, timeout=10, headers={"Content-Type": "application/json"})
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "ok":
        raise RuntimeError(data.get("message", "API error"))

    vehicles = data.get("vehicles", [])
    # В ответе “number” — это идентификатор ТС (в доке он называется number)
    simple = []
    for v in vehicles:
        vid = int(v.get("number"))
        board = str(v.get("board_number", "")).strip()
        if board:
            simple.append({"id": vid, "board": board})

    _vehicles_cache["ts"] = now
    _vehicles_cache["data"] = simple
    return simple


def api_users_map():
    now = time.time()
    if _users_cache["map"] and (now - _users_cache["ts"]) < USERS_CACHE_TTL:
        return _users_cache["map"]

    url = f"{API_BASE}/api/users/{COMPANY}"
    r = requests.get(url, timeout=10, headers={"Content-Type": "application/json"})
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "ok":
        raise RuntimeError(data.get("message", "API error"))

    m = {}
    for u in data.get("users", []):
        domain = normalize_vk_value(u.get("vk", ""))
        name = str(u.get("name", "")).strip()
        if domain and name:
            m[domain] = name

    _users_cache["ts"] = now
    _users_cache["map"] = m
    return m


def vk_user_domain(vk, user_id: int) -> str:
    now = time.time()
    if (user_id in _vk_domain_cache["map"]) and (now - _vk_domain_cache["ts"] < VK_DOMAIN_CACHE_TTL):
        return _vk_domain_cache["map"][user_id]

    info = vk.users.get(user_ids=[user_id], fields="domain")[0]
    domain = (info.get("domain") or "").strip()

    _vk_domain_cache["map"][user_id] = domain
    _vk_domain_cache["ts"] = now
    return domain


# ---------------- Keyboards ----------------
def kb_status():
    kb = VkKeyboard(one_time=False, inline=False)
    kb.add_button("Выход", VkKeyboardColor.POSITIVE, payload=json.dumps({"a": "shift"}))
    kb.add_button("Обед", VkKeyboardColor.PRIMARY, payload=json.dumps({"a": "status", "v": "Обед"}))
    kb.add_line()
    kb.add_button("Вылет", VkKeyboardColor.PRIMARY, payload=json.dumps({"a": "status", "v": "Вылет"}))
    kb.add_button("Сход", VkKeyboardColor.NEGATIVE, payload=json.dumps({"a": "status", "v": "Сход"}))
    return kb


def kb_routes(page=1, per_page=6):
    routes = api_routes()
    total = len(routes)
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    chunk = routes[start:start + per_page]

    kb = VkKeyboard(one_time=True, inline=False)

    for r in chunk:
        kb.add_button(
            r["name"][:40],
            VkKeyboardColor.PRIMARY,
            payload=json.dumps({"a": "route", "id": r["id"], "name": r["name"]})
        )
        kb.add_line()

    if page > 1 or page < pages:
        if page > 1:
            kb.add_button("⬅️ Назад", VkKeyboardColor.SECONDARY,
                          payload=json.dumps({"a": "routes_page", "p": page - 1}))
        if page < pages:
            kb.add_button("Далее ➡️", VkKeyboardColor.SECONDARY,
                          payload=json.dumps({"a": "routes_page", "p": page + 1}))
        kb.add_line()

    kb.add_button("Отмена", VkKeyboardColor.SECONDARY, payload=json.dumps({"a": "cancel"}))
    return kb


def kb_vehicles(page=1, per_page=6):
    vehicles = api_vehicles()
    total = len(vehicles)
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    chunk = vehicles[start:start + per_page]

    kb = VkKeyboard(one_time=True, inline=False)

    for v in chunk:
        kb.add_button(
            v["board"][:40],
            VkKeyboardColor.PRIMARY,
            payload=json.dumps({"a": "vehicle", "id": v["id"], "board": v["board"]})
        )
        kb.add_line()

    if page > 1 or page < pages:
        if page > 1:
            kb.add_button("⬅️ Назад", VkKeyboardColor.SECONDARY,
                          payload=json.dumps({"a": "vehicles_page", "p": page - 1}))
        if page < pages:
            kb.add_button("Далее ➡️", VkKeyboardColor.SECONDARY,
                          payload=json.dumps({"a": "vehicles_page", "p": page + 1}))
        kb.add_line()

    kb.add_button("Отмена", VkKeyboardColor.SECONDARY, payload=json.dumps({"a": "cancel"}))
    return kb


# ---------------- VK send ----------------
def send(vk, peer_id, text, keyboard=None):
    params = {
        "peer_id": peer_id,
        "message": text,
        "random_id": random.randint(1, 2_000_000_000),
    }
    try:
        if keyboard:
            params["keyboard"] = keyboard.get_keyboard()
        vk.messages.send(**params)
    except Exception as e:
        # если ВК ругается на "chat bot feature" — отправим без клавиатуры
        if "912" in str(e) or "Chat bot feature" in str(e):
            params.pop("keyboard", None)
            vk.messages.send(**params)
        else:
            raise


# ---------------- Summary (только список активных) ----------------
def summary(vk, peer_id):
    rows = db_active(peer_id)
    if not rows:
        return "❌ Активных сотрудников сейчас нет."

    try:
        name_by_domain = api_users_map()
    except Exception:
        name_by_domain = {}

    status_emoji = {
        "Выход": "🟩",
        "Обед": "💤",
        "Вылет": "🚫",
        "Сход": "⚫",
    }

    lines = ["⚡ Активные сотрудники:"]
    for (uid, status, route_name, board_number) in rows:
        uid = int(uid)
        domain = vk_user_domain(vk, uid)
        name = name_by_domain.get(domain, domain or f"id{uid}")

        emoji = status_emoji.get(status, "🔵")
        route = route_name or "маршрут не выбран"
        board = board_number or "борт не выбран"

        # Формат: [эмодзи] [маршрут] ([борт]) | [Ник]
        lines.append(f"{emoji} {route} ({board}) | {name}")

    return "\n".join(lines)


def send_summary(vk, peer_id, keyboard=None):
    send(vk, peer_id, summary(vk, peer_id), keyboard=keyboard)


# ---------------- Main ----------------
def main():
    if not VK_TOKEN or VK_GROUP_ID == 0:
        print("Ошибка: заполни VK_TOKEN и VK_GROUP_ID в .env")
        return

    db_init()

    vk_session = vk_api.VkApi(token=VK_TOKEN)
    vk = vk_session.get_api()
    longpoll = VkBotLongPoll(vk_session, VK_GROUP_ID)

    print("Бот запущен. Работает только в чате peer_id =", CHAT_PEER_ID)

    for event in longpoll.listen():
        if event.type != VkBotEventType.MESSAGE_NEW:
            continue

        msg = event.obj.message
        peer_id = msg.get("peer_id")
        user_id = msg.get("from_id")
        payload = msg.get("payload")

        if peer_id != CHAT_PEER_ID:
            continue

        # без payload — молчим (чтобы не было мусора)
        if not payload:
            continue

        try:
            p = json.loads(payload)
        except Exception:
            continue

        a = p.get("a")

        # Отмена: просто список + кнопки статусов
        if a == "cancel":
            send_summary(vk, peer_id, keyboard=kb_status())
            continue

        # листаем маршруты
        if a == "routes_page":
            page = int(p.get("p", 1))
            send_summary(vk, peer_id, keyboard=kb_routes(page=page))
            continue

        # листаем борта
        if a == "vehicles_page":
            page = int(p.get("p", 1))
            send_summary(vk, peer_id, keyboard=kb_vehicles(page=page))
            continue

        # Выход (старт): сначала маршрут, потом борт
        if a == "shift":
            st = db_get(peer_id, user_id)
            if not st or not st.get("route_id"):
                send_summary(vk, peer_id, keyboard=kb_routes(page=1))
                continue
            if not st.get("vehicle_id"):
                send_summary(vk, peer_id, keyboard=kb_vehicles(page=1))
                continue

            # всё выбрано — ставим статус Выход
            db_set(peer_id, user_id, "Выход", st["route_id"], st["route_name"], st["vehicle_id"], st["board_number"])
            send_summary(vk, peer_id, keyboard=kb_status())
            continue

        # Выбор маршрута: сохраняем маршрут, потом просим борт
        if a == "route":
            route_id = int(p["id"])
            route_name = str(p["name"])
            st = db_get(peer_id, user_id)
            vehicle_id = st.get("vehicle_id") if st else None
            board = st.get("board_number") if st else None

            db_set(peer_id, user_id, "Выход", route_id, route_name, vehicle_id, board)

            # если борта нет — показываем выбор борта, иначе сразу статусы
            if not vehicle_id:
                send_summary(vk, peer_id, keyboard=kb_vehicles(page=1))
            else:
                send_summary(vk, peer_id, keyboard=kb_status())
            continue

        # Выбор борта: сохраняем борт, если маршрута нет — сначала маршрут, иначе статусы
        if a == "vehicle":
            vehicle_id = int(p["id"])
            board = str(p["board"])

            st = db_get(peer_id, user_id)
            route_id = st.get("route_id") if st else None
            route_name = st.get("route_name") if st else None

            # если маршрут ещё не выбран — сохраним борт, а потом предложим маршрут
            if not route_id:
                db_set(peer_id, user_id, "Выход", None, None, vehicle_id, board)
                send_summary(vk, peer_id, keyboard=kb_routes(page=1))
            else:
                db_set(peer_id, user_id, "Выход", route_id, route_name, vehicle_id, board)
                send_summary(vk, peer_id, keyboard=kb_status())
            continue

        # Статусы (требуем и маршрут, и борт)
        if a == "status":
            new_status = str(p.get("v", "")).strip()
            st = db_get(peer_id, user_id)

            if new_status != "Сход":
                if not st or not st.get("route_id"):
                    send_summary(vk, peer_id, keyboard=kb_routes(page=1))
                    continue
                if not st.get("vehicle_id"):
                    send_summary(vk, peer_id, keyboard=kb_vehicles(page=1))
                    continue

                db_set(peer_id, user_id, new_status, st["route_id"], st["route_name"], st["vehicle_id"], st["board_number"])
                send_summary(vk, peer_id, keyboard=kb_status())
            else:
                db_delete(peer_id, user_id)
                send_summary(vk, peer_id, keyboard=kb_status())
            continue


if __name__ == "__main__":
    main()
