import asyncio
import html
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ================= НАСТРОЙКИ =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = {1437371388}  # ЗАМЕНИТЕ на ваш Telegram ID (число без кавычек)
TIMEZONE_OFFSET = 3      # Часовой пояс от UTC (3 — Москва / СПб)
DB_PATH = "queue_bot.db"
CHECK_INTERVAL_SEC = 30  # Как часто проверять открытие очередей (в секундах)
# =============================================

TZ = timezone(timedelta(hours=TIMEZONE_OFFSET))
router = Router()


# ================= РАБОТА С ВРЕМЕНЕМ И БАЗОЙ =================
def now_local() -> datetime:
    return datetime.now(TZ).replace(tzinfo=None)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                full_name TEXT NOT NULL,
                username TEXT
            );

            CREATE TABLE IF NOT EXISTS queues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                event_time TEXT NOT NULL,
                open_time TEXT NOT NULL,
                notified_open INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id INTEGER NOT NULL,
                user_id INTEGER,
                display_name TEXT NOT NULL,
                position INTEGER NOT NULL,
                FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )

        # Безопасное обновление старой базы (если колонки ещё нет)
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(queues)").fetchall()]
        if "notified_open" not in cols:
            conn.execute("ALTER TABLE queues ADD COLUMN notified_open INTEGER NOT NULL DEFAULT 0")
            conn.execute(
                "UPDATE queues SET notified_open = 1 WHERE open_time <= ?",
                (now_local().strftime("%Y-%m-%d %H:%M"),),
            )


def are_notifications_enabled() -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = 'notifications_enabled'"
        ).fetchone()
        return True if row is None else row["value"] == "1"


def set_notifications_enabled(enabled: bool) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO settings (key, value)
            VALUES ('notifications_enabled', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            ("1" if enabled else "0",),
        )


def get_user_name(user_id: int) -> Optional[str]:
    with get_conn() as conn:
        row = conn.execute("SELECT full_name FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return row["full_name"] if row else None


def get_all_user_ids() -> list[int]:
    with get_conn() as conn:
        rows = conn.execute("SELECT user_id FROM users").fetchall()
        return [row["user_id"] for row in rows]


def save_user(user_id: int, full_name: str, username: Optional[str]) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO users (user_id, full_name, username)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                full_name = excluded.full_name,
                username = excluded.username
            """,
            (user_id, full_name, username),
        )
        conn.execute(
            "UPDATE entries SET display_name = ? WHERE user_id = ?",
            (full_name, user_id),
        )


def get_queue_close_dt(queue: dict) -> datetime:
    return datetime.strptime(queue["event_time"], "%Y-%m-%d %H:%M") + timedelta(days=1)


def get_queue_status(queue: dict) -> str:
    """Возвращает: 'waiting' (до открытия), 'open' (активна), 'closed' (прошло 24ч после события)."""
    now = now_local()
    open_dt = datetime.strptime(queue["open_time"], "%Y-%m-%d %H:%M")
    close_dt = get_queue_close_dt(queue)
    if now < open_dt:
        return "waiting"
    if now < close_dt:
        return "open"
    return "closed"


def is_queue_open(queue: dict) -> bool:
    return get_queue_status(queue) == "open"


def create_queue(title: str, event_dt: datetime) -> tuple[int, str]:
    open_dt = event_dt - timedelta(days=2)
    close_dt = event_dt + timedelta(days=1)
    now = now_local()
    if now < open_dt:
        status = "waiting"
    elif now < close_dt:
        status = "open"
    else:
        status = "closed"

    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO queues (title, event_time, open_time, notified_open) VALUES (?, ?, ?, ?)",
            (
                title,
                event_dt.strftime("%Y-%m-%d %H:%M"),
                open_dt.strftime("%Y-%m-%d %H:%M"),
                0 if status == "waiting" else 1,
            ),
        )
        return cur.lastrowid, status


def get_queue(queue_id: int) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM queues WHERE id = ?", (queue_id,)).fetchone()
        return dict(row) if row else None


def get_all_queues() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM queues ORDER BY event_time ASC").fetchall()
        return [dict(r) for r in rows]


def get_queues_to_notify_open() -> list[dict]:
    now_str = now_local().strftime("%Y-%m-%d %H:%M")
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM queues WHERE notified_open = 0 AND open_time <= ?",
            (now_str,),
        ).fetchall()
        return [dict(r) for r in rows]


def mark_queue_notified(queue_id: int) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE queues SET notified_open = 1 WHERE id = ?", (queue_id,))


def update_queue_title(queue_id: int, new_title: str) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE queues SET title = ? WHERE id = ?", (new_title, queue_id))


def update_queue_time(queue_id: int, event_dt: datetime) -> None:
    open_dt = event_dt - timedelta(days=2)
    already_passed_open = now_local() >= open_dt
    with get_conn() as conn:
        conn.execute(
            "UPDATE queues SET event_time = ?, open_time = ?, notified_open = ? WHERE id = ?",
            (
                event_dt.strftime("%Y-%m-%d %H:%M"),
                open_dt.strftime("%Y-%m-%d %H:%M"),
                1 if already_passed_open else 0,
                queue_id,
            ),
        )


def delete_queue(queue_id: int) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM entries WHERE queue_id = ?", (queue_id,))
        conn.execute("DELETE FROM queues WHERE id = ?", (queue_id,))


def get_entries(queue_id: int) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM entries WHERE queue_id = ? ORDER BY position ASC, id ASC",
            (queue_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_entry(entry_id: int) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM entries WHERE id = ?", (entry_id,)).fetchone()
        return dict(row) if row else None


def renumber_queue(conn: sqlite3.Connection, queue_id: int) -> None:
    rows = conn.execute(
        "SELECT id FROM entries WHERE queue_id = ? ORDER BY position ASC, id ASC",
        (queue_id,),
    ).fetchall()
    for pos, row in enumerate(rows, start=1):
        conn.execute("UPDATE entries SET position = ? WHERE id = ?", (pos, row["id"]))


def join_queue(queue_id: int, user_id: int, display_name: str) -> bool:
    with get_conn() as conn:
        exists = conn.execute(
            "SELECT 1 FROM entries WHERE queue_id = ? AND user_id = ?",
            (queue_id, user_id),
        ).fetchone()
        if exists:
            return False
        max_pos = conn.execute(
            "SELECT COALESCE(MAX(position), 0) AS mp FROM entries WHERE queue_id = ?",
            (queue_id,),
        ).fetchone()["mp"]
        conn.execute(
            "INSERT INTO entries (queue_id, user_id, display_name, position) VALUES (?, ?, ?, ?)",
            (queue_id, user_id, display_name, max_pos + 1),
        )
        return True


def add_manual_entry(queue_id: int, display_name: str) -> None:
    with get_conn() as conn:
        max_pos = conn.execute(
            "SELECT COALESCE(MAX(position), 0) AS mp FROM entries WHERE queue_id = ?",
            (queue_id,),
        ).fetchone()["mp"]
        conn.execute(
            "INSERT INTO entries (queue_id, user_id, display_name, position) VALUES (?, NULL, ?, ?)",
            (queue_id, display_name, max_pos + 1),
        )


def leave_queue(queue_id: int, user_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM entries WHERE queue_id = ? AND user_id = ?",
            (queue_id, user_id),
        )
        if cur.rowcount > 0:
            renumber_queue(conn, queue_id)
            return True
        return False


def remove_entry_by_id(entry_id: int) -> Optional[int]:
    with get_conn() as conn:
        row = conn.execute("SELECT queue_id FROM entries WHERE id = ?", (entry_id,)).fetchone()
        if not row:
            return None
        qid = row["queue_id"]
        conn.execute("DELETE FROM entries WHERE id = ?", (entry_id,))
        renumber_queue(conn, qid)
        return qid


def move_entry(entry_id: int, direction: int) -> Optional[int]:
    """direction: -1 (вверх) или +1 (вниз)"""
    with get_conn() as conn:
        row = conn.execute("SELECT queue_id FROM entries WHERE id = ?", (entry_id,)).fetchone()
        if not row:
            return None
        qid = row["queue_id"]
        entries = conn.execute(
            "SELECT id, position FROM entries WHERE queue_id = ? ORDER BY position ASC, id ASC",
            (qid,),
        ).fetchall()
        idx = next((i for i, e in enumerate(entries) if e["id"] == entry_id), None)
        if idx is None:
            return qid
        target = idx + direction
        if 0 <= target < len(entries):
            id_a, pos_a = entries[idx]["id"], entries[idx]["position"]
            id_b, pos_b = entries[target]["id"], entries[target]["position"]
            conn.execute("UPDATE entries SET position = ? WHERE id = ?", (pos_b, id_a))
            conn.execute("UPDATE entries SET position = ? WHERE id = ?", (pos_a, id_b))
        return qid


# ================= СОСТОЯНИЯ (FSM) =================
class Form(StatesGroup):
    waiting_for_name = State()
    creating_title = State()
    creating_date = State()
    editing_title = State()
    editing_date = State()
    adding_manual_user = State()


# ================= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def fmt_dt(dt_str: str) -> str:
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M").strftime("%d.%m.%Y в %H:%M")


def fmt_dt_obj(dt_obj: datetime) -> str:
    return dt_obj.strftime("%d.%m.%Y в %H:%M")


async def safe_edit(message: Message, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise


async def broadcast_to_users(
    bot: Bot,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> int:
    user_ids = get_all_user_ids()
    sent_count = 0
    for uid in user_ids:
        try:
            await bot.send_message(chat_id=uid, text=text, reply_markup=reply_markup)
            sent_count += 1
        except (TelegramForbiddenError, TelegramBadRequest):
            pass
        except Exception as e:
            logging.warning(f"Не удалось отправить уведомление пользователю {uid}: {e}")
        await asyncio.sleep(0.05)
    return sent_count


async def check_opening_queues(bot: Bot) -> None:
    """Фоновая задача: рассылает уведомления, когда до события остаётся 48 часов."""
    while True:
        try:
            queues_to_notify = get_queues_to_notify_open()
            for q in queues_to_notify:
                mark_queue_notified(q["id"])

                # Если очередь уже успела закрыться или уведомления отключены — не рассылаем
                if get_queue_status(q) != "open":
                    continue
                if not are_notifications_enabled():
                    logging.info(f"Очередь #{q['id']} открылась, но уведомления отключены админом.")
                    continue

                text = (
                    f"🟢 <b>Открылась запись в очередь!</b>\n\n"
                    f"📌 <b>{html.escape(q['title'])}</b>\n"
                    f"📅 Событие: {fmt_dt(q['event_time'])}\n\n"
                    f"Нажмите кнопку ниже, чтобы занять место:"
                )
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="✅ Открыть очередь", callback_data=f"view:{q['id']}")]
                    ]
                )
                sent = await broadcast_to_users(bot, text, reply_markup=kb)
                logging.info(f"Очередь #{q['id']} открылась. Уведомлено пользователей: {sent}")
        except asyncio.CancelledError:
            break
        except Exception:
            logging.exception("Ошибка в фоновой проверке открытия очередей")

        await asyncio.sleep(CHECK_INTERVAL_SEC)


def format_queue_text(
    queue: dict,
    entries: list[dict],
    for_admin: bool = False,
    bot_username: Optional[str] = None,
) -> str:
    title = html.escape(queue["title"])
    status = get_queue_status(queue)
    close_dt = get_queue_close_dt(queue)

    if status == "open":
        status_text = f"🟢 Запись открыта (закроется {fmt_dt_obj(close_dt)})"
    elif status == "waiting":
        status_text = f"⏳ Откроется: {fmt_dt(queue['open_time'])}"
    else:
        status_text = f"🔴 Закрыта (завершилась {fmt_dt_obj(close_dt)})"

    lines = [
        f"<b>📌 {title}</b>",
        f"📅 Событие: {fmt_dt(queue['event_time'])}",
        f"Статус: {status_text}",
        "",
    ]

    if for_admin and bot_username:
        link = f"https://t.me/{bot_username}?start=q_{queue['id']}"
        lines.append(f"🔗 Ссылка для участников:\n<code>{link}</code>\n")

    if entries:
        lines.append("<b>Список очереди:</b>")
        for idx, item in enumerate(entries, start=1):
            lines.append(f"{idx}. {html.escape(item['display_name'])}")
    else:
        if status == "open":
            lines.append("<i>Очередь пока пуста.</i>")
        elif status == "waiting":
            lines.append("<i>Запись ещё не открыта. Доступ появится автоматически за двое суток до события.</i>")
        else:
            lines.append("<i>Очередь закрыта. Участников не было.</i>")

    return "\n".join(lines)


# ================= КЛАВИАТУРЫ =================
def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🟢 Активные очереди", callback_data="list_queues")
    kb.button(text="🔒 Закрытые очереди", callback_data="list_closed_queues")
    kb.button(text="✏️ Изменить имя", callback_data="change_name")
    if is_admin(user_id):
        kb.button(text="⚙️ Админ-панель", callback_data="admin_menu")
    kb.adjust(1)
    return kb.as_markup()


def cancel_kb(back_cb: str = "cancel_fsm") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data=back_cb)]]
    )


def queue_view_kb(queue: dict, entries: list[dict], user_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    qid = queue["id"]
    status = get_queue_status(queue)
    in_queue = any(e["user_id"] == user_id for e in entries)

    if status == "open":
        if in_queue:
            kb.button(text="❌ Выйти из очереди", callback_data=f"leave:{qid}")
        else:
            kb.button(text="✅ Встать в очередь", callback_data=f"join:{qid}")
        kb.button(text="🔄 Обновить список", callback_data=f"view:{qid}")
        kb.button(text="⬅️ К активным очередям", callback_data="list_queues")
    elif status == "waiting":
        kb.button(text="🔄 Проверить открытие", callback_data=f"view:{qid}")
        kb.button(text="⬅️ К закрытым очередям", callback_data="list_closed_queues")
    else:
        kb.button(text="🔄 Обновить список", callback_data=f"view:{qid}")
        kb.button(text="⬅️ К закрытым очередям", callback_data="list_closed_queues")

    kb.button(text="🏠 В главное меню", callback_data="main_menu")
    if is_admin(user_id):
        kb.button(text="⚙️ Редактировать (админ)", callback_data=f"adm_q:{qid}")
    kb.adjust(1)
    return kb.as_markup()


def admin_menu_text() -> str:
    notify_status = "🔔 ВКЛ (рассылка работает)" if are_notifications_enabled() else "🔕 ВЫКЛ (режим тишины)"
    return (
        f"<b>⚙️ Админ-панель</b>\n"
        f"Уведомления пользователям: <b>{notify_status}</b>\n\n"
        f"Выберите действие:"
    )


def admin_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    notify_btn = "🔕 Отключить уведомления" if are_notifications_enabled() else "🔔 Включить уведомления"
    kb.button(text="➕ Создать очередь", callback_data="adm_create")
    kb.button(text="📂 Управление очередями", callback_data="adm_queues")
    kb.button(text=notify_btn, callback_data="adm_toggle_notify")
    kb.button(text="⬅️ В главное меню", callback_data="main_menu")
    kb.adjust(1)
    return kb.as_markup()


def admin_queue_kb(qid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="👥 Редактировать участников", callback_data=f"adm_members:{qid}")
    kb.button(text="➕ Добавить участника вручную", callback_data=f"adm_add_manual:{qid}")
    kb.button(text="📝 Изменить название", callback_data=f"adm_edit_title:{qid}")
    kb.button(text="📅 Изменить дату события", callback_data=f"adm_edit_date:{qid}")
    kb.button(text="🗑 Удалить очередь", callback_data=f"adm_del_ask:{qid}")
    kb.button(text="⬅️ Ко всем очередям", callback_data="adm_queues")
    kb.adjust(1)
    return kb.as_markup()


# ================= ПОЛЬЗОВАТЕЛЬСКАЯ ЧАСТЬ =================
async def show_queue_to_user(target: Message, qid: int, user_id: int, edit: bool = False) -> None:
    queue = get_queue(qid)
    if not queue:
        text = "Эта очередь не найдена или была удалена."
        kb = main_menu_kb(user_id)
    else:
        entries = get_entries(qid)
        text = format_queue_text(queue, entries)
        kb = queue_view_kb(queue, entries, user_id)

    if edit:
        await safe_edit(target, text, reply_markup=kb)
    else:
        await target.answer(text, reply_markup=kb)


@router.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject, state: FSMContext) -> None:
    await state.clear()
    args = command.args or ""
    pending_qid = int(args[2:]) if args.startswith("q_") and args[2:].isdigit() else None

    name = get_user_name(message.from_user.id)
    if not name:
        await state.update_data(pending_queue_id=pending_qid)
        await state.set_state(Form.waiting_for_name)
        await message.answer(
            "👋 Привет! Прежде чем начать, представьтесь.\n"
            "Напишите ваше <b>Имя и Фамилию</b> (они будут отображаться в списке очереди):"
        )
        return

    if pending_qid is not None:
        await show_queue_to_user(message, pending_qid, message.from_user.id, edit=False)
    else:
        await message.answer(
            f"С возвращением, <b>{html.escape(name)}</b>! Выберите действие:",
            reply_markup=main_menu_kb(message.from_user.id),
        )


@router.message(Form.waiting_for_name)
async def process_name(message: Message, state: FSMContext) -> None:
    full_name = (message.text or "").strip()
    if len(full_name) < 2 or len(full_name) > 60:
        await message.answer("Введите корректное имя (от 2 до 60 символов):")
        return

    save_user(message.from_user.id, full_name, message.from_user.username)
    data = await state.get_data()
    pending_qid = data.get("pending_queue_id")
    await state.clear()

    await message.answer(f"✅ Приятно познакомиться, <b>{html.escape(full_name)}</b>!")
    if pending_qid is not None:
        await show_queue_to_user(message, pending_qid, message.from_user.id, edit=False)
    else:
        await message.answer(
            "Выберите нужный раздел:",
            reply_markup=main_menu_kb(message.from_user.id),
        )


@router.callback_query(F.data == "main_menu")
async def cb_main_menu(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    name = get_user_name(callback.from_user.id) or "Пользователь"
    await safe_edit(
        callback.message,
        f"👤 Вы вошли как: <b>{html.escape(name)}</b>\nВыберите действие:",
        reply_markup=main_menu_kb(callback.from_user.id),
    )
    await callback.answer()


@router.callback_query(F.data == "cancel_fsm")
async def cb_cancel_fsm(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await safe_edit(
        callback.message,
        "Действие отменено.",
        reply_markup=main_menu_kb(callback.from_user.id),
    )
    await callback.answer()


@router.callback_query(F.data == "change_name")
async def cb_change_name(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(Form.waiting_for_name)
    await safe_edit(
        callback.message,
        "Напишите новое <b>Имя и Фамилию</b>:",
        reply_markup=cancel_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "list_queues")
async def cb_list_queues(callback: CallbackQuery) -> None:
    all_queues = get_all_queues()
    open_queues = [q for q in all_queues if get_queue_status(q) == "open"]

    kb = InlineKeyboardBuilder()
    for q in open_queues:
        kb.button(
            text=f"🟢 {q['title']} ({fmt_dt(q['event_time'])})",
            callback_data=f"view:{q['id']}",
        )
    kb.button(text="🔒 Закрытые очереди", callback_data="list_closed_queues")
    kb.button(text="⬅️ В главное меню", callback_data="main_menu")
    kb.adjust(1)

    if open_queues:
        text = "<b>🟢 Активные очереди</b>\nВыберите очередь, чтобы посмотреть список или записаться:"
    else:
        text = (
            "<b>🟢 Активные очереди</b>\n\n"
            "Сейчас нет открытых очередей.\n"
            "<i>Предстоящие и завершённые очереди можно посмотреть в разделе «🔒 Закрытые очереди».</i>"
        )

    await safe_edit(callback.message, text, reply_markup=kb.as_markup())
    await callback.answer()


@router.callback_query(F.data == "list_closed_queues")
async def cb_list_closed_queues(callback: CallbackQuery) -> None:
    all_queues = get_all_queues()
    waiting_queues = [q for q in all_queues if get_queue_status(q) == "waiting"]
    finished_queues = [q for q in all_queues if get_queue_status(q) == "closed"]

    kb = InlineKeyboardBuilder()
    for q in waiting_queues:
        kb.button(
            text=f"⏳ {q['title']} (откр. {fmt_dt(q['open_time'])})",
            callback_data=f"view:{q['id']}",
        )
    for q in reversed(finished_queues):
        kb.button(
            text=f"🔴 {q['title']} (завершена)",
            callback_data=f"view:{q['id']}",
        )
    kb.button(text="🟢 Активные очереди", callback_data="list_queues")
    kb.button(text="⬅️ В главное меню", callback_data="main_menu")
    kb.adjust(1)

    if waiting_queues or finished_queues:
        text = (
            "<b>🔒 Закрытые очереди</b>\n"
            "⏳ — ожидают открытия (за 48 ч до события)\n"
            "🔴 — завершены (прошло 24 ч после события)\n\n"
            "Нажмите на очередь, чтобы посмотреть подробности и список:"
        )
    else:
        text = "<b>🔒 Закрытые очереди</b>\n\nСейчас нет закрытых очередей."

    await safe_edit(callback.message, text, reply_markup=kb.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("view:"))
async def cb_view_queue(callback: CallbackQuery) -> None:
    qid = int(callback.data.split(":")[1])
    await show_queue_to_user(callback.message, qid, callback.from_user.id, edit=True)
    await callback.answer()


@router.callback_query(F.data.startswith("join:"))
async def cb_join_queue(callback: CallbackQuery) -> None:
    qid = int(callback.data.split(":")[1])
    queue = get_queue(qid)
    if not queue:
        await callback.answer("Очередь не найдена.", show_alert=True)
        return

    status = get_queue_status(queue)
    if status == "waiting":
        await callback.answer(f"Запись откроется {fmt_dt(queue['open_time'])}.", show_alert=True)
        return
    if status == "closed":
        await show_queue_to_user(callback.message, qid, callback.from_user.id, edit=True)
        await callback.answer("Событие прошло, эта очередь уже закрыта.", show_alert=True)
        return

    name = get_user_name(callback.from_user.id)
    if not name:
        await callback.answer("Сначала нажмите /start и представьтесь.", show_alert=True)
        return

    added = join_queue(qid, callback.from_user.id, name)
    await show_queue_to_user(callback.message, qid, callback.from_user.id, edit=True)
    await callback.answer("✅ Вы встали в очередь!" if added else "Вы уже в этой очереди.")


@router.callback_query(F.data.startswith("leave:"))
async def cb_leave_queue(callback: CallbackQuery) -> None:
    qid = int(callback.data.split(":")[1])
    queue = get_queue(qid)
    if not queue:
        await callback.answer("Очередь не найдена.", show_alert=True)
        return

    if get_queue_status(queue) != "open":
        await show_queue_to_user(callback.message, qid, callback.from_user.id, edit=True)
        await callback.answer("Очередь закрыта, изменения недоступны.", show_alert=True)
        return

    removed = leave_queue(qid, callback.from_user.id)
    await show_queue_to_user(callback.message, qid, callback.from_user.id, edit=True)
    await callback.answer("Вы вышли из очереди." if removed else "Вас не было в этой очереди.")


# ================= АДМИН-ПАНЕЛЬ =================
async def show_admin_queue(message: Message, bot: Bot, qid: int, edit: bool = True) -> None:
    queue = get_queue(qid)
    if not queue:
        text = "Очередь не найдена."
        kb = admin_menu_kb()
    else:
        me = await bot.get_me()
        entries = get_entries(qid)
        text = format_queue_text(queue, entries, for_admin=True, bot_username=me.username)
        kb = admin_queue_kb(qid)

    if edit:
        await safe_edit(message, text, reply_markup=kb)
    else:
        await message.answer(text, reply_markup=kb)


@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer(admin_menu_text(), reply_markup=admin_menu_kb())


@router.callback_query(F.data == "admin_menu")
async def cb_admin_menu(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.clear()
    await safe_edit(callback.message, admin_menu_text(), reply_markup=admin_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "adm_toggle_notify")
async def cb_adm_toggle_notify(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    new_state = not are_notifications_enabled()
    set_notifications_enabled(new_state)
    await safe_edit(callback.message, admin_menu_text(), reply_markup=admin_menu_kb())
    await callback.answer(
        "🔔 Уведомления включены!" if new_state else "🔕 Уведомления отключены (режим тишины)."
    )


@router.callback_query(F.data == "adm_queues")
async def cb_adm_queues(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return

    queues = get_all_queues()
    icons = {"open": "🟢", "waiting": "⏳", "closed": "🔴"}
    kb = InlineKeyboardBuilder()
    for q in queues:
        icon = icons[get_queue_status(q)]
        kb.button(
            text=f"{icon} {q['title']} ({fmt_dt(q['event_time'])})",
            callback_data=f"adm_q:{q['id']}",
        )
    kb.button(text="➕ Создать очередь", callback_data="adm_create")
    kb.button(text="⬅️ В админ-панель", callback_data="admin_menu")
    kb.adjust(1)

    text = "<b>📂 Все очереди</b>\n🟢 — открыта | ⏳ — ждёт открытия | 🔴 — завершена:"
    await safe_edit(callback.message, text, reply_markup=kb.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("adm_q:"))
async def cb_adm_q(callback: CallbackQuery, bot: Bot) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    qid = int(callback.data.split(":")[1])
    await show_admin_queue(callback.message, bot, qid, edit=True)
    await callback.answer()


# --- Создание очереди ---
@router.callback_query(F.data == "adm_create")
async def cb_adm_create(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(Form.creating_title)
    await safe_edit(
        callback.message,
        "Введите <b>название новой очереди</b> (например: Сдача лабораторной №3):",
        reply_markup=cancel_kb("admin_menu"),
    )
    await callback.answer()


@router.message(Form.creating_title)
async def process_create_title(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    title = (message.text or "").strip()
    if not title or len(title) > 100:
        await message.answer("Название должно быть от 1 до 100 символов. Попробуйте снова:")
        return
    await state.update_data(title=title)
    await state.set_state(Form.creating_date)
    example_dt = (now_local() + timedelta(days=3)).strftime("%d.%m.%Y 14:00")
    await message.answer(
        f"Теперь введите <b>дату и время события</b> в формате <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n"
        f"Например: <code>{example_dt}</code>\n\n"
        f"<i>Очередь откроется за 48 часов до события и автоматически закроется через 24 часа после него.</i>",
        reply_markup=cancel_kb("admin_menu"),
    )


@router.message(Form.creating_date)
async def process_create_date(message: Message, state: FSMContext, bot: Bot) -> None:
    if not is_admin(message.from_user.id):
        return
    try:
        event_dt = datetime.strptime((message.text or "").strip(), "%d.%m.%Y %H:%M")
    except ValueError:
        await message.answer(
            "❌ Неверный формат даты. Введите как <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>, например <code>28.11.2026 14:00</code>:"
        )
        return

    data = await state.get_data()
    title = data["title"]
    qid, status = create_queue(title, event_dt)
    queue = get_queue(qid)
    await state.clear()

    if not are_notifications_enabled():
        await message.answer(
            "✅ Очередь создана!\n"
            "<i>🔕 Уведомления сейчас отключены — рассылка пользователям не выполнялась.</i>"
        )
    elif status == "closed":
        await message.answer(
            "✅ Очередь создана (сразу закрыта, так как с момента события прошло больше суток).\n"
            "<i>Рассылка пользователям не выполнялась.</i>"
        )
    else:
        if status == "open":
            notify_text = (
                f"🆕 <b>Создана новая очередь (запись уже открыта!)</b>\n\n"
                f"📌 <b>{html.escape(title)}</b>\n"
                f"📅 Событие: {fmt_dt(queue['event_time'])}\n\n"
                f"Нажмите кнопку ниже, чтобы встать в очередь:"
            )
            btn_text = "✅ Открыть очередь"
        else:
            notify_text = (
                f"🆕 <b>Создана новая очередь!</b>\n\n"
                f"📌 <b>{html.escape(title)}</b>\n"
                f"📅 Событие: {fmt_dt(queue['event_time'])}\n"
                f"⏳ Запись откроется: <b>{fmt_dt(queue['open_time'])}</b>\n\n"
                f"<i>Когда запись откроется, бот пришлёт отдельное уведомление.</i>"
            )
            btn_text = "👀 Посмотреть очередь"

        notify_kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text=btn_text, callback_data=f"view:{qid}")]]
        )
        sent = await broadcast_to_users(bot, notify_text, reply_markup=notify_kb)
        await message.answer(f"✅ Очередь создана! Уведомление отправлено пользователям: <b>{sent}</b>.")

    await show_admin_queue(message, bot, qid, edit=False)


# --- Редактирование названия и даты ---
@router.callback_query(F.data.startswith("adm_edit_title:"))
async def cb_adm_edit_title(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    qid = int(callback.data.split(":")[1])
    await state.update_data(edit_qid=qid)
    await state.set_state(Form.editing_title)
    await safe_edit(
        callback.message,
        "Введите <b>новое название</b> очереди:",
        reply_markup=cancel_kb(f"adm_q:{qid}"),
    )
    await callback.answer()


@router.message(Form.editing_title)
async def process_edit_title(message: Message, state: FSMContext, bot: Bot) -> None:
    if not is_admin(message.from_user.id):
        return
    title = (message.text or "").strip()
    if not title or len(title) > 100:
        await message.answer("Название должно быть от 1 до 100 символов:")
        return
    data = await state.get_data()
    qid = data["edit_qid"]
    update_queue_title(qid, title)
    await state.clear()
    await message.answer("✅ Название обновлено.")
    await show_admin_queue(message, bot, qid, edit=False)


@router.callback_query(F.data.startswith("adm_edit_date:"))
async def cb_adm_edit_date(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    qid = int(callback.data.split(":")[1])
    await state.update_data(edit_qid=qid)
    await state.set_state(Form.editing_date)
    await safe_edit(
        callback.message,
        "Введите <b>новую дату и время события</b> в формате <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n"
        "<i>Время открытия (-48ч) и закрытия (+24ч) пересчитаются автоматически.</i>",
        reply_markup=cancel_kb(f"adm_q:{qid}"),
    )
    await callback.answer()


@router.message(Form.editing_date)
async def process_edit_date(message: Message, state: FSMContext, bot: Bot) -> None:
    if not is_admin(message.from_user.id):
        return
    try:
        event_dt = datetime.strptime((message.text or "").strip(), "%d.%m.%Y %H:%M")
    except ValueError:
        await message.answer("❌ Неверный формат. Используйте <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>:")
        return
    data = await state.get_data()
    qid = data["edit_qid"]
    update_queue_time(qid, event_dt)
    await state.clear()
    await message.answer("✅ Дата события, время открытия и закрытия обновлены.")
    await show_admin_queue(message, bot, qid, edit=False)


# --- Добавление участника вручную ---
@router.callback_query(F.data.startswith("adm_add_manual:"))
async def cb_adm_add_manual(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    qid = int(callback.data.split(":")[1])
    await state.update_data(edit_qid=qid)
    await state.set_state(Form.adding_manual_user)
    await safe_edit(
        callback.message,
        "Введите <b>Имя и Фамилию</b> участника, которого нужно добавить в конец очереди:",
        reply_markup=cancel_kb(f"adm_q:{qid}"),
    )
    await callback.answer()


@router.message(Form.adding_manual_user)
async def process_add_manual(message: Message, state: FSMContext, bot: Bot) -> None:
    if not is_admin(message.from_user.id):
        return
    name = (message.text or "").strip()
    if len(name) < 2 or len(name) > 60:
        await message.answer("Имя должно быть от 2 до 60 символов:")
        return
    data = await state.get_data()
    qid = data["edit_qid"]
    add_manual_entry(qid, name)
    await state.clear()
    await message.answer(f"✅ Участник <b>{html.escape(name)}</b> добавлен в очередь.")
    await show_admin_queue(message, bot, qid, edit=False)


# --- Редактирование порядка и удаление участников ---
async def show_members_editor(message: Message, qid: int) -> None:
    queue = get_queue(qid)
    if not queue:
        await safe_edit(message, "Очередь не найдена.", reply_markup=admin_menu_kb())
        return
    entries = get_entries(qid)
    kb = InlineKeyboardBuilder()
    for idx, e in enumerate(entries, start=1):
        kb.button(text=f"{idx}. {e['display_name']}", callback_data=f"adm_member:{e['id']}")
    kb.button(text="⬅️ Назад к очереди", callback_data=f"adm_q:{qid}")
    kb.adjust(1)

    text = (
        f"<b>👥 Участники: {html.escape(queue['title'])}</b>\n"
        "Нажмите на участника, чтобы передвинуть его или удалить:"
        if entries
        else f"<b>👥 Участники: {html.escape(queue['title'])}</b>\n\n<i>Список пуст.</i>"
    )
    await safe_edit(message, text, reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("adm_members:"))
async def cb_adm_members(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    qid = int(callback.data.split(":")[1])
    await show_members_editor(callback.message, qid)
    await callback.answer()


async def show_single_member(message: Message, entry_id: int) -> None:
    entry = get_entry(entry_id)
    if not entry:
        await safe_edit(message, "Участник уже удалён.", reply_markup=admin_menu_kb())
        return
    qid = entry["queue_id"]
    entries = get_entries(qid)

    lines = [f"<b>Редактирование: {html.escape(entry['display_name'])}</b>\n"]
    for idx, e in enumerate(entries, start=1):
        mark = "👉 " if e["id"] == entry_id else ""
        lines.append(f"{mark}{idx}. {html.escape(e['display_name'])}")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⬆️ Выше", callback_data=f"adm_move:{entry_id}:up"),
                InlineKeyboardButton(text="⬇️ Ниже", callback_data=f"adm_move:{entry_id}:down"),
            ],
            [InlineKeyboardButton(text="🗑 Удалить из очереди", callback_data=f"adm_rm:{entry_id}")],
            [InlineKeyboardButton(text="⬅️ К списку участников", callback_data=f"adm_members:{qid}")],
        ]
    )
    await safe_edit(message, "\n".join(lines), reply_markup=kb)


@router.callback_query(F.data.startswith("adm_member:"))
async def cb_adm_member(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    entry_id = int(callback.data.split(":")[1])
    await show_single_member(callback.message, entry_id)
    await callback.answer()


@router.callback_query(F.data.startswith("adm_move:"))
async def cb_adm_move(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    _, entry_id_str, direction = callback.data.split(":")
    entry_id = int(entry_id_str)
    move_entry(entry_id, -1 if direction == "up" else 1)
    await show_single_member(callback.message, entry_id)
    await callback.answer()


@router.callback_query(F.data.startswith("adm_rm:"))
async def cb_adm_rm(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    entry_id = int(callback.data.split(":")[1])
    qid = remove_entry_by_id(entry_id)
    if qid is not None:
        await show_members_editor(callback.message, qid)
    await callback.answer("Участник удалён.")


# --- Удаление очереди ---
@router.callback_query(F.data.startswith("adm_del_ask:"))
async def cb_adm_del_ask(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    qid = int(callback.data.split(":")[1])
    queue = get_queue(qid)
    if not queue:
        await callback.answer("Очередь уже удалена.", show_alert=True)
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"adm_del_confirm:{qid}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"adm_q:{qid}")],
        ]
    )
    await safe_edit(
        callback.message,
        f"Точно удалить очередь <b>«{html.escape(queue['title'])}»</b> и весь её список?",
        reply_markup=kb,
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm_del_confirm:"))
async def cb_adm_del_confirm(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    qid = int(callback.data.split(":")[1])
    delete_queue(qid)
    await safe_edit(callback.message, "🗑 Очередь удалена.", reply_markup=admin_menu_kb())
    await callback.answer()


# ================= ЗАПУСК =================
async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    if not BOT_TOKEN:
        raise RuntimeError("Не найдена переменная окружения BOT_TOKEN!")

    init_db()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)

    notifier_task = asyncio.create_task(check_opening_queues(bot))
    print("Бот запущен! Для остановки нажмите Ctrl+C.")
    try:
        await dp.start_polling(bot)
    finally:
        notifier_task.cancel()
        try:
            await notifier_task
        except asyncio.CancelledError:
            pass
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())