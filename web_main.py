from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.templating import Jinja2Templates
from starlette.requests import Request
import uvicorn
import datetime
import hmac
import hashlib
import json
from urllib.parse import parse_qsl, unquote, quote
import logging
import httpx
from typing import Union

from core import core, LessonType
from database import db

BOT_TOKEN = "8195041032:AAGvHDGKzOnLYCL-TksT63znzieji9cRvdk"
BOT_USERNAME = "ygkschedulebot" 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Web")

templates = Jinja2Templates(directory="templates")
templates.env.globals['quote'] = quote
templates.env.globals['LessonType'] = LessonType

async def on_startup():
    await db.init_db()
    await core.update_replacements(force=True)

def check_webapp_signature(token: str, init_data: str) -> Union[dict, bool]:
    try:
        parsed_data = dict(parse_qsl(init_data))
        if 'user' in parsed_data:
            return json.loads(parsed_data['user'])
        return False
    except: return False

async def list_groups(request: Request):
    groups = sorted(list(core._base_schedule.keys()))
    return templates.TemplateResponse("group_list_template.html", {"request": request, "groups": groups})

async def view_schedule(request: Request):
    group = unquote(request.path_params['group_name'])
    
    today = datetime.date.today()
    
    # 1. Обработка даты из параметров запроса (?date=YYYY-MM-DD)
    date_str = request.query_params.get("date")
    if date_str:
        try:
            target_date = datetime.date.fromisoformat(date_str)
        except ValueError:
            target_date = today
    else:
        # Резервный вариант с offset, если параметра date нет
        try: 
            offset = int(request.query_params.get("offset", 0))
        except: 
            offset = 0
        target_date = today + datetime.timedelta(days=offset)

    # 2. Получаем готовый объект DaySchedule из core.py
    day_schedule = core.get_schedule(group, target_date)

    # 3. Считаем даты для кнопок навигации и свайпов (вчера/завтра)
    prev_date = (target_date - datetime.timedelta(days=1)).isoformat()
    next_date = (target_date + datetime.timedelta(days=1)).isoformat()

    # 4. Рендерим шаблон, передавая ровно те переменные, которые он ждет
    return templates.TemplateResponse("schedule_view_template.html", {
        "request": request,
        "group_name": group,
        "schedule": day_schedule,     
        "target_date": target_date,   
        "is_today": target_date == today,
        "prev_date": prev_date,
        "next_date": next_date
    })

async def view_replacements(request: Request):
    await core.update_replacements()
    repls = sorted(core._replacements_cache, key=lambda x: (x['groups'][0], x['pair_num']))
    return templates.TemplateResponse("replacements_view_template.html", {"request": request, "replacements": repls, "clean_date": "-", "cache_time": "-"})

async def homework_page(request: Request):
    groups = sorted(list(core._base_schedule.keys()))
    return templates.TemplateResponse("homework_form.html", {"request": request, "groups": groups})

async def headman_page(request: Request):
    groups = sorted(list(core._base_schedule.keys()))
    return templates.TemplateResponse("headman_panel.html", {"request": request, "groups": groups})

async def api_get_subjects(request: Request):
    try:
        body = await request.json()
        d = datetime.date.fromisoformat(body['date'])
        sched = core.get_schedule(body['group'], d)
        subjs =[]
        seen = set()
        for l in sched.lessons:
            if l.type.value == 'cancellation': continue
            s = l.subject
            if s not in seen: subjs.append(s); seen.add(s)
        return JSONResponse({"subjects": subjs})
    except Exception as e: return JSONResponse({"detail": str(e)}, 500)

async def api_save_homework(request: Request):
    try:
        body = await request.json()
        auth = body.get('auth', {})
        if not await db.check_pin(auth.get('group'), auth.get('pin')): return JSONResponse({"detail": "PIN Error"}, 403)
        await db.add_homework(auth.get('group'), datetime.date.fromisoformat(body['date']), body['subject'], body['text'], 777, body.get('mode', 'append'))
        return JSONResponse({"status": "ok"})
    except Exception as e: return JSONResponse({"detail": str(e)}, 500)

async def api_headman_login(request: Request):
    try:
        body = await request.json()
        grp = body.get('group'); pin = body.get('pin')
        if not await db.check_pin(grp, pin): return JSONResponse({"detail": "PIN Error"}, 403)
        async with db.session_factory() as session:
            from database import GroupSettings; from sqlalchemy import select
            s = (await session.execute(select(GroupSettings).where(GroupSettings.group_name == grp))).scalar_one_or_none()
            auto = s.autoset_enabled if s else False
        st = await db.get_students_by_group(grp)
        return JSONResponse({"status": "ok", "data": {"autoset": auto, "students":[{"id": x.id, "name": x.full_name, "username": x.tg_username} for x in st]}})
    except Exception as e: return JSONResponse({"detail": str(e)}, 500)

async def api_save_headman_data(request: Request):
    try:
        body = await request.json()
        auth = body.get('auth', {})
        grp = auth.get('group'); pin = auth.get('pin')
        if not await db.check_pin(grp, pin): return JSONResponse({"detail": "PIN Error"}, 403)
        data = body.get('data', {})
        await db.toggle_autoset(grp, data['autoset'])
        for i, s in enumerate(data['students']):
            if s['name'].strip(): await db.add_or_update_student(grp, s['name'], s['username'], order=i)
        return JSONResponse({"status": "ok"})
    except Exception as e: return JSONResponse({"detail": str(e)}, 500)

async def api_delete_student(request: Request):
    try:
        body = await request.json()
        auth = body.get('auth', {})
        grp = auth.get('group'); pin = auth.get('pin')
        if not await db.check_pin(grp, pin): return JSONResponse({"detail": "PIN Error"}, 403)
        await db.delete_student(body.get('student_id'), grp)
        return JSONResponse({"status": "ok"})
    except Exception as e: return JSONResponse({"detail": str(e)}, 500)

async def api_notify_duty(request: Request):
    try:
        body = await request.json()
        auth = body.get('auth', {})
        grp = auth.get('group'); pin = auth.get('pin')
        if not await db.check_pin(grp, pin): return JSONResponse({"detail": "PIN Error"}, 403)
        uname = body.get('username').replace("@","").strip()
        async with db.session_factory() as session:
            from database import Student; from sqlalchemy import select
            s = (await session.execute(select(Student).where(Student.tg_username == uname))).scalar_one_or_none()
            tid = s.telegram_id if s else None
        if not tid: return JSONResponse({"detail": "Студент не найден в боте"}, 404)
        async with httpx.AsyncClient() as client:
            await client.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={
                "chat_id": tid, "text": "📣 **Вас назначили дежурным вручную!**\nПожалуйста, заполните ДЗ.", "parse_mode": "Markdown",
                "reply_markup": {"inline_keyboard": [[{"text": "📝 Заполнить", "web_app": {"url": "https://gradygk.ru/homework"}}]]}
            })
        return JSONResponse({"status": "ok"})
    except Exception as e: return JSONResponse({"detail": str(e)}, 500)

async def api_get_schedule(request: Request):
    group = unquote(request.path_params['group_name'])
    try: d = datetime.date.fromisoformat(request.query_params.get('date'))
    except: d = datetime.date.today()
    return JSONResponse(core.get_schedule(group, d).model_dump(mode='json'))

async def api_kwgt(request: Request):
    g = unquote(request.path_params['group_name'])
    await core.update_replacements()
    d = core._cache_date or datetime.date.today()
    s = core.get_schedule(g, d)
    t = f"{d.strftime('%d.%m')} ({'Числ' if d.isocalendar()[1]%2==0 else 'Знам'})<br>"
    if not s.lessons: t += "Пар нет<br>"
    else:
        for l in s.lessons:
            ic = "🚫 " if l.type.value=='cancellation' else ("🔄 " if l.type.value=='replacement' else "")
            t += f"{ic}{l.pair_num}. {l.subject}{f' ({l.room})' if l.room else ''}<br>"
    return PlainTextResponse(t)

routes =[
    Route('/', list_groups), Route('/list_groups', list_groups),
    Route('/schedule/{group_name:path}', view_schedule), Route('/replacements', view_replacements),
    Route('/homework', homework_page), Route('/api/homework', api_save_homework, methods=['POST']),
    Route('/headman', headman_page), Route('/api/headman/login', api_headman_login, methods=['POST']),
    Route('/api/headman/save', api_save_headman_data, methods=['POST']),
    Route('/api/headman/delete', api_delete_student, methods=['POST']),
    Route('/api/headman/notify', api_notify_duty, methods=['POST']),
    Route('/api/get_subjects', api_get_subjects, methods=['POST']),
    Route('/api/schedule/{group_name:path}', api_get_schedule),
    Route('/api/kwgt/schedule/{group_name:path}', api_kwgt),
]

app_web = Starlette(debug=True, routes=routes, on_startup=[on_startup])
if __name__ == "__main__": uvicorn.run(app_web, host="127.0.0.1", port=5000)