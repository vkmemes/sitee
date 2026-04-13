from __future__ import annotations
import json
import re
import logging
import datetime
import httpx
from bs4 import BeautifulSoup
from typing import List, Optional, Dict, Tuple
from pydantic import BaseModel, Field
from enum import Enum

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("Core")

SCHEDULE_FILE = 'schedule.json'
REPLACEMENTS_URLS =[
    "https://menu.sttec.yar.ru/timetable/rasp_first.html",
    "https://menu.sttec.yar.ru/timetable/rasp_second.html"
]
STOP_WORDS_CANCEL =["отмена", "нет пары", "самоподготовка", "праздник", "❌", "снято"]

class LessonType(str, Enum):
    REGULAR = "regular"
    REPLACEMENT = "replacement"
    CANCELLATION = "cancellation"
    ADDED = "added"

class Lesson(BaseModel):
    pair_num: int
    subject: str
    teacher: Optional[str] = None
    room: Optional[str] = None
    raw_text: str = ""
    type: LessonType = LessonType.REGULAR
    original_subject: Optional[str] = None 
    is_subgroup: bool = False 

class DaySchedule(BaseModel):
    date: datetime.date
    week_type: str
    lessons: List[Lesson] =[]
    has_replacements: bool = False
    last_updated: datetime.datetime = Field(default_factory=datetime.datetime.now)

class ScheduleManager:
    def __init__(self):
        self._base_schedule: Dict[str, Dict] = {}
        self._replacements_cache: List[dict] =[]
        self._cache_date: Optional[datetime.date] = None
        self._last_fetch: datetime.datetime = datetime.datetime.min
        self.load_base_schedule()

    def load_base_schedule(self):
        try:
            with open(SCHEDULE_FILE, 'r', encoding='utf-8') as f:
                self._base_schedule = json.load(f)
            logger.info(f"✅ База загружена: {len(self._base_schedule)} групп.")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки базы: {e}")
            self._base_schedule = {}

    def _normalize_name(self, name: str) -> str:
        """Убирает пробелы и регистр для поиска"""
        return name.strip().lower().replace(" ", "").replace("-", "")

    def _parse_replacement_lesson(self, raw_text: str) -> Tuple[str, str]:
        """
        Парсит строку с сайта (HTML).
        Использует Regex для поиска ФИО.
        """
        if not raw_text: return "", ""
        
        # Чистим неразрывные пробелы
        clean_text = raw_text.replace('\xa0', ' ').replace('\t', ' ').strip()
        
        # Паттерн: Фамилия + Инициалы
        teacher_pattern = r'([A-ZА-ЯЁ][a-zа-яё]+(?:-[A-ZА-ЯЁ][a-zа-яё]+)?\s+[A-ZА-ЯЁ]\.\s?[A-ZА-ЯЁ]\.?)'
        teachers = re.findall(teacher_pattern, clean_text)
        
        if teachers:
            unique_teachers = sorted(list(set(teachers)))
            teacher_str = ", ".join(unique_teachers)
            
            subject = clean_text
            for t in unique_teachers:
                subject = subject.replace(t, "")
            
            subject = re.sub(r'\s+', ' ', subject).strip(' .,;')
            return subject, teacher_str

        # Fallback на скобки
        match_brackets = re.search(r'\(([^)]+)\)', clean_text)
        if match_brackets:
            content = match_brackets.group(1).strip()
            stop_words =['каб', 'ауд', 'подгр', 'смена', 'снято', 'отмена']
            if len(content) > 2 and not any(w in content.lower() for w in stop_words):
                return clean_text.replace(match_brackets.group(0), "").strip(), content

        return clean_text, "Не указан"

    def _parse_pair_nums(self, raw_num: str) -> List[int]:
        """Превращает '2,3' или '2-3' в список [2, 3]"""
        nums = set()
        clean_raw = re.sub(r'[,;&-]', ' ', raw_num)
        for part in clean_raw.split():
            if part.isdigit():
                nums.add(int(part))
        return sorted(list(nums))

    def _parse_pair_nums_from_json(self, raw_num: str) -> List[int]:
        """Обрабатывает номера пар из JSON, включая '2 знам' и '2 четн'"""
        nums = set()
        # Извлекаем число из строк вида "2 знам", "2 четн", "2"
        match = re.search(r'(\d+)', raw_num)
        if match:
            nums.add(int(match.group(1)))
        return sorted(list(nums))

    def _extract_date(self, text: str) -> Optional[datetime.date]:
        """Парсит дату из заголовка сайта"""
        months = {'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12}
        match = re.search(r'(\d{1,2})\s+([а-я]+)(?:\s+(\d{4}))?', text.lower())
        if match:
            day, month_str, year_str = match.groups()
            month = months.get(month_str)
            if month:
                year = int(year_str) if year_str else datetime.date.today().year
                if datetime.date.today().month == 12 and month == 1: year += 1
                elif datetime.date.today().month == 1 and month == 12: year -= 1
                return datetime.date(year, month, int(day))
        return None

    async def update_replacements(self, force: bool = False):
        """
        Скачивает замены с сайта.
        Возвращает: (список_замен, флаг_изменений)
        """
        now = datetime.datetime.now()
        if not force and (now - self._last_fetch).total_seconds() < 300:
            return self._replacements_cache, False

        new_cache =[]
        parsed_date = None
        is_changed = False

        async with httpx.AsyncClient() as client:
            for url in REPLACEMENTS_URLS:
                try:
                    resp = await client.get(url, timeout=15)
                    if resp.status_code != 200: continue
                    
                    soup = BeautifulSoup(resp.content, 'html.parser')
                    
                    if not parsed_date:
                        for tag in soup.find_all(['div', 'b', 'strong', 'h1', 'h2']):
                            if tag.text and 'изменения' in tag.text.lower():
                                d = self._extract_date(tag.text)
                                if d: parsed_date = d; break

                    table = soup.find('table')
                    if not table: continue

                    for row in table.find_all('tr'):
                        cols = row.find_all('td')
                        if len(cols) < 6: continue 
                        
                        group_col = 1 if len(cols[0].text) < 3 else 0
                        subj_idx = group_col + 3
                        room_idx = group_col + 4
                        
                        if subj_idx >= len(cols): continue

                        raw_groups = cols[group_col].text.strip()
                        raw_pair = cols[group_col+1].text.strip()
                        subject_new = cols[subj_idx].text.strip()
                        room = cols[room_idx].text.strip() if room_idx < len(cols) else ""

                        pair_nums = self._parse_pair_nums(raw_pair)
                        groups_list =[g.strip() for g in raw_groups.split('/')]

                        for p_num in pair_nums:
                            for g in groups_list:
                                new_cache.append({
                                    "groups": [g],
                                    "pair_num": p_num,
                                    "subject_new": subject_new,
                                    "room": room
                                })
                except Exception as e:
                    logger.error(f"Ошибка URL {url}: {e}")

        if (self._cache_date != parsed_date) or (len(new_cache) != len(self._replacements_cache)):
            is_changed = True

        self._replacements_cache = new_cache
        self._cache_date = parsed_date or datetime.date.today()
        self._last_fetch = now
        
        return new_cache, is_changed

    def get_schedule(self, group_name: str, target_date: datetime.date) -> DaySchedule:
        is_numerator = target_date.isocalendar()[1] % 2 == 0
        week_type_str = "числитель" if is_numerator else "знаменатель"
        day_names =["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        day_of_week = day_names[target_date.weekday()]

        raw_schedule = self._base_schedule.get(group_name, {}).get(day_of_week, [])
        final_lessons: Dict[int, Lesson] = {} 
        
        # 1. ОБРАБОТКА БАЗЫ (JSON)
        for item in raw_schedule:
            # Обрабатываем номер пары из JSON
            raw_pair = item.get('пара', '')
            pair_nums = self._parse_pair_nums_from_json(raw_pair)
            
            # Определяем тип пары (знаменатель/числитель)
            is_week_type_match = True
            if 'знам' in raw_pair.lower():
                is_week_type_match = not is_numerator  # знаменатель
            elif 'четн' in raw_pair.lower() or 'четная' in raw_pair.lower():
                is_week_type_match = is_numerator  # числитель
            
            if not is_week_type_match:
                continue
            
            subj = item.get('предмет', '')
            teach = item.get('преподаватель', '')
            room = item.get('кабинет', '')
            
            for p_num in pair_nums:
                lesson = Lesson(
                    pair_num=p_num,
                    subject=subj,
                    teacher=teach,
                    room=room,
                    raw_text=f"{subj} {teach}",
                    is_subgroup=("подгр" in subj.lower())
                )
                final_lessons[p_num] = lesson

        # 2. НАЛОЖЕНИЕ ЗАМЕН (HTML)
        has_replacements = False
        if self._cache_date == target_date:
            target_parts = set(self._normalize_name(p) for p in group_name.split('/'))
            
            relevant_reps =[]
            for r in self._replacements_cache:
                rep_parts = set(self._normalize_name(g) for g in r['groups'])
                if not target_parts.isdisjoint(rep_parts):
                    relevant_reps.append(r)
            
            for r in relevant_reps:
                p = r['pair_num']
                txt = r['subject_new']
                
                contains_cancel = any(w in txt.lower() for w in STOP_WORDS_CANCEL)
                is_complex = len(txt) > 15 or "п/гр" in txt.lower()
                
                l_type = LessonType.CANCELLATION if (contains_cancel and not is_complex) else LessonType.REPLACEMENT

                subj_new, teach_new = self._parse_replacement_lesson(txt)
                
                if p in final_lessons:
                    target = final_lessons[p]
                    target.original_subject = target.subject
                    target.type = l_type
                    has_replacements = True
                    
                    if l_type == LessonType.CANCELLATION:
                        target.subject = "ОТМЕНА"
                        target.teacher = ""
                        target.room = ""
                    else:
                        target.subject = subj_new
                        target.teacher = teach_new
                        target.room = r['room']
                else:
                    if l_type != LessonType.CANCELLATION:
                        has_replacements = True
                        final_lessons[p] = Lesson(
                            pair_num=p,
                            subject=subj_new,
                            teacher=teach_new,
                            room=r['room'],
                            type=LessonType.ADDED,
                            original_subject="(Окно)"
                        )

        sorted_lessons = sorted(final_lessons.values(), key=lambda x: x.pair_num)
        return DaySchedule(date=target_date, week_type=week_type_str, lessons=sorted_lessons, has_replacements=has_replacements)

core = ScheduleManager()