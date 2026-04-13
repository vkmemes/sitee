import logging
import datetime
from typing import Optional, List, Dict
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, Boolean, DateTime, Date, func, select, BigInteger, update

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DB")

DATABASE_URL = "sqlite+aiosqlite:///sttec.db"

class Base(AsyncAttrs, DeclarativeBase):
    pass

# --- МОДЕЛИ ---
class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    username: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    full_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    group_name: Mapped[Optional[str]] = mapped_column(String(50), index=True, nullable=True)
    role: Mapped[str] = mapped_column(String(20), default="student") 
    referral_source: Mapped[str] = mapped_column(String(50), default="organic")
    is_active: Mapped[bool] = mapped_column(default=True)
    sub_check_time: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    last_notify_date: Mapped[datetime.date] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class GroupSettings(Base):
    __tablename__ = "group_settings"
    group_name: Mapped[str] = mapped_column(String(50), primary_key=True)
    hw_enabled: Mapped[bool] = mapped_column(default=False)
    autoset_enabled: Mapped[bool] = mapped_column(default=False)
    last_queue_index: Mapped[int] = mapped_column(default=0)
    chat_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

class GroupPin(Base):
    __tablename__ = "group_pins"
    group_name: Mapped[str] = mapped_column(String(50), primary_key=True)
    pin_code: Mapped[str] = mapped_column(String(50))

class Student(Base):
    __tablename__ = "students"
    id: Mapped[int] = mapped_column(primary_key=True)
    group_name: Mapped[str] = mapped_column(String(50), index=True)
    full_name: Mapped[str] = mapped_column(String(100))
    tg_username: Mapped[Optional[str]] = mapped_column(String(100)) 
    telegram_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    queue_order: Mapped[int] = mapped_column(default=0)
    is_sick: Mapped[bool] = mapped_column(default=False)

class Homework(Base):
    __tablename__ = "homeworks"
    id: Mapped[int] = mapped_column(primary_key=True)
    group_name: Mapped[str] = mapped_column(String(50), index=True)
    target_date: Mapped[datetime.date] = mapped_column()
    subject: Mapped[str] = mapped_column(String(200), nullable=True)
    text: Mapped[str] = mapped_column(String(1000))
    created_by: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

# Log tables dummies
class MessageLog(Base):
    __tablename__ = "message_logs"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    chat_type: Mapped[str] = mapped_column(String(20))
    content_type: Mapped[str] = mapped_column(String(20))
    timestamp: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class LatencyLog(Base):
    __tablename__ = "latency_logs"
    id: Mapped[int] = mapped_column(primary_key=True)
    duration_ms: Mapped[float] = mapped_column()
    timestamp: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

# --- МЕНЕДЖЕР ---
class Database:
    def __init__(self):
        self.engine = create_async_engine(DATABASE_URL, echo=False)
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)

    async def init_db(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    # === USERS ===
    async def register_user(self, telegram_id: int, username: str = None, full_name: str = None, source: str = "organic"):
        async with self.session_factory() as session:
            stmt = select(User).where(User.telegram_id == telegram_id)
            user = (await session.execute(stmt)).scalar_one_or_none()
            if user:
                user.is_active = True
                if username: user.username = username
                if full_name: user.full_name = full_name
                if username:
                    clean = username.replace("@", "")
                    s_stmt = select(Student).where(Student.tg_username == clean)
                    st = (await session.execute(s_stmt)).scalar_one_or_none()
                    if st: st.telegram_id = telegram_id
            else:
                session.add(User(telegram_id=telegram_id, username=username, full_name=full_name, referral_source=source))
            await session.commit()

    async def set_group(self, telegram_id: int, group: str):
        async with self.session_factory() as session:
            stmt = select(User).where(User.telegram_id == telegram_id)
            user = (await session.execute(stmt)).scalar_one_or_none()
            if user: user.group_name = group
            else: session.add(User(telegram_id=telegram_id, group_name=group))
            await session.commit()

    async def mark_inactive(self, telegram_id: int):
        async with self.session_factory() as session:
            await session.execute(update(User).where(User.telegram_id == telegram_id).values(is_active=False))
            await session.commit()

    # === SUBSCRIPTION ===
    async def is_subscription_cached(self, telegram_id: int, ttl_minutes: int = 10) -> bool:
        async with self.session_factory() as session:
            stmt = select(User.sub_check_time).where(User.telegram_id == telegram_id)
            last = (await session.execute(stmt)).scalar_one_or_none()
            if not last: return False
            now = datetime.datetime.now(datetime.timezone.utc) if last.tzinfo else datetime.datetime.now()
            return (now - last).total_seconds() < (ttl_minutes * 60)

    async def update_sub_check(self, telegram_id: int):
        async with self.session_factory() as session:
            stmt = select(User).where(User.telegram_id == telegram_id)
            user = (await session.execute(stmt)).scalar_one_or_none()
            if user: user.sub_check_time = func.now()
            else: session.add(User(telegram_id=telegram_id, sub_check_time=func.now()))
            await session.commit()

    # === ROLES ===
    async def get_user_role(self, telegram_id: int) -> str:
        async with self.session_factory() as session:
            res = await session.execute(select(User.role).where(User.telegram_id == telegram_id))
            return res.scalar_one_or_none() or "student"

    async def set_user_role(self, telegram_id: int, role: str):
        async with self.session_factory() as session:
            await session.execute(update(User).where(User.telegram_id == telegram_id).values(role=role))
            await session.commit()

    # === HOMEWORK ===
    async def add_homework(self, group_name: str, target_date: datetime.date, subject: str, text: str, author_id: int, mode: str = "append"):
        async with self.session_factory() as session:
            stmt = select(Homework).where(
                Homework.group_name == group_name, 
                Homework.target_date == target_date,
                Homework.subject == subject 
            )
            existing = (await session.execute(stmt)).scalar_one_or_none()
            
            if existing:
                if mode == "overwrite": existing.text = text
                else: existing.text = existing.text + "\n\n" + text
                existing.created_by = author_id
            else:
                session.add(Homework(group_name=group_name, target_date=target_date, subject=subject, text=text, created_by=author_id))
            await session.commit()

    async def get_homework(self, group_name: str, target_date: datetime.date) -> Dict[str, str]:
        async with self.session_factory() as session:
            stmt = select(Homework).where(Homework.group_name == group_name, Homework.target_date == target_date)
            results = (await session.execute(stmt)).scalars().all()
            hw_map = {}
            for hw in results:
                key = hw.subject if hw.subject else "Общее"
                hw_map[key] = hw.text
            return hw_map

    async def check_homework_exists(self, group_name: str, target_date: datetime.date) -> bool:
        async with self.session_factory() as session:
            stmt = select(func.count(Homework.id)).where(Homework.group_name == group_name, Homework.target_date == target_date)
            return (await session.scalar(stmt)) > 0

    # === PIN SYSTEM ===
    async def set_group_pin(self, group_name: str, pin: str):
        async with self.session_factory() as session:
            stmt = select(GroupPin).where(GroupPin.group_name == group_name)
            obj = (await session.execute(stmt)).scalar_one_or_none()
            if obj: obj.pin_code = pin
            else: session.add(GroupPin(group_name=group_name, pin_code=pin))
            await session.commit()

    async def get_group_pin(self, group_name: str) -> Optional[str]:
        async with self.session_factory() as session:
            return await session.scalar(select(GroupPin.pin_code).where(GroupPin.group_name == group_name))

    async def check_pin(self, group_name: str, input_pin: str) -> bool:
        real = await self.get_group_pin(group_name)
        if not real: return False
        return str(real).strip() == str(input_pin).strip()

    # === HEADMAN & STUDENTS ===
    async def get_headman_id(self, group_name: str) -> Optional[int]:
        async with self.session_factory() as session:
            stmt = select(User.telegram_id).where(User.group_name == group_name, User.role == 'headman')
            return await session.scalar(stmt)

    async def add_or_update_student(self, group: str, name: str, username: str, order: int):
        async with self.session_factory() as session:
            clean = username.replace("@", "").strip() if username else None
            stmt = select(Student).where(Student.group_name == group, Student.full_name == name)
            student = (await session.execute(stmt)).scalar_one_or_none()
            
            tg_id = None
            if clean:
                u_stmt = select(User.telegram_id).where(User.username == clean)
                tg_id = (await session.execute(u_stmt)).scalar_one_or_none()

            if student:
                student.tg_username = clean
                student.queue_order = order
                if tg_id: student.telegram_id = tg_id
            else:
                session.add(Student(group_name=group, full_name=name, tg_username=clean, queue_order=order, telegram_id=tg_id))
            await session.commit()

    async def register_student_self(self, telegram_id: int, group_name: str, full_name: str, username: str):
        async with self.session_factory() as session:
            stmt = select(Student).where(Student.telegram_id == telegram_id)
            student = (await session.execute(stmt)).scalar_one_or_none()
            clean = username.replace("@", "") if username else None
            if student:
                student.full_name = full_name; student.group_name = group_name; student.tg_username = clean
            else:
                ghost = (await session.execute(select(Student).where(Student.group_name == group_name, func.lower(Student.full_name) == full_name.lower()))).scalar_one_or_none()
                if ghost: ghost.telegram_id = telegram_id; ghost.tg_username = clean
                else:
                    mx = (await session.execute(select(func.max(Student.queue_order)).where(Student.group_name == group_name))).scalar_one() or 0
                    session.add(Student(group_name=group_name, full_name=full_name, tg_username=clean, telegram_id=telegram_id, queue_order=mx+1))
            await session.commit()

    async def delete_student(self, student_id: int, group_name: str):
        async with self.session_factory() as session:
            stmt = select(Student).where(Student.id == student_id, Student.group_name == group_name)
            student = (await session.execute(stmt)).scalar_one_or_none()
            if student:
                await session.delete(student)
                await session.commit()
                return True
            return False

    async def get_students_by_group(self, group: str):
        async with self.session_factory() as session:
            return (await session.execute(select(Student).where(Student.group_name == group).order_by(Student.queue_order))).scalars().all()

    async def toggle_autoset(self, group: str, status: bool):
        async with self.session_factory() as session:
            stmt = select(GroupSettings).where(GroupSettings.group_name == group)
            s = (await session.execute(stmt)).scalar_one_or_none()
            if s: s.autoset_enabled = status
            else: session.add(GroupSettings(group_name=group, autoset_enabled=status))
            await session.commit()

    async def get_autoset_groups(self) -> List[str]:
        async with self.session_factory() as session:
            return list((await session.execute(select(GroupSettings.group_name).where(GroupSettings.autoset_enabled == True))).scalars().all())

    async def get_active_hw_groups(self) -> List[str]:
        async with self.session_factory() as session:
            return list((await session.execute(select(GroupSettings.group_name).where(GroupSettings.hw_enabled == True))).scalars().all())

    async def get_next_duty_students(self, group: str, count: int = 2) -> List[Student]:
        async with self.session_factory() as session:
            settings = (await session.execute(select(GroupSettings).where(GroupSettings.group_name == group))).scalar_one_or_none()
            if not settings: return[]
            students = (await session.execute(select(Student).where(Student.group_name == group).order_by(Student.queue_order))).scalars().all()
            if not students: return []
            
            selected =[]
            attempts = 0
            current_idx = settings.last_queue_index
            
            while len(selected) < count and attempts < len(students) * 2:
                cand = students[current_idx % len(students)]
                if not cand.is_sick: selected.append(cand)
                current_idx += 1
                attempts += 1
            
            settings.last_queue_index = current_idx % len(students)
            await session.commit()
            return selected

    async def get_next_duty_students_readonly(self, group: str, count: int = 2) -> List[Student]:
        async with self.session_factory() as session:
            settings = (await session.execute(select(GroupSettings).where(GroupSettings.group_name == group))).scalar_one_or_none()
            if not settings: return[]
            students = (await session.execute(select(Student).where(Student.group_name == group).order_by(Student.queue_order))).scalars().all()
            if not students: return[]
            
            sel =[]
            cur = settings.last_queue_index
            attempts = 0
            
            while len(sel) < count and attempts < len(students) * 2:
                cand = students[cur % len(students)]
                if not cand.is_sick: sel.append(cand)
                cur += 1
                attempts += 1
            
            return sel

    async def get_student_by_tg_id(self, tg_id: int) -> Optional[Student]:
        async with self.session_factory() as session:
            return (await session.execute(select(Student).where(Student.telegram_id == tg_id))).scalar_one_or_none()

    async def set_student_sick(self, student_id: int, is_sick: bool):
        async with self.session_factory() as session:
            await session.execute(update(Student).where(Student.id == student_id).values(is_sick=is_sick))
            await session.commit()

    async def reset_sick_flags(self):
        async with self.session_factory() as session:
            await session.execute(update(Student).values(is_sick=False))
            await session.commit()

    # === MISC & NOTIFICATIONS ===
    async def get_all_unique_groups(self) -> List[str]:
        async with self.session_factory() as session:
            return list((await session.execute(select(User.group_name).where(User.is_active == True, User.group_name.is_not(None)).distinct())).scalars().all())

    async def get_users_to_notify(self, group_name: str) -> List[int]:
        today = datetime.date.today()
        async with self.session_factory() as session:
            return list((await session.execute(select(User.telegram_id).where(User.group_name == group_name, User.is_active == True, (User.last_notify_date == None) | (User.last_notify_date < today)))).scalars().all())

    async def update_notify_date(self, telegram_id: int):
        today = datetime.date.today()
        async with self.session_factory() as session:
            await session.execute(update(User).where(User.telegram_id == telegram_id).values(last_notify_date=today))
            await session.commit()
            
    async def get_detailed_stats(self) -> Dict:
        async with self.session_factory() as session:
            total = await session.scalar(select(func.count(User.id)))
            active = await session.scalar(select(func.count(User.id)).where(User.is_active == True))
            return {"total_users": total or 0, "active_db": active or 0}

    async def get_users_for_broadcast(self, mode: str, group_filter: str = None) -> List[int]:
        async with self.session_factory() as session:
            query = select(User.telegram_id)
            if mode == 'active': query = query.where(User.is_active == True)
            elif mode == 'inactive': query = query.where(User.is_active == False)
            if group_filter: query = query.where(User.group_name == group_filter)
            return list((await session.execute(query)).scalars().all())

    async def log_message(self, u, c, t): pass
    async def log_latency(self, d): pass

db = Database()