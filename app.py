# --- TỰ ĐỘNG NÂNG CẤP THƯ VIỆN AI NẾU CŨ (FIX LỖI 404) ---
import subprocess
import sys
try:
    import google.generativeai as genai
    import pkg_resources
    # Kiểm tra version, nếu thấp hơn 0.8.0 thì update ngay lập tức
    ver = pkg_resources.get_distribution("google-generativeai").version
    if ver < "0.8.0":
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "google-generativeai"])
        import google.generativeai as genai
except:
    pass 

import streamlit as st
import pandas as pd
import sqlite3
import hashlib
import datetime
import unidecode
import time
import os
import zipfile
import glob
# Thử import thư viện Cloud, nếu lỗi thì chạy chế độ Offline
try:
    from google.cloud import firestore
    from google.oauth2 import service_account
    CLOUD_AVAILABLE = True
except ImportError:
    CLOUD_AVAILABLE = False

import json

# --- CẤU HÌNH ỨNG DỤNG ---
st.set_page_config(
    page_title="Hệ thống BHXH Chuyên Nghiệp",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================================================================
# 🔑 CẤU HÌNH HỆ THỐNG
# ==============================================================================
HARDCODED_API_KEY = "AIzaSyBd6MNZdWTsJiTy1yrrWK4G2PsltqFV6eg" 
ZALO_PHONE_NUMBER = "0986053006" 

# CÁC HẰNG SỐ TÍNH BHXH TỰ NGUYỆN (LUẬT 2025)
CHUAN_NGHEO = 1500000 
LUONG_CO_SO = 2340000 
MAX_MUC_DONG = 20 * LUONG_CO_SO 
TY_LE_DONG = 0.22 

# Mức hỗ trợ mới nhất (2025)
HO_TRO_NGHEO = 0.50     # 50%
HO_TRO_CAN_NGHEO = 0.40 # 40%
HO_TRO_DAN_TOC = 0.30   # 30%
HO_TRO_KHAC = 0.20      # 20%

# Tên file dữ liệu
EXCEL_FILE = 'aaa.xlsb'
DB_FILE = 'bhxh_data.db'
ZIP_PART_PREFIX = 'bhxh_data.zip.' 
USER_DB_LOCAL = 'users_local.db' # Database dự phòng nếu không có Cloud

# --- 1. KẾT NỐI DATABASE (CLOUD + LOCAL FALLBACK) ---
@st.cache_resource
def get_firestore_db():
    if not CLOUD_AVAILABLE: return None
    try:
        if "gcp_service_account" in st.secrets:
            key_dict = dict(st.secrets["gcp_service_account"])
            creds = service_account.Credentials.from_service_account_info(key_dict)
            return firestore.Client(credentials=creds, project=key_dict["project_id"])
    except: return None
    return None

def init_local_db():
    """Tạo DB Local dự phòng"""
    conn = sqlite3.connect(USER_DB_LOCAL, check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)''')
    c.execute("SELECT * FROM users WHERE username = 'admin'")
    if not c.fetchone():
        c.execute("INSERT INTO users VALUES (?, ?, ?)", ('admin', make_hashes('admin123'), 'admin'))
    conn.commit()
    return conn

def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

# --- QUẢN LÝ USER ---
def create_user(username, password, role):
    # Thử lưu Cloud
    db = get_firestore_db()
    if db:
        doc_ref = db.collection("users").document(username)
        if doc_ref.get().exists: return False 
        doc_ref.set({"password": make_hashes(password), "role": role, "created_at": datetime.datetime.now()})
        return True
    
    # Nếu Cloud lỗi, lưu Local
    try:
        conn = init_local_db()
        conn.execute("INSERT INTO users VALUES (?, ?, ?)", (username, make_hashes(password), role))
        conn.commit(); conn.close()
        return True
    except: return False

def verify_login(username, password):
    # Thử Cloud
    db = get_firestore_db()
    if db:
        doc = db.collection("users").document(username).get()
        if doc.exists and doc.to_dict()["password"] == make_hashes(password): return doc.to_dict()["role"]
    
    # Thử Local
    conn = init_local_db()
    res = conn.execute("SELECT role FROM users WHERE username=? AND password=?", (username, make_hashes(password))).fetchone()
    conn.close()
    if res: return res[0]
    return None

def update_password(username, new_password):
    db = get_firestore_db()
    success = False
    if db: # Update Cloud
        try: 
            db.collection("users").document(username).update({"password": make_hashes(new_password)})
            success = True
        except: pass
    
    # Update Local luôn cho chắc
    try:
        conn = init_local_db()
        conn.execute("UPDATE users SET password=? WHERE username=?", (make_hashes(new_password), username))
        conn.commit(); conn.close()
        success = True
    except: pass
    return success

def delete_user(username):
    db = get_firestore_db()
    if db: db.collection("users").document(username).delete()
    
    conn = init_local_db()
    conn.execute("DELETE FROM users WHERE username=?", (username,))
    conn.commit(); conn.close()
    return True

def get_all_users():
    data = []
    # Lấy từ Cloud
    db = get_firestore_db()
    if db:
        try: 
            docs = db.collection("users").stream()
            for doc in docs: data.append({"source": "Cloud", "username": doc.id, **doc.to_dict()})
        except: pass
    
    # Lấy từ Local nếu Cloud trống hoặc lỗi
    if not data:
        conn = init_local_db()
        df = pd.read_sql("SELECT * FROM users", conn)
        conn.close()
        if not df.empty:
            df['source'] = 'Local'
            return df
            
    return pd.DataFrame(data)

# --- LOGS ---
def log_action(username, action, details=""):
    try:
        db = get_firestore_db()
        if db:
            vn_timezone = datetime.timezone(datetime.timedelta(hours=7))
            now_vn = datetime.datetime.now(vn_timezone)
            db.collection("logs").add({
                "timestamp": now_vn.strftime("%Y-%m-%d %H:%M:%S"),
                "sort_time": firestore.SERVER_TIMESTAMP,
                "username": username, "action": action, "details": str(details)
            })
    except: pass

def get_logs(limit=100):
    db = get_firestore_db()
    if not db: return pd.DataFrame()
    try:
        logs_ref = db.collection("logs").order_by("sort_time", direction=firestore.Query.DESCENDING).limit(limit)
        return pd.DataFrame([{"Thời gian": d.to_dict().get("timestamp"), "User": d.to_dict().get("username"), "Hành động": d.to_dict().get("action"), "Chi tiết": d.to_dict().get("details")} for d in logs_ref.stream()])
    except: return pd.DataFrame()

def init_admin_account():
    if "admin_checked" not in st.session_state:
        if not verify_login("admin", "admin123"): create_user("admin", "admin123", "admin")
        st.session_state["admin_checked"] = True

def render_zalo_widget():
    st.markdown(f"""<style>.z{{position:fixed;bottom:20px;right:20px;width:60px;height:60px;z-index:9999;animation:s 3s infinite}}@keyframes s{{0%,100%{{transform:rotate(0deg)}}10%,30%{{transform:rotate(10deg)}}20%,40%{{transform:rotate(-10deg)}}}}</style><a href="https://zalo.me/{ZALO_PHONE_NUMBER}" target="_blank" class="z"><img src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/91/Icon_of_Zalo.svg/1200px-Icon_of_Zalo.svg.png" width="100%"></a>""", unsafe_allow_html=True)

# --- 2. HỆ THỐNG AI THÔNG MINH (AUTO-DETECT) ---
def configure_ai():
    key = HARDCODED_API_KEY or st.session_state.get('user_api_key') or st.secrets.get("GOOGLE_API_KEY")
    if key: genai.configure(api_key=key); return True
    return False

# Cache để không phải dò model lại nhiều lần
@st.cache_resource(show_spinner=False)
def find_best_model():
    """Tự động tìm model hoạt động tốt nhất"""
    try:
        models = genai.list_models()
        # Ưu tiên Flash
        for m in models:
            if 'generateContent' in m.supported_generation_methods and 'flash' in m.name:
                return m.name
    except: pass
    
    # Fallback list
    candidates = ["gemini-1.5-flash", "gemini-1.5-pro", "gemini-pro", "gemini-1.0-pro"]
    for m in candidates:
        try:
            genai.GenerativeModel(m).generate_content("Hi")
            return m
        except: continue
    return None

def get_ai_response(prompt, role_desc="", stream=False):
    if not configure_ai(): return "⚠️ Lỗi: Chưa có API Key."
    
    model_name = find_best_model()
    if not model_name: return "⚠️ Không tìm thấy Model AI khả dụng. Vui lòng kiểm tra Key."
    
    full_prompt = f"{role_desc}\n\n{prompt}" if role_desc else prompt
    try:
        model = genai.GenerativeModel(model_name)
        if stream: return model.generate_content(full_prompt, stream=True)
        return model.generate_content(full_prompt).text
    except Exception as e:
        return f"⚠️ Lỗi AI ({model_name}): {str(e)}"

# --- 3. XỬ LÝ DỮ LIỆU ---
def clean(text): return unidecode.unidecode(str(text)).lower().replace(' ', '') if pd.notna(text) else ""
def init_data_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def check_data():
    if os.path.exists(DB_FILE):
        try: 
            c=init_data_db(); c.execute("SELECT count(*) FROM bhxh"); c.close(); return True, "OK"
        except: os.remove(DB_FILE)
    
    parts = sorted(glob.glob(f"{ZIP_PART_PREFIX}*"))
    if parts:
        msg = st.empty(); msg.info(f"📦 Đang nối {len(parts)} phần dữ liệu...")
        try:
            with open("bhxh_full.zip", 'wb') as o:
                for p in parts: 
                    with open(p, 'rb') as i: o.write(i.read())
            msg.info("📦 Đang giải nén..."); 
            with zipfile.ZipFile("bhxh_full.zip", 'r') as z: z.extractall()
            if os.path.exists("bhxh_full.zip"): os.remove("bhxh_full.zip")
            msg.empty(); return True, "Restored"
        except Exception as e: return False, str(e)
    
    if os.path.exists(EXCEL_FILE): return import_excel_to_sqlite()
    return False, "Thiếu dữ liệu"

def import_excel_to_sqlite():
    st.warning("⚠️ Đang nạp Excel. Nên dùng tool chia nhỏ file để nhanh hơn.")
    conn = init_data_db(); msg = st.empty(); bar = st.progress(0)
    try:
        msg.info("⏳ Đang xử lý..."); df = pd.read_excel(EXCEL_FILE, engine='pyxlsb'); bar.progress(30)
        df.columns = [unidecode.unidecode(str(c)).strip().replace(' ', '_').lower() for c in df.columns]
        df = df.astype(str).replace(['nan', 'None', 'NaT'], '')
        df['idx']=df.apply(lambda x: clean(' '.join(x.values)), axis=1)
        for col in df.columns: 
            if col != 'idx': df[f'i_{col}']=df[col].apply(clean)
        bar.progress(80)
        df.to_sql('bhxh', conn, if_exists='replace', index=False, chunksize=5000)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_m ON bhxh (idx)")
        bar.progress(100); msg.empty(); bar.empty(); conn.close(); return True, "Done"
    except Exception as e: conn.close(); return False, str(e)

@st.cache_data(ttl=3600)
def get_cols():
    c=init_data_db(); 
    try: return [r[1] for r in c.execute("PRAGMA table_info(bhxh)") if not r[1].startswith('i_') and r[1] not in ['idx','index'] and 'kcb' not in r[1]]
    except: return []
    finally: c.close()

def search(mode, q):
    c=init_data_db(); cols=get_cols()
    if not cols: return pd.DataFrame()
    sel = ", ".join([f'"{x}"' for x in cols])
    try:
        if mode=='ai': 
            k=clean(q); return pd.read_sql(f'SELECT {sel} FROM bhxh WHERE idx LIKE ? LIMIT 50', c, params=(f'%{k}%',))
        else:
            conds, vals = [], []
            for k,v in q.items():
                if v.strip(): conds.append(f'i_{clean(k)} LIKE ?'); vals.append(f'%{clean(v)}%')
            if not conds: return pd.DataFrame()
            return pd.read_sql(f'SELECT {sel} FROM bhxh WHERE {" AND ".join(conds)} LIMIT 50', c, params=tuple(vals))
    except: return pd.DataFrame()
    finally: c.close()

# --- TÍNH BHXH ---
def format_vnd(v): return f"{int(v):,} VNĐ".replace(",", ".")
def render_calculator():
    st.subheader("🧮 Tính Mức Đóng BHXH Tự Nguyện (Luật 2025)")
    c1, c2 = st.columns([2, 1])
    with c1:
        inc = st.slider("Mức thu nhập:", CHUAN_NGHEO, MAX_MUC_DONG, CHUAN_NGHEO, 50000, format="%d")
        exc = st.number_input("Nhập số chính xác:", CHUAN_NGHEO, MAX_MUC_DONG, inc, 1000)
        if exc != inc: inc = exc
    with c2: st.info(f"Chuẩn nghèo: {format_vnd(CHUAN_NGHEO)}\nTỷ lệ: 22%")
    
    dt = st.radio("Đối tượng:", ["Khác (20%)", "Hộ nghèo (50%)", "Hộ cận nghèo (40%)", "Dân tộc (30%)"], horizontal=True)
    base = inc * TY_LE_DONG
    
    if "nghèo" in dt and "cận" not in dt: supp=base*HO_TRO_NGHEO; l="50%"
    elif "cận" in dt: supp=base*HO_TRO_CAN_NGHEO; l="40%"
    elif "Dân tộc" in dt: supp=base*HO_TRO_DAN_TOC; l="30%"
    else: supp=base*HO_TRO_KHAC; l="20%"
    
    final = base - supp
    st.markdown(f"#### 📊 Bảng Đóng (Hỗ trợ: {l})")
    data = {"Kỳ hạn": ["1 tháng", "3 tháng", "6 tháng", "12 tháng"], "Tháng": [1,3,6,12], "Tổng": [], "Hỗ trợ": [], "PHẢI ĐÓNG": []}
    for m in data["Tháng"]:
        data["Tổng"].append(format_vnd(base*m))
        data["Hỗ trợ"].append(format_vnd(supp*m))
        data["PHẢI ĐÓNG"].append(format_vnd(final*m))
    st.dataframe(pd.DataFrame(data).style.highlight_max(axis=0, subset=["PHẢI ĐÓNG"], color='#e6ffe6'), use_container_width=True, hide_index=True)

# --- GIAO DIỆN CHÍNH ---
def login():
    st.markdown("<h2 style='text-align: center;'>🔐 Đăng Nhập</h2>", unsafe_allow_html=True)
    if not get_firestore_db(): st.toast("⚠️ Chế độ Offline (Dữ liệu user lưu trên máy).", icon="ℹ️")
    render_zalo_widget()
    c1,c2,c3=st.columns([1,2,1])
    with c2:
        with st.form("l"):
            u=st.text_input("User"); p=st.text_input("Pass", type='password')
            if st.form_submit_button("Vào"):
                r=verify_login(u,p)
                if r: st.session_state.update({'logged_in':True,'username':u,'role':r}); log_action(u,"Login"); st.rerun()
                else: st.error("Sai thông tin")

def page_pass():
    st.subheader("🔒 Đổi Mật Khẩu")
    with st.form("p"):
        o=st.text_input("Cũ",type='password'); n=st.text_input("Mới",type='password'); c=st.text_input("Nhập lại",type='password')
        if st.form_submit_button("Đổi"):
            u=st.session_state['username']
            if verify_login(u,o):
                if n==c and len(n)>=6:
                    if update_password(u,n): st.success("OK! Đăng nhập lại."); log_action(u,"ChangePass"); time.sleep(1); st.session_state['logged_in']=False; st.rerun()
                    else: st.error("Lỗi mạng")
                else: st.warning("Mật khẩu ngắn/không khớp")
            else: st.error("Mật khẩu cũ sai")

def page_search(cols):
    st.subheader("🔍 Tra Cứu")
    t1,t2=st.tabs(["Nhanh","Chi tiết"])
    with t1:
        q=st.text_input("Từ khóa:", placeholder="vd: nguyen van a")
        if q:
            log_action(st.session_state['username'],"Search AI",q)
            df=search('ai',q)
            if not df.empty:
                st.success(f"Thấy {len(df)} KQ"); st.dataframe(df,use_container_width=True,hide_index=True)
                if len(df)==1:
                    with st.expander("✨ AI Phân tích"): st.write(get_ai_response(f"Hồ sơ: {df.iloc[0].to_dict()}", "Chuyên gia BHXH tóm tắt."))
            else: st.warning("Không thấy.")
    with t2:
        defs=['sobhxh','hoten','ngaysinh','socmnd']; sel=[c for c in cols if any(x in clean(c) for x in defs)] or cols[:4]
        s=st.multiselect("Cột:",cols,default=sel); inp={}
        if s:
            c=st.columns(4)
            for i,n in enumerate(s): inp[n]=c[i%4].text_input(n)
        if st.button("Tìm"):
            v={k:v for k,v in inp.items() if v.strip()}
            if v:
                log_action(st.session_state['username'],"Search Manual",str(v))
                df=search('manual',v)
                if not df.empty: st.success(f"Thấy {len(df)} KQ"); st.dataframe(df,use_container_width=True,hide_index=True)
                else: st.warning("Không thấy.")
            else: st.warning("Nhập thông tin.")

def page_chat():
    st.subheader("🤖 Chatbot")
    if "msg" not in st.session_state: st.session_state.msg=[{"role":"model","content":"Chào bạn! Tôi có thể giúp gì?"}]
    for m in st.session_state.msg: st.chat_message(m["role"]).markdown(m["content"])
    if p:=st.chat_input():
        log_action(st.session_state['username'],"Chatbot",p)
        st.session_state.msg.append({"role":"user","content":p}); st.chat_message("user").markdown(p)
        with st.chat_message("model"):
            ph=st.empty(); res=""; 
            s=get_ai_response(p,"Chuyên gia BHXH.",True)
            try:
                if isinstance(s,str): ph.markdown(s); res=s
                else:
                    for c in s: 
                        if c.text: res+=c.text; ph.markdown(res+"▌")
                    ph.markdown(res)
            except: ph.markdown(res)
            st.session_state.msg.append({"role":"model","content":res})

def page_content():
    st.subheader("✍️ Tạo Nội Dung")
    t=st.text_input("Chủ đề:"); 
    if st.button("Viết") and t:
        log_action(st.session_state['username'],"Content",t)
        with st.spinner("..."): st.session_state['txt']=get_ai_response(f"Viết về: {t}", "Chuyên viên truyền thông")
    if 'txt' in st.session_state: st.text_area("KQ:",value=st.session_state['txt'],height=400)

def page_admin():
    st.header("🛠️ Quản Trị")
    t1,t2=st.tabs(["User","Logs"])
    with t1:
        st.dataframe(get_all_users(),use_container_width=True)
        c1,c2,c3=st.columns(3)
        with c1:
            with st.popover("➕ Thêm"):
                with st.form("a"):
                    u=st.text_input("User"); p=st.text_input("Pass"); r=st.selectbox("Role",["user","admin"])
                    if st.form_submit_button("Tạo"):
                        if create_user(u,p,r): st.success("OK"); log_action(st.session_state['username'],"Add",u); time.sleep(1); st.rerun()
                        else: st.error("Trùng")
        with c2:
            with st.popover("🗑️ Xóa"):
                d=st.text_input("User xóa:")
                if st.button("Xóa"):
                    if d!="admin" and delete_user(d): st.success("OK"); log_action(st.session_state['username'],"Del",d); time.sleep(1); st.rerun()
                    else: st.error("Lỗi")
        with c3:
            with st.popover("🔄 Reset Pass"):
                rs=st.text_input("User reset (123456):")
                if st.button("Reset"):
                    if update_password(rs,"123456"): st.success("OK"); log_action(st.session_state['username'],"Reset",rs)
                    else: st.error("Lỗi")
    with t2:
        if st.button("Tải lại"): st.rerun()
        st.dataframe(get_logs(200),use_container_width=True)

def main():
    init_admin_account()
    if 'logged_in' not in st.session_state: st.session_state.update({'logged_in':False,'page':'search'})
    render_zalo_widget()
    ok, msg = check_data()
    if not ok: st.error(msg); return
    
    if not st.session_state['logged_in']: render_login()
    else:
        with st.sidebar:
            st.title(f"Hi, {st.session_state['username']}")
            if st.button("🔍 Tra cứu",use_container_width=True): st.session_state['page']='search'
            if st.button("🧮 Tính BHXH",use_container_width=True): st.session_state['page']='calc'
            if st.button("🤖 Chatbot",use_container_width=True): st.session_state['page']='chat'
            if st.button("✍️ Nội dung",use_container_width=True): st.session_state['page']='content'
            st.divider()
            if st.button("🔒 Đổi Mật khẩu",use_container_width=True): st.session_state['page']='pass'
            if st.session_state['role']=='admin':
                st.divider(); 
                if st.button("🛠️ Quản trị",use_container_width=True): st.session_state['page']='admin'
            st.divider()
            if st.button("Đăng xuất",use_container_width=True):
                log_action(st.session_state['username'],"Logout"); st.session_state['logged_in']=False; st.rerun()
        
        p=st.session_state['page']; cols=get_cols()
        if p=='search': page_search(cols)
        elif p=='calc': render_calculator()
        elif p=='chat': page_chat()
        elif p=='content': page_content()
        elif p=='pass': page_pass()
        elif p=='admin': page_admin()

if __name__ == '__main__': main()
