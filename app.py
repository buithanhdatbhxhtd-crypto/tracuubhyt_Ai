import streamlit as st
import pandas as pd
import sqlite3
import hashlib
import datetime
import unidecode
import google.generativeai as genai
import time

# --- CẤU HÌNH ỨNG DỤNG ---
st.set_page_config(
    page_title="Hệ thống BHXH Thông Minh",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Tên file dữ liệu
DATA_FILE = 'aaa.xlsb'

# --- 1. CÁC HÀM XỬ LÝ HỆ THỐNG & DATABASE ---
def init_db():
    """Khởi tạo cơ sở dữ liệu user và log"""
    conn = sqlite3.connect('users.db', check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (username TEXT PRIMARY KEY, password TEXT, role TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS logs
                 (timestamp TEXT, username TEXT, action TEXT, details TEXT)''')
    
    # Tạo admin mặc định (pass: admin123)
    c.execute("SELECT * FROM users WHERE username = 'admin'")
    if not c.fetchone():
        c.execute("INSERT INTO users VALUES (?, ?, ?)", 
                  ('admin', hashlib.sha256(str.encode('admin123')).hexdigest(), 'admin'))
    conn.commit()
    return conn

def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def log_action(username, action, details=""):
    try:
        conn = init_db()
        c = conn.cursor()
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("INSERT INTO logs VALUES (?, ?, ?, ?)", (ts, username, action, details))
        conn.commit()
        conn.close()
    except:
        pass

def configure_gemini():
    """Cấu hình API Key an toàn"""
    api_key = None
    # 1. Thử lấy từ Secrets (Streamlit Cloud)
    try:
        api_key = st.secrets["GOOGLE_API_KEY"]
    except:
        pass
    
    # 2. Nếu không có, lấy từ Session State
    if not api_key:
        api_key = st.session_state.get('user_api_key', '')

    if api_key:
        genai.configure(api_key=api_key)
        return True
    return False

# --- 2. XỬ LÝ DỮ LIỆU (TỐI ƯU TÌM KIẾM) ---
@st.cache_data(show_spinner="Đang tối ưu hóa dữ liệu tìm kiếm...", ttl=3600)
def load_data_master():
    try:
        start_time = time.time()
        # Đọc file
        df = pd.read_excel(DATA_FILE, engine='pyxlsb')
        df.columns = [str(c).strip() for c in df.columns]
        df = df.astype(str)
        df.replace(['nan', 'None', 'NaT'], '', inplace=True)

        # Hàm xử lý text: Không dấu + Viết thường + Xóa khoảng trắng
        def clean_text(text):
            if not text: return ''
            return unidecode.unidecode(text).lower().replace(' ', '')

        # 1. Tạo index cho từng cột (Tra cứu thủ công)
        for col in df.columns:
            df[f'idx_{col}'] = df[col].apply(clean_text)

        # 2. Tạo Master Index (Tra cứu AI)
        df['Master_Search'] = df[[c for c in df.columns if not c.startswith('idx_')]].apply(
            lambda row: ' '.join(row.values.astype(str)), axis=1
        )
        df['Master_Search'] = df['Master_Search'].apply(clean_text)

        return df, f"Đã xử lý {len(df)} hồ sơ ({round(time.time() - start_time, 2)}s)"
    
    except FileNotFoundError:
        return None, "Lỗi: Không tìm thấy file dữ liệu."
    except Exception as e:
        return None, f"Lỗi nghiêm trọng: {str(e)}"

# --- 3. LOGIC TRA CỨU ---

def manual_search_logic(df, filters):
    if not filters: return pd.DataFrame()
    result_df = df.copy()
    for col, value in filters.items():
        if value:
            # Xử lý từ khóa nhập vào: xóa dấu, xóa khoảng trắng
            value_clean = unidecode.unidecode(value).lower().replace(' ', '')
            idx_col = f'idx_{col}'
            result_df = result_df[result_df[idx_col].str.contains(value_clean, na=False)]
            if result_df.empty: break
    return result_df

def ai_search_logic(df, keyword):
    if not keyword: return pd.DataFrame()
    keyword_clean = unidecode.unidecode(keyword).lower().replace(' ', '')
    return df[df['Master_Search'].str.contains(keyword_clean, na=False)]

# --- 4. GIAO DIỆN ---

def render_login():
    st.markdown("<h2 style='text-align: center;'>🔐 Đăng Nhập Hệ Thống BHXH</h2>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login_form"):
            username = st.text_input("Tên đăng nhập")
            password = st.text_input("Mật khẩu", type='password')
            submitted = st.form_submit_button("Đăng nhập", use_container_width=True)
            
            if submitted:
                conn = init_db()
                c = conn.cursor()
                c.execute('SELECT * FROM users WHERE username = ? AND password = ?', 
                          (username, make_hashes(password)))
                data = c.fetchall()
                conn.close()
                
                if data:
                    st.session_state['logged_in'] = True
                    st.session_state['username'] = username
                    st.session_state['role'] = data[0][2]
                    log_action(username, "Login", "Success")
                    st.rerun()
                else:
                    st.error("Sai tên đăng nhập hoặc mật khẩu.")

def render_manual_search(df, cols_to_show):
    st.subheader("📋 Tra Cứu Thủ Công (Chính Xác)")
    st.caption("Chọn cột cần tìm và nhập thông tin (có thể viết liền không dấu).")
    
    with st.expander("⚙️ Cấu hình cột lọc", expanded=True):
        default_options = []
        for c in ['hoten', 'ngaysinh', 'socmnd', 'cccd', 'mabhxh', 'mathe']:
            for col in cols_to_show:
                if c in unidecode.unidecode(col).lower():
                    default_options.append(col)
        if not default_options: default_options = cols_to_show[:3]
        selected_filters = st.multiselect("Chọn tiêu chí:", cols_to_show, default=default_options)

    user_inputs = {}
    if selected_filters:
        cols = st.columns(len(selected_filters))
        for i, col_name in enumerate(selected_filters):
            user_inputs[col_name] = cols[i].text_input(f"Nhập {col_name}", key=f"search_{col_name}")

    if st.button("🔍 Tìm kiếm ngay", type="primary"):
        valid_filters = {k: v for k, v in user_inputs.items() if v.strip() != ""}
        if valid_filters:
            results = manual_search_logic(df, valid_filters)
            st.success(f"Tìm thấy **{len(results)}** kết quả.")
            if not results.empty:
                st.dataframe(results[cols_to_show], use_container_width=True)
                log_action(st.session_state['username'], "Manual Search", str(valid_filters))
        else:
            st.warning("Vui lòng nhập ít nhất một thông tin.")

def render_ai_search(df, cols_to_show, is_ai_ready):
    st.subheader("🤖 Tra Cứu Thông Minh (Smart Search)")
    st.caption("Nhập bất kỳ thông tin nào bạn có: Tên dính liền, ngày sinh, số thẻ...")
    
    query = st.text_input("Nhập từ khóa:", placeholder="Ví dụ: nguyenvana 1990, 03123...")
    
    if query:
        results = ai_search_logic(df, query)
        count = len(results)
        
        if count > 0:
            st.success(f"Tìm thấy **{count}** hồ sơ khớp.")
            st.dataframe(results[cols_to_show], use_container_width=True)
            
            if count <= 3:
                st.markdown("---")
                st.markdown("### 💡 Phân tích AI")
                if not is_ai_ready:
                    st.warning("Vui lòng nhập API Key ở menu bên trái để dùng tính năng phân tích.")
                else:
                    for idx, row in results[cols_to_show].iterrows():
                        with st.expander(f"Chi tiết: {row.iloc[0]}", expanded=True):
                            with st.spinner("AI đang phân tích..."):
                                try:
                                    model = genai.GenerativeModel('gemini-pro')
                                    prompt = f"""
                                    Dữ liệu: {row.to_dict()}
                                    Yêu cầu: Tóm tắt thông tin người này (Họ tên, tuổi, nơi ĐKCB) và giải thích quyền lợi.
                                    Trả lời tiếng Việt.
                                    """
                                    response = model.generate_content(prompt)
                                    st.write(response.text)
                                except Exception as e:
                                    st.error(f"Lỗi AI: {e}")
            log_action(st.session_state['username'], "AI Search", query)
        else:
            st.warning("Không tìm thấy dữ liệu.")

def render_admin():
    st.header("🛠️ Quản Trị Hệ Thống")
    conn = init_db()
    
    tab1, tab2 = st.tabs(["👥 Quản lý Người dùng", "📜 Nhật ký hoạt động"])
    
    with tab1:
        # Lấy danh sách user
        c = conn.cursor()
        c.execute("SELECT username, role FROM users")
        users_data = c.fetchall()
        df_users = pd.DataFrame(users_data, columns=['User', 'Role'])
        
        # Hiển thị bảng user
        st.dataframe(df_users, use_container_width=True)
        st.divider()
        
        # Chia cột cho Thêm và Xóa
        col_add, col_del = st.columns(2)
        
        # --- PHẦN THÊM USER ---
        with col_add:
            st.subheader("Thêm Người Dùng")
            with st.form("add_user_form"):
                new_u = st.text_input("Tên đăng nhập mới")
                new_p = st.text_input("Mật khẩu", type="password")
                new_r = st.selectbox("Quyền hạn", ["user", "admin"])
                if st.form_submit_button("Tạo tài khoản"):
                    if new_u and new_p:
                        try:
                            c.execute("INSERT INTO users VALUES (?,?,?)", (new_u, make_hashes(new_p), new_r))
                            conn.commit()
                            st.success(f"Đã tạo user: {new_u}")
                            log_action(st.session_state['username'], "Create User", new_u)
                            time.sleep(1)
                            st.rerun()
                        except sqlite3.IntegrityError:
                            st.error("Tên đăng nhập đã tồn tại!")
                    else:
                        st.warning("Vui lòng nhập đủ thông tin.")

        # --- PHẦN XÓA USER (ĐÃ THÊM) ---
        with col_del:
            st.subheader("Xóa Người Dùng")
            # Lọc danh sách để không cho xóa admin gốc
            user_list = [u[0] for u in users_data if u[0] != 'admin']
            
            if user_list:
                user_to_delete = st.selectbox("Chọn tài khoản cần xóa", user_list)
                st.warning(f"Cảnh báo: Hành động xóa '{user_to_delete}' không thể hoàn tác.")
                
                if st.button("🗑️ Xác nhận xóa", type="primary"):
                    try:
                        c.execute("DELETE FROM users WHERE username = ?", (user_to_delete,))
                        conn.commit()
                        st.success(f"Đã xóa thành công: {user_to_delete}")
                        log_action(st.session_state['username'], "Delete User", user_to_delete)
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Lỗi: {e}")
            else:
                st.info("Không có tài khoản phụ nào để xóa.")

    with tab2:
        st.subheader("Lịch sử truy cập & tra cứu")
        col_log1, col_log2 = st.columns([4,1])
        with col_log2:
            if st.button("Xóa toàn bộ nhật ký"):
                c.execute("DELETE FROM logs")
                conn.commit()
                st.success("Đã dọn dẹp nhật ký!")
                time.sleep(1)
                st.rerun()
        
        c.execute("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 200")
        df_logs = pd.DataFrame(c.fetchall(), columns=['Thời gian', 'Người dùng', 'Hành động', 'Chi tiết'])
        st.dataframe(df_logs, use_container_width=True)
    
    conn.close()

# --- 5. HÀM MAIN ---
def main():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False

    if not st.session_state['logged_in']:
        render_login()
    else:
        with st.sidebar:
            st.image("https://cdn-icons-png.flaticon.com/512/2303/2303512.png", width=50)
            st.title("Menu Điều Khiển")
            st.write(f"Xin chào, **{st.session_state['username']}**")
            
            with st.popover("Cấu hình AI Key"):
                key = st.text_input("Nhập API Key", type="password", value=st.session_state.get('user_api_key',''))
                if key:
                    st.session_state['user_api_key'] = key
                    st.success("Đã lưu!")
            
            st.divider()
            
            if 'page' not in st.session_state: st.session_state['page'] = 'search_manual'
            
            if st.button("📋 Tra cứu Thủ công", use_container_width=True):
                st.session_state['page'] = 'search_manual'
                st.rerun()
                
            if st.button("🤖 Tra cứu AI", use_container_width=True):
                st.session_state['page'] = 'search_ai'
                st.rerun()
                
            if st.session_state['role'] == 'admin':
                if st.button("🛠️ Quản trị hệ thống", use_container_width=True):
                    st.session_state['page'] = 'admin'
                    st.rerun()
            
            st.divider()
            if st.button("Đăng xuất", use_container_width=True):
                log_action(st.session_state['username'], "Logout")
                st.session_state['logged_in'] = False
                st.rerun()

        # Nạp dữ liệu
        df, status_msg = load_data_master()
        
        if df is None:
            st.error(f"⚠️ {status_msg}. Vui lòng kiểm tra file 'aaa.xlsb'.")
            return

        cols_to_show = [c for c in df.columns if not c.startswith('idx_') and c != 'Master_Search']
        is_ai_ready = configure_gemini()

        page = st.session_state['page']
        if page == 'search_manual':
            render_manual_search(df, cols_to_show)
        elif page == 'search_ai':
            render_ai_search(df, cols_to_show, is_ai_ready)
        elif page == 'admin':
            if st.session_state['role'] == 'admin':
                render_admin()
            else:
                st.error("Bạn không có quyền truy cập trang này.")

if __name__ == '__main__':
    init_db()
    main()
