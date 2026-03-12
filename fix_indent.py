with open("app.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

new_lines = []
in_main_block = False

for line in lines:
    stripped = line.strip()
    
    if stripped == 'if st.session_state.current_view == "Main":':
        in_main_block = True
        new_lines.append(line)
        continue
        
    if in_main_block:
        if stripped == 'elif st.session_state.current_view == "Dashboard":':
            in_main_block = False
            new_lines.append(line)
            continue
            
        if stripped == "if 'data_loaded' not in st.session_state or not st.session_state.data_loaded:":
            in_main_block = False
            new_lines.append(line)
            continue
            
        if line != "\\n":
            new_lines.append("    " + line)
        else:
            new_lines.append(line)
    else:
        new_lines.append(line)

with open("app.py", "w", encoding="utf-8") as f:
    f.writelines(new_lines)
print("Done!")
