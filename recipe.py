import os
import mysql.connector
import shutil
# กำหนดเครื่องจักร
machines = [f"AD{str(i).zfill(2)}" for i in range(5, 31)]
directory = r"D:\masterlist"

os.makedirs(directory, exist_ok=True)
# เชื่อมต่อฐานข้อมูล
db = mysql.connector.connect(
    host="172.18.106.100",
    user="pe",
    password="123456",
    database="qtsoft"
)
cursor = db.cursor(dictionary=True)
db2=mysql.connector.connect(
    host="172.18.106.100",
    user="pe",
    password="123456",
    database="vipdatabase"

)
cursor2 = db2.cursor(dictionary=True)
try:
    for machine in machines:
        # ดึง setting จาก DB
        cursor.execute("SELECT * FROM `master mc ad_ program` WHERE MCNO = %s", (machine,))
        result = cursor.fetchone()
        if not result:
            print(f"ไม่พบข้อมูลของ {machine}")
            continue
   
    

        lines = []
        for key,value in result.items():
            lines.append(str(value))
        recipe = "\n".join(lines)


        # กำหนดชื่อไฟล์ .txt
        filename = os.path.join(directory, f"Master list{machine}.txt")

        # อ่านค่าเดิมในไฟล์ (ถ้ามี)
        file_setting = None
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                file_setting = f.read()

        # ถ้า setting เปลี่ยนแปลง ให้เขียนไฟล์ใหม่
        # if file_setting != str(recipe):
        #     with open(filename, "w", encoding="utf-8") as f:
        #         f.write(str(recipe))
        #     print(f"เขียนไฟล์ใหม่: {filename}")
        # else:
        #     print(f"ไม่มีการเปลี่ยนแปลง: {filename}")
        old_lines = file_setting.splitlines() if file_setting else []
        new_lines = recipe.splitlines()

        if old_lines != new_lines:
            with open(filename, "w", encoding="utf-8") as f:
                f.write(recipe)
            print(f"เขียนไฟล์ใหม่: {filename}")
        else:
            print(f"ไม่มีการเปลี่ยนแปลง: {filename}")

        cursor2.execute("SELECT Machine, IP, Version FROM ad_ip where Machine=%s",(machine,))
        result2=cursor2.fetchone()
        # เครื่องที่ไม่ได้ลงทะเบียนใน ad_ip ต้องข้ามไป ไม่ใช่ทำให้ทั้ง loop หยุด
        if not result2:
            print(f"ไม่พบ {machine} ใน ad_ip — ข้าม")
            continue

        mcpart = rf"\\{result2['IP']}\{result2['Version']}"
        print(mcpart)
        distination=mcpart
        try:
            shutil.copy(filename,distination)
            print(f"Copy {filename} --> {distination}")
        except Exception as e:
            print(f"error copy {filename}:{e}")

finally:
    # ปิดการเชื่อมต่อเสมอ แม้ loop จะพังกลางทาง
    cursor.close()
    db.close()
    cursor2.close()
    db2.close()
