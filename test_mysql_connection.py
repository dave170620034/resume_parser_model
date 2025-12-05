import mysql.connector

try:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Cybrom#1123",
        database="candidate_info"
    )

    cursor = conn.cursor()

    sql = """
        INSERT INTO candidate_details
        (file_name, name, email, phone, skills, education)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    data = (
        "sample.pdf",
        "Test User",
        "test@example.com",
        "9876543210",
        "python, sql",
        "B.Tech Computer Science"
    )

    cursor.execute(sql, data)
    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Data inserted successfully!")

except Exception as e:
    print("❌ Insert Error:", e)
