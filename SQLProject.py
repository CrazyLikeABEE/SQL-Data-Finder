#!/usr/bin/python
import psycopg2

conn = psycopg2.connect(database="news")
cur = conn.cursor()

cur.execute('''SELECT title, COUNT(log.time)
            FROM articles, log
            WHERE path LIKE CONCAT('%', slug)
            GROUP BY title
            ORDER BY COUNT DESC LIMIT 3;''')

rows = cur.fetchall()
titles = [x[0] for x in rows]
counts = [x[1] for x in rows]

print("\nHere is the solution to question 1:")
for i in range(len(titles)):
    print("   " + titles[i] + " - " + str(counts[i]) + " views")

print("\n")

cur.execute('''SELECT authors.name, COUNT(log.time)
            FROM authors, articles, log
            WHERE path LIKE CONCAT('%', slug) AND authors.id = articles.author
            GROUP BY authors.name
            ORDER BY COUNT DESC LIMIT 4;''')

rows = cur.fetchall()

print("\nHere is the solution to question 2:")
for i in rows:
    print(" %s - %s views" % (i[0], i[1]))

print("\n")

cur.execute('''CREATE VIEW total AS
            SELECT date(time), COUNT(*) AS views
            FROM log
            GROUP BY date(time)
            ORDER BY date(time);''')
cur.execute('''CREATE VIEW err AS
            SELECT date(time), COUNT(*) AS errors
            FROM log WHERE status LIKE '404%'
            GROUP BY date(time)
            ORDER BY date(time);''')
cur.execute('''SELECT total.date, ((100.0*err.errors/total.views)>1) AS percent
            FROM total, err
            WHERE total.date = err.date
            ORDER BY percent DESC LIMIT 1;''')

rows = cur.fetchall()

print("\nHere is the solution to question 3:")
for i in rows:
    print(" %s - %s error" % (i[0], i[1]))

print("\n")

conn.commit()
conn.close()